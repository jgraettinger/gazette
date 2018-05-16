package broker

import (
	"context"
	"crypto/sha1"
	"io"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

func acquireSpool(ctx context.Context, r *replica) (spool fragment.Spool, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		err = r.ctx.Err() // replica cancelled.
	case spool = <-r.spoolCh:
		// Pass.
	}
	return
}

func acquirePipeline(ctx context.Context, r *replica, connFn buildConnFn) (*pipeline, int64, error) {
	var pln *pipeline
	var err error

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		return nil, 0, r.ctx.Err() // replica cancelled.
	case pln = <-r.plnHandoffCh:
		// Pass.
	}

	// If |pln| is a placeholder indicating the need to read through a revision
	// which we have since read through, clear it.
	if pln != nil && pln.readThroughRev != 0 && pln.readThroughRev <= r.route.Revision {
		pln = nil
	}

	// If |pln| is a valid pipeline but is built on a non-equivalent & older Route,
	// tear it down to begin a new one.
	if pln != nil && !pln.route.Equivalent(&r.route) && pln.route.Revision < r.route.Revision {
		if pln.closeSend(r.spoolCh); pln.sendErr() != nil {
			log.WithField("err", pln.sendErr()).Warn("tearing down pipeline: failed to closeSend")
		}

		// Block for, and read peer EOFs in a goroutine. This lets us start building the
		// new pipeline concurrently with the old one completing shutdown.
		go func(pln *pipeline) {
			<-pln.readBarrierCh

			if pln.gatherEOF(); pln.recvErr() != nil {
				log.WithField("err", pln.recvErr()).Warn("tearing down pipeline: failed to gatherEOF")
			}
		}(pln)

		pln = nil
	}

	if pln == nil {
		pln, err = startPipeline(ctx, r, connFn)
	}

	if err != nil {
		r.plnHandoffCh <- nil
		return nil, 0, err
	} else if pln.readThroughRev != 0 {
		r.plnHandoffCh <- pln
		return nil, pln.readThroughRev, nil
	}
	return pln, 0, nil
}

func startPipeline(ctx context.Context, r *replica, connFn buildConnFn) (*pipeline, error) {
	var pln *pipeline

	// Block until we've completed a first load of remote Fragment metadata.
	if err := waitForInitialIndexLoad(ctx, r); err != nil {
		return nil, err
	} else if spool, err := acquireSpool(ctx, r); err != nil {
		return nil, err
	} else {
		pln = newPipeline(r.ctx, r.route, spool, connFn)
	}

	// Potentially roll or update the Spool Fragment to conform to the current JournalSpec.
	// Also ensure the Spool reflects the maximal offset of the Fragment Index.
	var proposal, _ = updateProposal(pln.spool.Fragment.Fragment, r.spec().Fragment)

	if eo := r.index.EndOffset(); eo > proposal.End {
		proposal.Begin = eo
		proposal.End = eo
		proposal.Sum = pb.SHA1Sum{}
	}

	for {
		var rollToOffset, readThroughRev, err = pln.sync(proposal)

		if err != nil {
			pln.closeSend(r.spoolCh)
			pln.gatherEOF()
			return nil, err
		}

		if rollToOffset != 0 {
			// Update our |proposal| to roll forward to the new offset. Loop to try
			// again. This time all peers should agree on the new Fragment.
			proposal.Begin = rollToOffset
			proposal.End = rollToOffset
			proposal.Sum = pb.SHA1Sum{}
			continue
		}

		if readThroughRev != 0 {
			// Peer has a non-equivalent Route at a later Etcd revision. Close the
			// pipeline, and set its |readThroughRev| as an indication to other RPCs
			// of the revision which must first be read through before attempting
			// another pipeline.
			pln.closeSend(r.spoolCh)
			pln.gatherEOF()
			pln.readThroughRev = readThroughRev
		}
		return pln, nil
	}
}

func updateProposal(cur pb.Fragment, spec pb.JournalSpec_Fragment) (pb.Fragment, bool) {
	// If the proposed Fragment is non-empty, but not yet at the target length,
	// don't propose changes to it.
	if cur.ContentLength() > 0 && cur.ContentLength() < spec.Length {
		return cur, false
	}

	var next = cur
	next.Begin = next.End
	next.CompressionCodec = spec.CompressionCodec

	if len(spec.Stores) != 0 {
		next.BackingStore = spec.Stores[0]
	} else {
		next.BackingStore = ""
	}
	return next, next != cur
}

func proxyAppendRequest(srv pb.Broker_AppendServer, pln *pipeline) (*pb.Fragment, error) {
	// |result| will define the Fragment of Journal into which the Append was recorded.
	var result = &pb.Fragment{
		Journal: pln.spool.Fragment.Journal,
		Begin:   pln.spool.Fragment.End,
		End:     pln.spool.Fragment.End,
	}
	var readErr error
	var req = new(pb.AppendRequest)
	var summer = sha1.New()

	for readErr == nil && pln.sendErr() == nil {
		if readErr = srv.RecvMsg(req); readErr == io.EOF {
			readErr = nil // Reached end-of-input for this Append stream.
			break
		} else if readErr != nil {
			// Pass.
		} else if readErr = req.Validate(); readErr != nil {
			// Pass.
		} else {
			// Multiplex content to each replication stream, and the local Spool.
			pln.scatter(&pb.ReplicateRequest{
				Content:      req.Content,
				ContentDelta: result.ContentLength(),
			})
			_, _ = summer.Write(req.Content) // Cannot error.
			result.End += int64(len(req.Content))
		}
	}
	result.Sum = pb.SHA1SumFromDigest(summer.Sum(nil))

	var proposal = new(pb.Fragment)
	if readErr == nil {
		// Commit the Append, by scattering the next Fragment to be committed
		// to each peer. They will inspect & validate the Fragment locally,
		// and commit or return an error.
		*proposal = pln.spool.Next()
	} else {
		// A client-side read error occurred. The pipeline is still in a good
		// state, but any partial spooled content must be rolled back.
		*proposal = pln.spool.Fragment.Fragment
	}

	pln.scatter(&pb.ReplicateRequest{
		Proposal:    proposal,
		Acknowledge: true,
	})
	return result, readErr
}
