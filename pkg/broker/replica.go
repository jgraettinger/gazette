package broker

import (
	"context"
	"fmt"

	"github.com/LiveRamp/gazette/pkg/client"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type replica struct {
	// Context tied to processing lifetime of this replica by this broker.
	// Cancelled when this broker is no longer responsible for the replica.
	ctx    context.Context
	cancel context.CancelFunc
	// Index of all known Fragments of the replica.
	index *fragment.Index
	// spoolCh synchronizes access to the single Spool of the replica.
	spoolCh chan fragment.Spool
	// pipelineCh synchronizes access to the single pipeline of the replica.
	pipelineCh chan *pipeline
}

func newReplica(journal pb.Journal) *replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var r = &replica{
		ctx:        ctx,
		cancel:     cancel,
		index:      fragment.NewIndex(ctx),
		spoolCh:    make(chan fragment.Spool, 1),
		pipelineCh: make(chan *pipeline, 1),
	}

	r.spoolCh <- fragment.NewSpool(journal, struct {
		*fragment.Index
		*fragment.Persister
	}{r.index, sharedPersister})

	r.pipelineCh <- nil

	return r
}

func acquireSpool(ctx context.Context, r *replica, waitForRemoteLoad bool) (spool fragment.Spool, err error) {
	if waitForRemoteLoad {
		if err = r.index.WaitForFirstRemoteLoad(ctx); err != nil {
			return
		}
	}

	select {
	case <-ctx.Done():
		err = ctx.Err() // |ctx| cancelled.
		return
	case <-r.ctx.Done():
		err = r.ctx.Err() // replica cancelled.
		return
	case spool = <-r.spoolCh:
		// Pass.
	}

	if eo := r.index.EndOffset(); eo > spool.Fragment.End {
		// If the Fragment index knows of an offset great than the current Spool,
		// roll the Spool forward to the new offset.
		var proposal = spool.Fragment.Fragment
		proposal.Begin, proposal.End, proposal.Sum = eo, eo, pb.SHA1Sum{}

		var resp pb.ReplicateResponse
		resp, err = spool.Apply(&pb.ReplicateRequest{Proposal: &proposal})

		if err != nil || resp.Status != pb.Status_OK {
			// Cannot happen, as we crafted |proposal| relative to the |spool.Fragment|.
			panic(fmt.Sprintf("failed to roll to EndOffset: %s err %s", &resp, err))
		}
	}

	return
}

func acquirePipeline(ctx context.Context, r *replica, hdr pb.Header, dialer client.Dialer) (*pipeline, int64, error) {
	var pln *pipeline

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		return nil, 0, r.ctx.Err() // replica cancelled.
	case pln = <-r.pipelineCh:
		// Pass.
	}

	// Is |pln| is a placeholder indicating the need to read through a revision, which we've since read through?
	if pln != nil && pln.readThroughRev != 0 && pln.readThroughRev <= hdr.Etcd.Revision {
		pln = nil
	}

	// If |pln| is a valid pipeline but is built on a non-equivalent & older Route,
	// tear it down asynchronously and immediately begin a new one.
	if pln != nil && pln.readThroughRev == 0 &&
		!pln.Route.Equivalent(&hdr.Route) && pln.Etcd.Revision < hdr.Etcd.Revision {

		go func(pln *pipeline) {
			var waitFor, _ = pln.barrier()

			if pln.closeSend(); pln.sendErr() != nil {
				log.WithField("err", pln.sendErr()).Warn("tearing down pipeline: failed to closeSend")
			}
			<-waitFor

			if pln.gatherEOF(); pln.recvErr() != nil {
				log.WithField("err", pln.recvErr()).Warn("tearing down pipeline: failed to gatherEOF")
			}
		}(pln)

		pln = nil
	}

	var err error

	if pln == nil {
		// We must construct a new pipeline.
		var spool fragment.Spool
		spool, err = acquireSpool(ctx, r, true)

		if err == nil {
			pln = newPipeline(r.ctx, hdr, spool, r.spoolCh, dialer)
			err = pln.synchronize()
		}
	}

	if err != nil {
		r.pipelineCh <- nil // Release ownership, allow next acquirer to retry.
		return nil, 0, err
	} else if pln.readThroughRev != 0 {
		r.pipelineCh <- pln // Release placeholder for next acquirer to observe.
		return nil, pln.readThroughRev, nil
	}

	return pln, 0, nil
}

var sharedPersister *fragment.Persister

func SetSharedPersister(p *fragment.Persister) { sharedPersister = p }
