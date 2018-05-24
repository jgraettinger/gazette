package broker

import (
	"crypto/sha1"
	"hash"
	"io"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type appender struct {
	pln  *pipeline
	spec pb.JournalSpec_Fragment

	reqFragment *pb.Fragment
	reqSummer   hash.Hash
	reqErr      error
}

func beginAppending(pln *pipeline, spec pb.JournalSpec_Fragment) appender {
	// Potentially roll the Fragment forward prior to serving the append.
	// We expect this to always succeed and don't ask for an acknowledgement.
	var proposal, update = updateProposal(pln.spool.Fragment.Fragment, spec)

	if update {
		pln.scatter(&pb.ReplicateRequest{
			Proposal:    &proposal,
			Acknowledge: false,
		})
	}

	return appender{
		pln:  pln,
		spec: spec,

		reqFragment: &pb.Fragment{
			Journal: pln.spool.Fragment.Journal,
			Begin:   pln.spool.Fragment.End,
			End:     pln.spool.Fragment.End,
		},
		reqSummer: sha1.New(),
	}
}

func (a *appender) onRecv(req *pb.AppendRequest, err error) bool {
	if err == nil {
		err = req.Validate()
	}

	if err != nil {
		// Reached end-of-input for this Append stream.
		a.reqFragment.Sum = pb.SHA1SumFromDigest(a.reqSummer.Sum(nil))

		var proposal = new(pb.Fragment)
		if err == io.EOF {
			// Commit the Append, by scattering the next Fragment to be committed
			// to each peer. They will inspect & validate the Fragment locally,
			// and commit or return an error.
			*proposal = a.pln.spool.Next()
		} else {
			// A client-side read error occurred. The pipeline is still in a good
			// state, but any partial spooled content must be rolled back.
			*proposal = a.pln.spool.Fragment.Fragment

			a.reqErr = err
			a.reqFragment = nil
		}

		a.pln.scatter(&pb.ReplicateRequest{
			Proposal:    proposal,
			Acknowledge: true,
		})
		return false
	}

	// Forward content through the pipeline.
	a.pln.scatter(&pb.ReplicateRequest{
		Content:      req.Content,
		ContentDelta: a.reqFragment.ContentLength(),
	})
	_, _ = a.reqSummer.Write(req.Content) // Cannot error.
	a.reqFragment.End += int64(len(req.Content))

	return a.pln.sendErr() == nil
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
