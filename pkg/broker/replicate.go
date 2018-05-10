package broker

import (
	"fmt"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// replicate evaluates a peer broker's replication RPC.
func replicate(r *replica, req *pb.ReplicateRequest, srv pb.Broker_ReplicateServer) (resp *pb.ReplicateResponse, err error) {
	// Acquire ownership of the replica Spool.
	var spool fragment.Spool
	select {
	case spool = <-r.spoolCh:
		// Pass.
	case <-r.ctx.Done():
		err = r.ctx.Err() // Journal cancelled.
		return
	case <-srv.Context().Done():
		err = srv.Context().Err() // Request cancelled.
		return
	}
	// Defer releasing it. Note |spool| may change, so close over it (rather
	// than deferring a call which would evaluate arguments immediately).
	defer func() { r.spoolCh <- spool }()

	// The coordinator may know of written offset ranges that we don't.
	// Skip our offset forward (only) to match the coordinator offset.
	if req.NextOffset > spool.Fragment.End {
		spool.Roll(r.spec(), req.NextOffset)
	}
	if err = prepareSpool(&spool, r); err != nil {
		return
	}

	// Generate and send our ReplicateResponse.
	if req.NextOffset != spool.Fragment.End {
		// Precondition: require that the request NextOffset matches our own.
		resp = &pb.ReplicateResponse{
			Status:    pb.Status_WRONG_WRITE_HEAD,
			WriteHead: spool.Fragment.End,
		}
	} else if !r.route.Equivalent(req.Route) {
		// Precondition: require that the request Route matches our own.
		resp = &pb.ReplicateResponse{
			Status: pb.Status_WRONG_ROUTE,
			Route:  &r.route,
		}
	} else {
		// We're ready to proceed with the pipeline.
		resp = &pb.ReplicateResponse{Status: pb.Status_OK}
	}
	if err = srv.SendMsg(resp); err != nil || resp.Status != pb.Status_OK {
		return
	}

	var total int64 // Total bytes across all streamed ReplicateRequests.

	// Receive pipeline content.
	for {
		if err = srv.RecvMsg(req); err != nil {
			return
		} else if err = req.Validate(); err != nil {
			return
		} else if expect := spool.Fragment.End + total; req.NextOffset != expect {
			err = fmt.Errorf("invalid NextOffset (%d; expected %d)", req.NextOffset, expect)
			return
		} else if req.Content == nil {
			// Content EOF. This message signals that it's time to commit, and its
			// |Commit| field tells us how many bytes to include. It may be less
			// than |total| (eg, if the coordinator wrote two Append RPCs, and then
			// encountered a read error from the client, partway through the third).
			break
		} else if _, err = spool.File.WriteAt(req.Content, spool.ContentLength()+total); err != nil {
			return
		} else {
			total += int64(len(req.Content))
		}
	}

	if req.Commit > total {
		err = fmt.Errorf("invalid req.Commit (%d > total %d)", req.Commit, total)
		return
	} else if err = spool.Commit(req.Commit); err != nil {
		// |err| invalidates |spool| for further writes. We must Roll it.
		spool.Roll(r.spec(), spool.Fragment.End)
		return
	}

	// Add the updated local Fragment to the index.
	r.index.addLocal(spool.Fragment)
	return
}
