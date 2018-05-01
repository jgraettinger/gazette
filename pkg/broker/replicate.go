package broker

import (
	"fmt"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// replicate evaluates a peer broker's replication RPC.
func replicate(j *journal, req *pb.ReplicateRequest, srv pb.Broker_ReplicateServer) (err error) {
	// Acquire ownership of the journal Spool.
	var spool fragment.Spool
	select {
	case spool = <-j.spoolCh:
		// Pass.
	case <-j.ctx.Done():
		return j.ctx.Err() // Journal cancelled.
	case <-srv.Context().Done():
		return srv.Context().Err() // Request cancelled.
	}
	// Defer releasing it. Note |spool| may change, so close over it (rather
	// than deferring a call which would evaluate arguments immediately).
	defer func() { j.spoolCh <- spool }()

	// The coordinator may know of written offset ranges that we don't.
	// Skip our offset forward (only) to match the coordinator offset.
	if req.NextOffset > spool.Fragment.End {
		spool.Roll(j.spec, req.NextOffset)
	}
	if err = prepareSpool(&spool, j); err != nil {
		return
	}

	// Precondition: require that the request NextOffset matches our own.
	if req.NextOffset != spool.Fragment.End {
		err = srv.SendMsg(&pb.ReplicateResponse{
			Status:    pb.Status_WRONG_WRITE_HEAD,
			WriteHead: spool.Fragment.End,
		})
		return
	}
	// Precondition: require that the request Route matches our own.
	if !j.route.Equivalent(req.Route) {
		err = srv.SendMsg(&pb.ReplicateResponse{
			Status: pb.Status_WRONG_ROUTE,
			Route:  &j.route,
		})
		return
	}

	// Signal that we're ready to proceed with the transaction.
	if err = srv.SendMsg(&pb.ReplicateResponse{Status: pb.Status_OK}); err != nil {
		return
	}

	var total int64 // Total bytes across all streamed ReplicateRequests.

	// Receive transaction content.
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
		spool.Roll(j.spec, spool.Fragment.End)
		return
	}

	// Add the updated local Fragment to the index.
	j.index.addLocal(spool.Fragment)
	return
}
