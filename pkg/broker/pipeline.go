package broker

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// pipeline is an in-flight write replication pipeline of a journal.
type pipeline struct {
	route    pb.Route
	spool    fragment.Spool
	streams  []pb.Broker_ReplicateClient
	sendErrs []error

	readBarrierCh chan struct{}
	recvResp      []pb.ReplicateResponse
	recvErrs      []error

	// readThroughRev, if set, indicates that a pipeline cannot be established
	// until we have read through (and our Route reflects) this Etcd revision.
	readThroughRev int64
}

// newPipeline returns a new pipeline.
func newPipeline(ctx context.Context, route pb.Route, spool fragment.Spool, connFn buildConnFn) *pipeline {
	if route.Primary == -1 {
		panic("newPipeline requires Route with Primary != -1")
	}

	// Enable compression while the Spool is serving as primary within a pipeline.
	spool.Primary = true

	var pln = &pipeline{
		route:         route,
		spool:         spool,
		streams:       make([]pb.Broker_ReplicateClient, len(route.Brokers)),
		sendErrs:      make([]error, len(route.Brokers)),
		readBarrierCh: make(chan struct{}),
		recvResp:      make([]pb.ReplicateResponse, len(route.Brokers)),
		recvErrs:      make([]error, len(route.Brokers)),
	}
	close(pln.readBarrierCh)

	for i, b := range route.Brokers {
		if i == int(route.Primary) {
			continue
		}
		var conn *grpc.ClientConn

		if conn, pln.sendErrs[i] = connFn(b); pln.sendErrs[i] == nil {
			pln.streams[i], pln.sendErrs[i] = pb.NewBrokerClient(conn).Replicate(ctx)
		}
	}
	return pln
}

// scatter asynchronously applies the ReplicateRequest to all replicas.
func (pln *pipeline) scatter(r *pb.ReplicateRequest) {
	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			pln.sendErrs[i] = s.Send(r)
		}
	}
	if i := pln.route.Primary; pln.sendErrs[i] == nil {
		var resp pb.ReplicateResponse
		resp, pln.sendErrs[i] = pln.spool.Apply(r)

		if resp.Status != pb.Status_OK {
			// Must never happen, since proposals are derived from local Spool Fragment.
			panic(resp.String())
		}
	}
}

// closeSend closes the send-side of all replica connections.
func (pln *pipeline) closeSend(spoolCh chan<- fragment.Spool) {
	// Apply a Spool commit which rolls back any partial content.
	pln.spool.Apply(&pb.ReplicateRequest{
		Proposal: &pln.spool.Fragment.Fragment,
	})
	pln.spool.Primary = false
	spoolCh <- pln.spool // Release ownership of Spool.

	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			pln.sendErrs[i] = s.CloseSend()
		}
	}
}

// sendErr returns the first encountered send-side error.
func (pln *pipeline) sendErr() error {
	for i, err := range pln.sendErrs {
		if err != nil {
			return fmt.Errorf("send to %s: %s", pln.route.Brokers[i], err)
		}
	}
	return nil
}

// gather synchronously receives a ReplicateResponse from all replicas.
func (pln *pipeline) gather() {
	for i, s := range pln.streams {
		if s != nil && pln.recvErrs[i] == nil {
			pln.recvErrs[i] = s.RecvMsg(&pln.recvResp[i])
		}
	}
}

// gatherOK calls gather, and treats any non-OK response status as an error.
func (pln *pipeline) gatherOK() {
	pln.gather()

	for i, s := range pln.streams {
		if s == nil || pln.recvErrs[i] != nil {
			// Pass.
		} else if pln.recvResp[i].Status != pb.Status_OK {
			pln.recvErrs[i] = fmt.Errorf("unexpected !OK response: %s", pln.recvResp[i])
		}
	}
}

// gatherEOF synchronously gathers expected EOFs from all replicas.
// An unexpected received message is treated as an error.
func (pln *pipeline) gatherEOF() {
	for i, s := range pln.streams {
		if s == nil || pln.recvErrs[i] != nil {
			continue
		}
		var msg, err = s.Recv()

		if err == io.EOF {
			// Pass.
		} else if err != nil {
			pln.recvErrs[i] = err
		} else {
			pln.recvErrs[i] = fmt.Errorf("unexpected response: %s", msg.String())
		}
	}
}

// recvErr returns the first encountered receive-side error.
func (pln *pipeline) recvErr() error {
	for i, err := range pln.recvErrs {
		if err != nil {
			return fmt.Errorf("recv from %s: %s", pln.route.Brokers[i], err)
		}
	}
	return nil
}

func (pln *pipeline) readBarrier() (waitFor <-chan struct{}, closeAfter chan<- struct{}) {
	waitFor, pln.readBarrierCh = pln.readBarrierCh, make(chan struct{})
	closeAfter = pln.readBarrierCh
	return
}

func (pln *pipeline) sync(proposal pb.Fragment) (rollToOffset, readThroughRev int64, err error) {
	pln.scatter(&pb.ReplicateRequest{
		Journal:     pln.spool.Journal,
		Route:       &pln.route,
		Proposal:    &proposal,
		Acknowledge: true,
	})
	pln.gather()

	for i, s := range pln.streams {
		if s == nil || pln.recvErrs[i] != nil {
			continue
		}

		switch resp := pln.recvResp[i]; resp.Status {
		case pb.Status_OK:
			// Pass.
		case pb.Status_WRONG_ROUTE:
			if !resp.Route.Equivalent(&pln.route) && resp.Route.Revision > pln.route.Revision {
				// Peer has a non-equivalent Route at a later Etcd revision.
				if resp.Route.Revision > readThroughRev {
					readThroughRev = resp.Route.Revision
				}
			} else {
				pln.recvErrs[i] = fmt.Errorf("unexpected Route mismatch: %s (remote) vs %s (local)",
					resp.Route, &pln.route)
			}

		case pb.Status_FRAGMENT_MISMATCH:
			if resp.Fragment.Begin != pln.spool.Fragment.Begin &&
				resp.Fragment.End >= pln.spool.Fragment.End {
				// Peer has a Fragment at matched or larger End offset, and with a
				// differing Begin offset.
				if rollToOffset < resp.Fragment.End {
					rollToOffset = resp.Fragment.End
				}
			} else {
				pln.recvErrs[i] = fmt.Errorf("unexpected Fragment mismatch: %s (remote) vs %s (local)",
					resp.Fragment, &pln.spool.Fragment.Fragment)
			}

		default:
			pln.recvErrs[i] = fmt.Errorf("unexpected Status: %s", resp)
		}
	}

	if err = pln.recvErr(); err == nil {
		err = pln.sendErr()
	}
	return
}
