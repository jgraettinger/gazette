package broker

import (
	"context"
	"fmt"
	"io"

	"github.com/LiveRamp/gazette/pkg/client"
	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// pipeline is an in-flight write replication pipeline of a journal.
type pipeline struct {
	pb.Header                                 // Header of the pipeline.
	spool         fragment.Spool              // Local, primary replication Spool.
	returnCh      chan<- fragment.Spool       // |spool| return channel.
	streams       []pb.Broker_ReplicateClient // Established streams to each replication peer.
	sendErrs      []error                     // First error on send from each peer.
	readBarrierCh chan struct{}               // Coordinates hand-off of receive-side of the pipeline.
	recvResp      []pb.ReplicateResponse      // Most recent response gathered from each peer.
	recvErrs      []error                     // First error on receive from each peer.

	// readThroughRev, if set, indicates that a pipeline cannot be established
	// until we have read through (and our Route reflects) this etcd revision.
	readThroughRev int64
}

// newPipeline returns a new pipeline.
func newPipeline(ctx context.Context, hdr pb.Header, spool fragment.Spool, returnCh chan<- fragment.Spool, dialer client.Dialer) *pipeline {
	if hdr.Route.Primary == -1 {
		panic("dial requires Route with Primary != -1")
	}
	// Enable compression while the Spool is serving as primary within a pipeline.
	spool.Primary = true

	var R = len(hdr.Route.Brokers)

	var pln = &pipeline{
		Header:        hdr,
		spool:         spool,
		returnCh:      returnCh,
		streams:       make([]pb.Broker_ReplicateClient, R),
		sendErrs:      make([]error, R),
		readBarrierCh: make(chan struct{}),
		recvResp:      make([]pb.ReplicateResponse, R),
		recvErrs:      make([]error, R),
	}
	close(pln.readBarrierCh)

	for i, b := range pln.Route.Brokers {
		if i == int(pln.Route.Primary) {
			continue
		}
		var conn *grpc.ClientConn

		if conn, pln.sendErrs[i] = dialer.Dial(ctx, b, pln.Route); pln.sendErrs[i] == nil {
			pln.streams[i], pln.sendErrs[i] = pb.NewBrokerClient(conn).Replicate(ctx)
		}
	}
	return pln
}

// synchronize all pipeline peers by scattering proposals and gathering peer
// responses. On disagreement, synchronize will iteratively update the proposal
// if it's possible to do so and reach agreement. If peers disagree on Etcd
// revision, synchronize will close the pipeline and set |readThroughRev|.
func (pln *pipeline) synchronize() error {
	var proposal = pln.spool.Fragment.Fragment

	for {
		pln.scatter(&pb.ReplicateRequest{
			Header:      &pln.Header,
			Journal:     pln.spool.Journal,
			Proposal:    &proposal,
			Acknowledge: true,
		})
		var rollToOffset, readThroughRev = pln.gatherSync(proposal)

		var err = pln.recvErr()
		if err == nil {
			err = pln.sendErr()
		}

		if err != nil {
			pln.closeSend()
			pln.gatherEOF()
			return err
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
			// Peer has a non-equivalent Route at a later etcd revision. Close the
			// pipeline, and set its |readThroughRev| as an indication to other RPCs
			// of the revision which must first be read through before attempting
			// another pipeline.
			pln.closeSend()
			pln.gatherEOF()
			pln.readThroughRev = readThroughRev
		}
		return nil
	}
}

// scatter asynchronously applies the ReplicateRequest to all replicas.
func (pln *pipeline) scatter(r *pb.ReplicateRequest) {
	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			if r.Header != nil {
				// Copy and update to peer BrokerId.
				r.Header = boxHeaderBroker(*r.Header, pln.Route.Brokers[i])
			}
			pln.sendErrs[i] = s.Send(r)
		}
	}
	if i := pln.Route.Primary; pln.sendErrs[i] == nil {
		var resp pb.ReplicateResponse
		resp, pln.sendErrs[i] = pln.spool.Apply(r)

		if resp.Status != pb.Status_OK {
			// Must never happen, since proposals are derived from local Spool Fragment.
			panic(resp.String())
		}
	}
}

// sendErr returns the first encountered send-side error.
func (pln *pipeline) sendErr() error {
	for i, err := range pln.sendErrs {
		if err != nil {
			return fmt.Errorf("send to %s: %s", &pln.Route.Brokers[i], err)
		}
	}
	return nil
}

// barrier installs a new barrier in the pipeline. Clients should:
//   * Invoke readBarrier after issuing all sent writes, and release the
//     pipeline for other clients.
//   * Block until |waitFor| is selectable.
//   * Read expected responses from the pipeline.
//   * Close |closeAfter|.
// By following this convention a pipeline can safely be passed among multiple
// clients, each performing writes followed by reads, while allowing for those
// writes and reads to happen concurrently.
func (pln *pipeline) barrier() (waitFor <-chan struct{}, closeAfter chan<- struct{}) {
	waitFor, pln.readBarrierCh = pln.readBarrierCh, make(chan struct{})
	closeAfter = pln.readBarrierCh
	return
}

// closeSend closes the send-side of all replica connections. If |release|,
// this goroutine's lock on the pipeline is released, allowing a new pipeline
// to be constructed. The caller must wait for |waitFor| before gathering EOF
// responses from peers.
func (pln *pipeline) closeSend() {
	// Apply a Spool commit which rolls back any partial content.
	pln.spool.Apply(&pb.ReplicateRequest{
		Proposal: &pln.spool.Fragment.Fragment,
	})

	pln.spool.Primary = false
	pln.returnCh <- pln.spool // Release ownership of Spool.

	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			pln.sendErrs[i] = s.CloseSend()
		}
	}
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
			pln.recvErrs[i] = fmt.Errorf("unexpected !OK response: %s", &pln.recvResp[i])
		}
	}
}

// gatherSync calls gather, extracts and returns a peer-advertised future offset
// or etcd revision to read through relative to |proposal|, and treats any other
// non-OK response status as an error.
func (pln *pipeline) gatherSync(proposal pb.Fragment) (rollToOffset, readThroughRev int64) {
	pln.gather()

	for i, s := range pln.streams {
		if s == nil || pln.recvErrs[i] != nil {
			continue
		}

		switch resp := pln.recvResp[i]; resp.Status {
		case pb.Status_OK:
			// Pass.
		case pb.Status_WRONG_ROUTE:
			if !resp.Header.Route.Equivalent(&pln.Route) && resp.Header.Etcd.Revision > pln.Etcd.Revision {
				// Peer has a non-equivalent Route at a later etcd revision.
				if resp.Header.Etcd.Revision > readThroughRev {
					readThroughRev = resp.Header.Etcd.Revision
				}
			} else {
				pln.recvErrs[i] = fmt.Errorf("unexpected WRONG_ROUTE: %s (remote) vs %s (local)",
					resp.Header, &pln.Header)
			}

		case pb.Status_FRAGMENT_MISMATCH:
			if resp.Fragment.Begin != proposal.Begin &&
				resp.Fragment.End >= proposal.End {
				// Peer has a Fragment at matched or larger End offset, and with a
				// differing Begin offset.
				if rollToOffset < resp.Fragment.End {
					rollToOffset = resp.Fragment.End
				}
			} else {
				pln.recvErrs[i] = fmt.Errorf("unexpected FRAGMENT_MISMATCH: %s (remote) vs %s (local)",
					resp.Fragment, &pln.spool.Fragment.Fragment)
			}

		default:
			pln.recvErrs[i] = fmt.Errorf("unexpected Status: %s", resp)
		}
	}
	return
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
			return fmt.Errorf("recv from %s: %s", &pln.Route.Brokers[i], err)
		}
	}
	return nil
}

func boxHeaderBroker(hdr pb.Header, id pb.BrokerSpec_ID) *pb.Header {
	var out = new(pb.Header)
	*out = hdr
	out.BrokerId = id
	return out
}
