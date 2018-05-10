package broker

import (
	"context"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
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
		panic("newTransaction requires Route with Primary != -1")
	}

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

func (pln *pipeline) scatter(r *pb.ReplicateRequest) {
	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			pln.sendErrs[i] = s.Send(r)
		}
	}
}

func (pln *pipeline) closeSend() {
	for i, s := range pln.streams {
		if s != nil && pln.sendErrs[i] == nil {
			pln.sendErrs[i] = s.CloseSend()
		}
	}
}

func (pln *pipeline) sendErr() error {
	for i, err := range pln.sendErrs {
		if err != nil {
			return fmt.Errorf("send to peer %s: %s", pln.route.Brokers[i], err)
		}
	}
	return nil
}

func (pln *pipeline) gather() {
	for i, s := range pln.streams {
		if s != nil && pln.recvErrs[i] == nil {
			pln.recvErrs[i] = s.RecvMsg(&pln.recvResp[i])
		}
	}
}

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

func (pln *pipeline) recvErr() error {
	for i, err := range pln.recvErrs {
		if err != nil {
			return fmt.Errorf("recv from peer %s: %s", pln.route.Brokers[i], err)
		}
	}
	return nil
}

func (pln *pipeline) readBarrier() (waitFor <-chan struct{}, closeAfter chan<- struct{}) {
	waitFor, pln.readBarrierCh = pln.readBarrierCh, make(chan struct{})
	closeAfter = pln.readBarrierCh
	return
}

func (pln *pipeline) sync() (rollToOffset, readThroughRev int64, err error) {
	// Perform a trivial commit of the current Fragment, to sync all peers.
	pln.scatter(&pb.ReplicateRequest{
		Journal: pln.spool.Journal,
		Route:   &pln.route,
		Commit:  &pln.spool.Fragment.Fragment,
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
					resp.Route, pln.route)
			}

		case pb.Status_WRONG_WRITE_HEAD:
			if resp.Fragment.Begin != pln.spool.Fragment.Begin &&
				resp.Fragment.End >= pln.spool.Fragment.End {
				// Peer has a Fragment at matched or larger End offset, and with a
				// differing Begin offset.
				if rollToOffset < resp.Fragment.End {
					rollToOffset = resp.Fragment.End
				}
			} else {
				pln.recvErrs[i] = fmt.Errorf("unexpected Fragment mismatch: %s (remote) vs %s (local)",
					resp.Fragment, pln.spool.Fragment.Fragment)
			}

		default:
			pln.recvErrs[i] = fmt.Errorf("unexpected Status: %s", resp)
		}
	}

	if err = pln.sendErr(); err == nil {
		err = pln.recvErr()
	}
	return
}

func (pln *pipeline) close(spoolCh chan<- fragment.Spool) {
	pln.spool.Rollback()
	spoolCh <- pln.spool // Release ownership of Journal Spool.

	pln.closeSend()
	if err := pln.sendErr(); err != nil {
		log.WithField("err", err).Warn("failed to send pipeline close")
	}

	<-pln.readBarrierCh
	pln.gatherEOF()

	if err := pln.recvErr(); err != nil {
		log.WithField("err", err).Warn("failed to read pipeline close")
	}
}

/*
func (pln *pipeline) scatterCommit() {

	if err := pln.spool.Commit(); err != nil {
		log.WithFields(log.Fields{"err": err, "fragment": pln.spool.Fragment.String()}).
			Warn("pln.scatterPropose: local Commit failed")

		// Error invalidates |txn.spool| for further writes. We must Roll it.
		// txn.spool.Roll(txn.replica.spec(), txn.spool.Fragment.End) ?????
		// txn.sendFailed = true
		// return ???
	}

}

func (txn *pipeline) gatherResponse() {
}

// begin the pipeline. Initialize a replication stream to each replica peer,
// collecting responses from each to verify the Route and NextOffset.
func (txn *pipeline) begin(connFn buildConnFn) {
	if err := prepareSpool(&txn.spool, txn.replica); err != nil {
		log.WithField("err", err).Warn("txn.begin: failed to prepare spool")
		txn.failed = true
		return
	}

	var req = &pb.ReplicateRequest{
		Journal:    txn.replica.spec().Name,
		Route:      &txn.replica.route,
		NextOffset: txn.spool.Fragment.End,
	}
	var resp = new(pb.ReplicateResponse)

	// Scatter a ReplicateRequest out to all replicas to begin a pipeline.
	// They will independently verify Route and NextOffset.
	for i, b := range txn.replica.route.Brokers {
		if i == int(txn.replica.route.Primary) {
			continue
		}

		var err error
		var conn *grpc.ClientConn
		var s pb.Broker_ReplicateClient

		if conn, err = connFn(b); err == nil {
			if s, err = pb.NewBrokerClient(conn).Replicate(txn.replica.ctx); err == nil {
				err = s.Send(req)
			}
		}

		if err != nil {
			log.WithFields(log.Fields{"err": err, "req": req.String()}).
				Warn("txn.begin: request failed")
			txn.failed = true
		} else {
			txn.streams = append(txn.streams, s)
		}
	}

	// Gather ReplicateResponses from each replica.
	for i, s := range txn.streams {
		var err = s.RecvMsg(resp)

		if err == nil {
			if err = resp.Validate(); err == nil && resp.Status != pb.Status_OK {
				err = errors.New(resp.Status.String())

				if resp.Status == pb.Status_WRONG_WRITE_HEAD && resp.WriteHead > txn.spool.Fragment.End {
					// Roll forward to the response WriteHead. This pipeline
					// has failed, but the next attempt may now succeed.
					txn.spool.Roll(txn.replica.spec(), resp.WriteHead)
				}
			}
		}

		if err != nil {
			log.WithFields(log.Fields{"err": err, "resp": resp.String()}).
				Warn("txn.begin: response failed")
			txn.streams[i] = nil
			txn.failed = true
		}

		if tr, ok := trace.FromContext(txn.replica.ctx); ok {
			tr.LazyPrintf("pipeline.begin: %v (err: %v)", resp, err)
		}
	}
}

// commitOrAbort commits the first |nCommit| bytes of the replicated pipeline
// if the pipeline hasn't failed (|txn.err| is nil), or aborts by committing
// zero bytes. |txn.doneCh| is closed when finished, and |txn.err| holds a
// encountered error.
func (txn *pipeline) commitOrAbort() {
	if txn.doneCh == nil {
		panic("already committed")
	}

	var req = &pb.ReplicateRequest{
		Content: nil, // Signal that the txn is committing.
		Commit:  txn.nCommit,
	}
	var resp = new(pb.ReplicateResponse)

	if txn.failed {
		req.Commit = 0
	}

	// Scatter a ReplicateRequest out to remaining replicas to commit
	// (or abort) the pipeline.
	for i, s := range txn.streams {
		if s == nil {
			continue
		}
		var err = s.Send(req)

		if err == nil {
			err = s.CloseSend()
		}
		if err != nil {
			log.WithFields(log.Fields{"err": err, "req": req.String()}).
				Warn("txn.commitOrAbort: request failed")
			txn.streams[i] = nil
			txn.failed = true
		}
	}

	// Commit locally. This may involve relatively expensive compression, which
	// now happens in parallel to our round-trip commit messages with replicas.
	if !txn.failed {
		if err := txn.spool.Commit(txn.nCommit); err != nil {
			log.WithFields(log.Fields{"err": err, "fragment": txn.spool.Fragment.String()}).
				Warn("txn.commitOrAbort: local Commit failed")

			// Error invalidates |txn.spool| for further writes. We must Roll it.
			txn.spool.Roll(txn.replica.spec(), txn.spool.Fragment.End)
			txn.failed = true
		}
	}

	// Gather stream EOFs, which denote a successful commitOrAbort.
	for _, s := range txn.streams {
		if s == nil {
			continue
		}
		var err = s.RecvMsg(resp)

		if err == io.EOF {
			continue // Success.
		} else if err == nil {
			// We don't expect a message to be sent by the replica.
			log.WithField("resp", resp.String()).Warn("txn.commitOrAbort: unexpected response")
		} else {
			log.WithField("err", err).Warn("txn.commitOrAbort: response failed")
		}
		txn.failed = true
	}
	close(txn.doneCh)
	txn.doneCh = nil
}

*/
