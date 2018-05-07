package broker

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// transaction is an in-flight replicated write transaction of a journal.
type transaction struct {
	route    pb.Route
	spool    fragment.Spool
	streams  []pb.Broker_ReplicateClient
	sendErrs []error
	recvResp []pb.ReplicateResponse
	recvErrs []error
}

// newTransaction returns a new transaction.
func newTransaction(ctx context.Context, route pb.Route, spool fragment.Spool, connFn buildConnFn) *transaction {
	if route.Primary == -1 {
		panic("newTransaction requires Route with Primary != -1")
	}

	var txn = &transaction{
		route:    route,
		spool:    spool,
		streams:  make([]pb.Broker_ReplicateClient, len(route.Brokers)),
		sendErrs: make([]error, len(route.Brokers)),
		recvResp: make([]pb.ReplicateResponse, len(route.Brokers)),
		recvErrs: make([]error, len(route.Brokers)),
	}

	for i, b := range route.Brokers {
		if i == int(route.Primary) {
			continue
		}
		var conn *grpc.ClientConn

		if conn, txn.sendErrs[i] = connFn(b); txn.sendErrs[i] == nil {
			txn.streams[i], txn.sendErrs[i] = pb.NewBrokerClient(conn).Replicate(ctx)
		}
	}
	return txn
}

func (txn *transaction) scatter(r *pb.ReplicateRequest) {
	for i, s := range txn.streams {
		if s != nil && txn.sendErrs[i] == nil {
			txn.sendErrs[i] = s.Send(r)
		}
	}
}

func (txn *transaction) closeSend() {
	for i, s := range txn.streams {
		if s != nil && txn.sendErrs[i] == nil {
			txn.sendErrs[i] = s.CloseSend()
		}
	}
}

func (txn *transaction) gatherResp() {
	for i, s := range txn.streams {
		if s != nil && txn.recvErrs[i] == nil {
			txn.recvErrs[i] = s.RecvMsg(&txn.recvResp[i])
		}
	}
}

func (txn *transaction) gatherEOF() {
	for i, s := range txn.streams {
		if s != nil && txn.recvErrs[i] == nil {
			var msg, err = s.Recv()

			if err == io.EOF {
				// Pass.
			} else if err != nil {
				txn.recvErrs[i] = err
			} else {
				txn.recvErrs[i] = fmt.Errorf("unexpected ReplicateResponse: %s", msg.String())
			}
		}
	}
}

func (txn *transaction) anyErr() (pb.BrokerSpec_ID, error) {
	for i, e := range txn.sendErrs {
		if e != nil {
			return txn.route.Brokers[i], e
		}
	}
	for i, e := range txn.recvErrs {
		if e != nil {
			return txn.route.Brokers[i], e
		}
	}
	return pb.BrokerSpec_ID{}, nil
}

func (txn *transaction) scatterCommit() {

	if err := txn.spool.Commit(); err != nil {
		log.WithFields(log.Fields{"err": err, "fragment": txn.spool.Fragment.String()}).
			Warn("txn.scatterPropose: local Commit failed")

		// Error invalidates |txn.spool| for further writes. We must Roll it.
		// txn.spool.Roll(txn.replica.spec(), txn.spool.Fragment.End) ?????
		// txn.sendFailed = true
		// return ???
	}

}

func (txn *transaction) gatherResponse() {
}

// begin the transaction. Initialize a replication stream to each replica peer,
// collecting responses from each to verify the Route and NextOffset.
func (txn *transaction) begin(connFn buildConnFn) {
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

	// Scatter a ReplicateRequest out to all replicas to begin a transaction.
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
					// Roll forward to the response WriteHead. This transaction
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
			tr.LazyPrintf("transaction.begin: %v (err: %v)", resp, err)
		}
	}
}

// commitOrAbort commits the first |nCommit| bytes of the replicated transaction
// if the transaction hasn't failed (|txn.err| is nil), or aborts by committing
// zero bytes. |txn.doneCh| is closed when finished, and |txn.err| holds a
// encountered error.
func (txn *transaction) commitOrAbort() {
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
	// (or abort) the transaction.
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

// replicate reads stream of AppendRequest chunks from the client, and
// multiplexes them to all replicas. It returns the number of bytes read from
// the client, and any encountered read or write error. The transaction must
// not already be failed, or replicate panics.
func (txn *transaction) replicate(srv grpc.ServerStream) (sum pb.SHA1Sum, err error) {
	if txn.failed() {
		panic("transaction has already failed")
	}

	var summer = sha1.New()
	defer func() {
		sum = pb.SHA1SumFromDigest(summer.Sum(nil))
	}()

	var appendReq = new(pb.AppendRequest)
	var repReq = new(pb.ReplicateRequest)

	// TODO(johnny): Log but mask errors where no data was streamed to peers.

	for {
		if err = srv.RecvMsg(appendReq); err == io.EOF {
			err = nil // Reached end-of-input for this Append stream.
			return
		} else if err != nil {
			// Client read errors don't invalidate the transaction.
			// We may still commit previously & fully read Append streams.
			return
		} else if err = appendReq.Validate(); err != nil {
			return
		} else if l := int64(len(appendReq.Content)); l == 0 {
			// Empty chunks are allowed (though not expected). Ignore, and certainly
			// don't pass through as a ReplicateRequest, as that would be interpreted
			// as a commitOrAbort.
		} else {
			// Multiplex content to each replication stream, and the local Spool.
			repReq.Content = appendReq.Content
			n += l

			for _, s := range txn.streams {
				if err = s.Send(repReq); err != nil {
					txn.sendFailed = true
					return
				}
			}
			_, err = txn.spool.File.WriteAt(repReq.Content, repReq.NextOffset-txn.spool.Begin)
			if err != nil {
				txn.failed = true
				return
			}
		}
	}
}
