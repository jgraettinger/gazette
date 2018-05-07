package broker

import (
	"context"
	"crypto/sha1"
	"time"

	"github.com/LiveRamp/gazette/pkg/fragment"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

func beginTxn(ctx context.Context, r *replica, connFn buildConnFn) (*transaction, *pb.ReplicateResponse, error) {
	// Acquire sole ownership of the replica Spool, and
	// possibly of an ongoing transaction which wraps it.
	var txn *transaction
	var spool fragment.Spool

	select {
	case txn = <-r.txnHandoffCh:
		// We now hold the replica Spool, as well as an ongoing transaction.

		// Ensure the Route of the ongoing transaction matches our own. If not,
		// we need to close the current transaction and begin anew.
		if !r.route.Equivalent(&txn.route) {
			txn.closeSend()
			txn.gatherEOF()

			if bid, err := txn.anyErr(); err != nil {
				log.WithFields(log.Fields{"broker": bid, "err": err}).
					Warn("failed to close prior transaction")
			}
			spool = txn.spool
			txn = nil
		}

	case spool = <-r.spoolCh:
		// We now hold the replica Spool, but there is no current transaction.
	case <-r.ctx.Done():
		return nil, nil, r.ctx.Err() // Journal cancelled.
	case <-ctx.Done():
		return nil, nil, ctx.Err() // Request cancelled.
	}

	if txn != nil {
		// Return ready transaction.
		return txn, nil, nil
	}

	// There is no current transaction. Begin one.
	if err := prepareSpool(&spool, r); err != nil {
		return nil, nil, err
	}
	txn = newTransaction(r.ctx, r.route, spool, connFn)

	// Propose the current spool Fragment, to ensure all replicas are in sync.
	txn.scatter(&pb.ReplicateRequest{
		Journal: spool.Journal,
		Route:   &r.route,
		Commit:  &spool.Fragment.Fragment,
	})
	txn.gatherResp()

	if bid, err := txn.anyErr(); err != nil {
		pb.ExtendContext()

	}

		if err = txn.err(i); err != nil {
			log.WithFields(log.Fields{"err": txn.sendErrs[i], "broker": txn.route.Brokers[i]}).
				Warn("failed to start Replicate stream")

			txn.closeSend()
			txn = nil
			return
		}

	}

}

// coordinate establishes or obtains a current transaction and applies the client's
// Append RPC within it.
func coordinate(r *replica, _ *pb.AppendRequest, srv pb.Broker_AppendServer, connFn buildConnFn) (*pb.AppendResponse, error) {

	var sum pb.SHA1Sum
	var err error

	if !txn.failed() {
		if sum, err = txn.replicate(srv); err != nil {
			log.WithFields(log.Fields{"err": err}).Warn("txn.replicate failed")
		} else {
			txn.scatterCommit()
		}
	}

	var maybeHandoffCh = r.txnHandoffCh

	// We must commitOrAbort if:
	//  * The transaction failed (in which case we'll abort immediately)
	//  * A replicate error occurred n > 0 bytes into a client request.
	//  * We're over-threshold for the transaction size.
	if txn.failed() || err != nil || (txn.spool.ContentLength() >= r.spec().Fragment.Length) {
		maybeHandoffCh = nil // Cannot select.
	}

	// Attempt to hand off to another ready coordinate invocation.
	select {
	case maybeHandoffCh <- txn:
	default:
		txn.closeSend()
		r.spoolCh <- txn.spool // Release ownership of Journal Spool.
	}

	txn.gatherAck()

	// TODO - send stuff

	if err != nil {
		return nil, err
	} else if txn.failed {
		*resp = pb.AppendResponse{
			Status: pb.Status_REPLICATION_FAILED,
			Route:  &r.route,
		}
		return resp, srv.SendAndClose(resp)
	} else {
		resp.WriteHead += txn.nCommit
		return resp, srv.SendAndClose(resp)
	}
}

// issueEmptyWrite evaluates an Append RPC with no content.
func issueEmptyWrite(conn *grpc.ClientConn, journal pb.Journal) (resp *pb.AppendResponse, err error) {
	var backoff = time.Millisecond

	for {
		var srv pb.Broker_AppendClient
		var req = &pb.AppendRequest{Journal: journal}

		if srv, err = pb.NewBrokerClient(conn).Append(context.Background()); err == nil {
			if err = srv.Send(req); err == nil {
				resp, err = srv.CloseAndRecv()
			}
		}

		if err != nil {
			return
		}

		switch resp.Status {
		case pb.Status_REPLICATION_FAILED:
			time.Sleep(backoff)
			backoff = backoff * 2
		default:
			return
		}
	}
}
