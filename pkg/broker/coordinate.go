package broker

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// coordinate establishes or obtains a current transaction and applies the client's
// Append RPC within it.
func coordinate(r *replica, _ *pb.AppendRequest, srv pb.Broker_AppendServer, connFn buildConnFn) (*pb.AppendResponse, error) {

	// Gate the request on completing an initial index load of remote Fragments.
	select {
	case <-r.initialLoadCh:
		// Pass.
	case <-r.ctx.Done():
		return nil, r.ctx.Err() // Journal cancelled.
	case <-srv.Context().Done():
		return nil, srv.Context().Err() // Request cancelled.
	}

	// Acquire sole ownership of the replica Spool, and a transaction which wraps it.
	var txn *transaction

	select {
	case txn = <-r.txnHandoffCh:
		// We now hold the replica Spool, as well as an ongoing transaction.
	case spool := <-r.spoolCh:
		// We now hold the replica Spool, but there is no current transaction. Begin one.
		txn = newTransaction(r, spool)
		txn.begin(connFn)

	case <-r.ctx.Done():
		return nil, r.ctx.Err() // Journal cancelled.
	case <-srv.Context().Done():
		return nil, srv.Context().Err() // Request cancelled.
	}

	var resp = &pb.AppendResponse{
		Status:      pb.Status_OK,
		Route:       &r.route,
		FirstOffset: txn.spool.Fragment.End + txn.nCommit,
		LastOffset:  txn.spool.Fragment.End + txn.nCommit, // Adjusted by |n|.
		WriteHead:   txn.spool.Fragment.End,               // Adjusted by |nCommit| after commit.
	}

	var n int64
	var err error

	if !txn.failed {
		if n, err = txn.replicate(srv); err != nil {
			log.WithFields(log.Fields{"err": err, "n": n}).Warn("txn.replicate failed")
		} else {
			resp.LastOffset += n
			txn.nCommit += n
		}
	}

	var maybeHandoffCh = r.txnHandoffCh
	var doneCh = txn.doneCh

	// We must commitOrAbort if:
	//  * The transaction failed (in which case we'll abort immediately)
	//  * A replicate error occurred n > 0 bytes into a client request.
	//  * We're over-threshold for the transaction size.
	if txn.failed || (err != nil && n > 0) || (txn.nCommit >= r.spec().TransactionSize) {
		maybeHandoffCh = nil // Cannot select.
	}

	// Attempt to hand off to another ready coordinate invocation.
	select {
	case maybeHandoffCh <- txn:
		<-doneCh // Wait for receiving goroutine to commitOrAbort.
	default:
		txn.commitOrAbort()
		r.spoolCh <- txn.spool // Release ownership of Journal Spool.
	}

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
