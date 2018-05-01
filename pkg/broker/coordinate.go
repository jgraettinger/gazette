package broker

import (
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

// coordinate establishes or obtains a current transaction and applies the client's
// Append RPC within it.
func coordinate(j *journal, _ *pb.AppendRequest, srv pb.Broker_AppendServer, buildConn buildConnFn) error {

	// Gate the request on completing an initial index load of remote Fragments.
	select {
	case <-j.initialLoadCh:
		// Pass.
	case <-j.ctx.Done():
		return j.ctx.Err() // Journal cancelled.
	case <-srv.Context().Done():
		return srv.Context().Err() // Request cancelled.
	}

	for {
		// Acquire sole ownership of the journal Spool, and a transaction which wraps it.
		var txn *transaction

		select {
		case txn = <-j.txnHandoffCh:
			// We now hold the journal Spool, as well as an ongoing transaction.
		case spool := <-j.spoolCh:
			// We now hold the journal Spool, but there is no current transaction. Begin one.
			txn = newTransaction(j, spool)
			txn.begin(buildConn)

		case <-j.ctx.Done():
			return j.ctx.Err() // Journal cancelled.
		case <-srv.Context().Done():
			return srv.Context().Err() // Request cancelled.
		}

		var resp = &pb.AppendResponse{
			Route:       &j.route,
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

		var maybeHandoffCh = j.txnHandoffCh
		var doneCh = txn.doneCh

		// We must commitOrAbort if:
		//  * The transaction failed (in which case we'll abort immediately)
		//  * A replicate error occurred n > 0 bytes into a client request.
		//  * We're over-threshold for the transaction size.
		if txn.failed || (err != nil && n > 0) || (txn.nCommit >= j.spec.TransactionSize) {
			maybeHandoffCh = nil // Cannot select.
		}

		// Attempt to hand off to another ready coordinate invocation.
		select {
		case maybeHandoffCh <- txn:
			<-doneCh // Wait for receiving goroutine to commitOrAbort.
		default:
			txn.commitOrAbort()
			j.spoolCh <- txn.spool // Release ownership of Journal Spool.
		}

		if err != nil {
			return err
		} else if txn.failed && n == 0 {
			// If the transaction failed and no bytes were actually read from the client,
			// than we may loop to retry the present client request with a new transaction.
		} else if txn.failed {
			// If bytes were read from the client, we cannot recover and must report an error.
			*resp = pb.AppendResponse{
				Status: pb.Status_REPLICATION_FAILED,
				Route:  &j.route,
			}
			return srv.SendAndClose(resp)
		} else {
			resp.WriteHead += txn.nCommit
			return srv.SendAndClose(&pb.AppendResponse{Status: pb.Status_OK})
		}
	}
}
