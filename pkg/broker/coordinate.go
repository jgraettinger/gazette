package broker

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"time"

	"github.com/LiveRamp/gazette/pkg/fragment"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// acquireTxn returns a ready transaction, or returns an Etcd revision which
// should be read through before attempting a transaction, or returns an error.
func acquireTxn(ctx context.Context, r *replica, connFn buildConnFn) (*transaction, int64, error) {
	// Acquire sole ownership of the replica Spool, and
	// possibly of an ongoing transaction which wraps it.
	var spool fragment.Spool
	var txn *transaction

	select {
	case <-r.ctx.Done():
		return nil, 0, r.ctx.Err() // Journal cancelled.
	case <-ctx.Done():
		return nil, 0, ctx.Err() // Context cancelled.

	case txn = <-r.txnHandoffCh:
		// We now hold an ongoing transaction and its wrapped replica Spool.
		if txn.route.Revision < r.route.Revision && !r.route.Equivalent(&txn.route) {
			// The transaction was established with a non-equivalent Route which is
			// older than our own. Close this transaction, and recurse to begin anew.
			closeTxn(r, txn)
			return acquireTxn(ctx, r, connFn)
		}
		return txn, 0, nil // Return ready transaction.

	case spool = <-r.spoolCh:
		// We now hold the replica Spool. There is no current transaction;
		// and we must begin one.
	}

	if err := prepareSpool(&spool, r); err != nil {
		r.spoolCh <- spool // Release ownership of |spool|.
		return nil, 0, err
	}
	txn = newTransaction(r.ctx, r.route, spool, connFn)

	// Perform a trivial commit of the current Fragment, to sync all peers.
	txn.scatter(&pb.ReplicateRequest{
		Journal: spool.Journal,
		Route:   &r.route,
		Commit:  &spool.Fragment.Fragment,
	})
	txn.gather()

	var err = txn.sendRecvErr()

	for i := 0; err == nil && i != len(txn.recvResp); i++ {
		switch resp := txn.recvResp[i]; resp.Status {
		case pb.Status_OK:
			continue
		case pb.Status_WRONG_ROUTE:
			if !resp.Route.Equivalent(&r.route) && resp.Route.Revision > r.route.Revision {
				// Peer has a non-equivalent Route at a later Etcd revision. We should
				// process this RPC only after reading through that revision.
				closeTxn(r, txn)
				return nil, resp.Route.Revision, nil
			}
			err = fmt.Errorf("unexpected Route mismatch: %s (broker %s) vs %s (local)",
				resp.Route, txn.route.Brokers[i], r.route)

		case pb.Status_WRONG_WRITE_HEAD:
			if txn.recvResp[i].Fragment.Begin != txn.spool.Fragment.Begin &&
				txn.recvResp[i].Fragment.End >= txn.spool.Fragment.End {
				// Peer has a Fragment at matched or larger End offset, and with a
				// differing Begin offset. Roll the Spool forward to a new & empty
				// Fragment at the maximal offset. We can attempt the transaction
				// again, and this time all peers should agree on the new Fragment.
				txn.spool.Roll(r.spec(), txn.recvResp[i].Fragment.End)

				closeTxn(r, txn)
				return acquireTxn(ctx, r, connFn)
			}
			err = fmt.Errorf("unexpected Fragment mismatch: %s (broker %s) vs %s (local)",
				resp.Fragment, txn.route.Brokers[i], txn.spool.Fragment.Fragment)
		}
	}

	if err != nil {
		closeTxn(r, txn)
		txn = nil
	}
	return txn, 0, err
}

func closeTxn(r *replica, txn *transaction) {
	txn.closeSend()
	<-txn.readBarrierCh
	txn.gatherEOF()

	if err := txn.sendRecvErr(); err != nil {
		log.WithField("err", err).Warn("failed to close transaction")
	}

	txn.spool.Rollback()
	r.spoolCh <- txn.spool // Release ownership of Journal Spool.
}

func handOffTxn(r *replica, txn *transaction) bool {
	select {
	case r.txnHandoffCh <- txn:
		return true
	default:
		return false
	}
}

func coordinate(srv pb.Broker_AppendServer, r *replica, txn *transaction, etcd *clientv3.Client) (*pb.AppendResponse, error) {
	// |frag| defines the Fragment of Journal which records the Append content.
	var frag = &pb.Fragment{
		Journal: txn.spool.Fragment.Journal,
		Begin:   txn.spool.Fragment.End,
		End:     txn.spool.Fragment.End,
	}
	var summer = sha1.New()

	var err error
	var req = new(pb.AppendRequest)

	for {
		if err = srv.RecvMsg(req); err == io.EOF {
			err = nil // Reached end-of-input for this Append stream.
			break
		} else if err == nil {
			err = req.Validate()
		}

		if err != nil {
			// A client-side read error occurred. The transaction is still in a good
			// state, but any partial spooled content must be rolled back.
			txn.scatter(&pb.ReplicateRequest{Rollback: true})
			txn.spool.Rollback()

			if !handOffTxn(r, txn) {

			}
		}

		// Multiplex content to each replication stream, and the local Spool.
		txn.scatter(&pb.ReplicateRequest{Content: req.Content})

		if err := txn.sendErr(); err != nil {
			closeTxn(r, txn)
			return nil, err
		}
		if err = txn.spool.Append(req.Content); err != nil {
			closeTxn(r, txn)
			return nil, err
		}
		_, _ = summer.Write(req.Content) // Cannot error.
		frag.End += int64(len(req.Content))
	}

	// Commit the Append, by scattering the next Fragment to be committed
	// to each peer. They will inspect & validate the Fragment locally,
	// and commit or return an error.
	var next = new(pb.Fragment)
	*next = txn.spool.Next()

	txn.scatter(&pb.ReplicateRequest{
		Journal: txn.spool.Journal,
		Route:   &txn.route,
		Commit:  next,
	})
	if err := txn.sendErr(); err != nil {
		closeTxn(r, txn)
		return nil, err
	}

	// Commit locally. This may involve relatively expensive compression, which
	// now happens in parallel to our round-trip commit messages with replicas.
	if err = txn.spool.Commit(); err != nil {
		closeTxn(r, txn)
		return nil, err
	}

	var waitFor, closeAfter = txn.readBarrier()
	var mustClose = !handOffTxn(r, txn)

	// There may be pipelined commits prior to this one, who have not yet read
	// their responses. Block until they do so, such that our responses are the
	// next to receive. Similarly, defer a close to signal to RPCs pipelined
	// after this one, that they may in turn read their responses.
	<-waitFor

	defer func() {
		close(closeAfter)

		if mustClose {
			closeTxn(r, txn)
		}
	}()

	txn.gatherOK()
	if err := txn.recvErr(); err != nil {
		return nil, err
	}

	// Ensure the Route of this transaction matches our Etcd-announced one.
	r.maybeUpdateAssignmentRoute(etcd)

	frag.Sum = pb.SHA1SumFromDigest(summer.Sum(nil))
	return &pb.AppendResponse{
		Status: pb.Status_OK,
		Route:  &r.route,
		Commit: frag,
	}, nil
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
