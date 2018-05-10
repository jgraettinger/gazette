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

func acquireSpool(ctx context.Context, r *replica) (fragment.Spool, error) {
	// Block until we've completed a first load of remote Fragment metadata.
	if err := waitForInitialIndexLoad(ctx, r); err != nil {
		return fragment.Spool{}, err
	}
	var spool fragment.Spool

	select {
	case <-ctx.Done():
		return fragment.Spool{}, ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		return fragment.Spool{}, r.ctx.Err() // Journal cancelled.
	case spool = <-r.spoolCh:
		// Pass.
	}

	// Ensure the Spool reflects the maximal offset of the Fragment Index.
	if eo := r.index.endOffset(); eo > spool.Fragment.End {
		spool.Roll(r.spec(), eo)
	}
	return spool, nil
}

func startPipeline(ctx context.Context, r *replica, connFn buildConnFn) (*pipeline, error) {
	var spool, err = acquireSpool(ctx, r)
	if err != nil {
		return nil, err
	}
	var pln = newPipeline(r.ctx, r.route, spool, connFn)

	for {
		var rollToOffset, readThroughRev, err = pln.sync()

		if err != nil {
			pln.close(r.spoolCh)
			return nil, err
		}

		if rollToOffset != 0 {
			// Roll to the new offset, and sync the pipeline again. This
			// time all peers should agree on the new Fragment.
			pln.spool.Roll(r.spec(), rollToOffset)
			pln.scatter(&pb.ReplicateRequest{RollTo: rollToOffset})
			continue
		}

		if readThroughRev != 0 {
			// Peer has a non-equivalent Route at a later Etcd revision. Close the
			// pipeline, and set its |readThroughRev| as an indication to other RPCs
			// of the revision which must first be read through before attempting
			// another pipeline.
			pln.close(r.spoolCh)
			pln.readThroughRev = readThroughRev
		}
		return pln, nil
	}
}

func acquirePipeline(ctx context.Context, r *replica, connFn buildConnFn) (*pipeline, int64, error) {
	var pln *pipeline
	var err error

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		return nil, 0, r.ctx.Err() // Journal cancelled.
	case pln = <-r.txnHandoffCh:
		// Pass.
	}

	// Start a new pipeline if |pln| is nil, or if it's a placeholder indicating
	// the need to read through a revision which we have now read through.
	if pln == nil || pln.readThroughRev != 0 && pln.readThroughRev <= r.route.Revision {
		pln, err = startPipeline(ctx, r, connFn)
	}

	// Ensure our local spool is prepared to accept writes. As primary, we will
	// optimistically compress spool content as it commits (replica peers will not).
	if err == nil {
		err = pln.spool.Prepare(true)
	}

	if err != nil {
		r.txnHandoffCh <- pln
		return nil, 0, err
	} else if pln.readThroughRev != 0 {
		r.txnHandoffCh <- pln
		return nil, pln.readThroughRev, nil
	}
	return pln, 0, nil
}

func coordinate(srv pb.Broker_AppendServer, pln *pipeline, spec *pb.JournalSpec, etcd *clientv3.Client) (*pb.AppendResponse, error) {
	// |frag| defines the Fragment of Journal which captures the Append content.
	var frag = &pb.Fragment{
		Journal: pln.spool.Fragment.Journal,
		Begin:   pln.spool.Fragment.End,
		End:     pln.spool.Fragment.End,
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
			// A client-side read error occurred. The pipeline is still in a good
			// state, but any partial spooled content must be rolled back.
			pln.scatter(&pb.ReplicateRequest{RollTo: pln.spool.Fragment.End})
			pln.spool.Rollback() // ???

			return nil, err
		}

		// Multiplex content to each replication stream, and the local Spool.
		pln.scatter(&pb.ReplicateRequest{Content: req.Content})

		if err := pln.sendErr(); err != nil {
			closePipeline(r, txn)
			return nil, err
		}
		if err = txn.spool.Append(req.Content); err != nil {
			closePipeline(r, txn)
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
		closePipeline(r, txn)
		return nil, err
	}

	// Commit locally. This may involve relatively expensive compression, which
	// now happens in parallel to our round-trip commit messages with replicas.
	if err = txn.spool.Commit(); err != nil {
		closePipeline(r, txn)
		return nil, err
	}

	// If the current spool has reached our desired Fragment length,
	// roll it forward to a new Fragment and signal to our replicas
	// that we've done so.
	if pln.spool.Fragment.ContentLength() > r.spec().Fragment.Length {
		pln.scatter(&pb.ReplicateRequest{
			RollTo: pln.spool.Fragment.End,
		})
		pln.spool.Roll(r.spec(), pln.spool.Fragment.End)
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
			closePipeline(r, txn)
		}
	}()

	txn.gatherOK()
	if err := txn.recvErr(); err != nil {
		return nil, err
	}

	// Ensure the Route of this pipeline matches our Etcd-announced one.
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
