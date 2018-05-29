package broker

import (
	"context"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/fragment"
	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type replica interface {
	getRoute() *pb.Route

	transition(ks *keyspace.KeySpace, item, assignment keyspace.KeyValue, route pb.Route) replica
	cancel()

	serveRead(*pb.ReadRequest, pb.Broker_ReadServer) error
	serveAppend(*pb.AppendRequest, pb.Broker_AppendServer, dialer) (readThroughRev int64, err error)
	serveReplicate(*pb.ReplicateRequest, pb.Broker_ReplicateServer) error
}

type replicaImpl struct {
	journal keyspace.KeyValue
	// Local assignment of |replica|, motivating this replica instance.
	assignment keyspace.KeyValue
	// Current broker routing topology of the replica.
	route pb.Route

	// The following fields are held constant over the lifetime of a replica:

	// Context tied to processing lifetime of this replica by this broker.
	// Cancelled when this broker is no longer responsible for the replica.
	ctx        context.Context
	cancelFunc context.CancelFunc
	// Index of all known Fragments of the replica.
	index *fragment.Index
	// spoolCh synchronizes access to the single Spool of the replica.
	spoolCh chan fragment.Spool
	// pipelineCh synchronizes access to the single pipeline of the replica.
	pipelineCh chan *pipeline
}

func newReplicaImpl() replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var spoolCh = make(chan fragment.Spool, 1)
	spoolCh <- fragment.Spool{}

	var pipelineCh = make(chan *pipeline, 1)
	pipelineCh <- nil

	return &replicaImpl{
		// These are initialized on first transition().
		journal:    keyspace.KeyValue{},
		assignment: keyspace.KeyValue{},
		route:      pb.Route{},

		ctx:        ctx,
		cancelFunc: cancel,
		index:      fragment.NewIndex(ctx),
		spoolCh:    spoolCh,
		pipelineCh: pipelineCh,
	}
}

func (r *replicaImpl) getRoute() *pb.Route { return &r.route }

func (r *replicaImpl) cancel() { r.cancelFunc() }

// spec returns the replica's JournalSpec.
func (r *replicaImpl) spec() *pb.JournalSpec {
	return r.journal.Decoded.(v3_allocator.Item).ItemValue.(*pb.JournalSpec)
}

func (r *replicaImpl) serveReplicate(req *pb.ReplicateRequest, stream pb.Broker_ReplicateServer) error {
	var spool, err = r.acquireSpool(stream.Context(), false)
	if err != nil {
		return err
	}

	var resp = new(pb.ReplicateResponse)
	for {
		if *resp, err = spool.Apply(req); err != nil {
			return err
		}

		if req.Acknowledge {
			if err = stream.Send(resp); err != nil {
				return err
			}
		} else if resp.Status != pb.Status_OK {
			return fmt.Errorf("no ack requested but status != OK: %s", resp)
		}

		if err = stream.RecvMsg(req); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else if err = req.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (r *replicaImpl) serveAppend(req *pb.AppendRequest, stream pb.Broker_AppendServer, dialer dialer) (int64, error) {
	var pln, rev, err = r.acquirePipeline(stream.Context(), dialer)
	if pln == nil {
		return rev, err
	}

	// We now have sole ownership of the *send* side of the pipeline.

	var appender = beginAppending(pln, r.spec().Fragment)
	for appender.onRecv(req, stream.RecvMsg(req)) {
	}
	var waitFor, closeAfter = pln.readBarrier()
	var plnSendErr = pln.sendErr()

	if plnSendErr == nil {
		r.pipelineCh <- pln // Release the send side of |pln|.
	} else {
		pln.closeSend(r.spoolCh)
		r.pipelineCh <- nil

		log.WithFields(log.Fields{"err": err, "journal": r.spec().Name}).
			Warn("pipeline send failed")
	}

	// There may be pipelined commits prior to this one, who have not yet read
	// their responses. Block until they do so, such that our responses are the
	// next to receive. Similarly, defer a close to signal to RPCs pipelined
	// after this one, that they may in turn read their responses. When this
	// completes, we have sole ownership of the *receive* side of |pln|.
	<-waitFor
	defer func() { close(closeAfter) }()

	pln.gatherOK()

	if plnSendErr != nil {
		pln.gatherEOF()
	}

	if pln.recvErr() != nil {
		log.WithFields(log.Fields{"err": pln.recvErr(), "journal": r.spec().Name}).
			Warn("pipeline receive failed")
	}

	if appender.reqErr != nil {
		return 0, appender.reqErr
	} else if plnSendErr != nil {
		return 0, plnSendErr
	} else if pln.recvErr() != nil {
		return 0, pln.recvErr()
	} else {
		return 0, stream.SendAndClose(&pb.AppendResponse{
			Status: pb.Status_OK,
			Route:  &pln.route,
			Commit: appender.reqFragment,
		})
	}
}

func (r *replicaImpl) transition(ks *keyspace.KeySpace, item, assignment keyspace.KeyValue, route pb.Route) replica {
	// No need to transition if the |item|, |assignment|, and |route| are unchanged.
	if item.Raw.ModRevision == r.journal.Raw.ModRevision &&
		assignment.Raw.ModRevision == r.assignment.Raw.ModRevision &&
		r.route.Equivalent(&route) {
		return r
	}

	var clone = new(replicaImpl)
	*clone = *r

	clone.journal = item
	clone.assignment = assignment
	clone.route = route.Copy()
	clone.route.AttachEndpoints(ks)

	return clone

	/*
		if routeChanged && r.isPrimary() {
			log.Error("convergence not yet implemented!")
				// Issue an empty write to drive the quick convergence
				// of replica Route announcements in Etcd.
				if conn, err := rtr.peerConn(rtr.id); err == nil {
					go issueEmptyWrite(conn, name)
				} else {
					log.WithField("err", err).Error("failed to build loopback *grpc.ClientConn")
				}
			* /
		}
			/*
		func (rtr *resolverImpl) getJournalSpec(name pb.Journal) (*pb.JournalSpec, bool) {
		rtr.ks.Mu.RLock()
		defer rtr.ks.Mu.RUnlock()

		if item, ok := v3_allocator.LookupItem(rtr.ks, name.String()); ok {
		return item.ItemValue.(*pb.JournalSpec), true
		}
		return nil, false
		}
	*/
}

func (r *replicaImpl) acquireSpool(ctx context.Context, waitForRemoteLoad bool) (spool fragment.Spool, err error) {
	if waitForRemoteLoad {
		if err = r.index.WaitForFirstRemoteLoad(ctx); err != nil {
			return
		}
	}

	select {
	case <-ctx.Done():
		err = ctx.Err() // |ctx| cancelled.
		return
	case <-r.ctx.Done():
		err = r.ctx.Err() // replica cancelled.
		return
	case spool = <-r.spoolCh:
		// Pass.
	}

	if eo := r.index.EndOffset(); eo > spool.Fragment.End {
		// If the Fragment index knows of an offset great than the current Spool,
		// roll the Spool forward to the new offset.
		var proposal = spool.Fragment.Fragment
		proposal.Begin, proposal.End, proposal.Sum = eo, eo, pb.SHA1Sum{}

		var resp pb.ReplicateResponse
		resp, err = spool.Apply(&pb.ReplicateRequest{Proposal: &proposal})

		if err != nil || resp.Status != pb.Status_OK {
			// Cannot happen, as we crafted |proposal| relative to the |spool.Fragment|.
			panic(fmt.Sprintf("failed to roll to EndOffset: %s err %s", &resp, err))
		}
	}

	return
}

func (r *replicaImpl) acquirePipeline(ctx context.Context, dialer dialer) (*pipeline, int64, error) {
	var pln *pipeline
	var err error

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		return nil, 0, r.ctx.Err() // replica cancelled.
	case pln = <-r.pipelineCh:
		// Pass.
	}

	// If |pln| is a placeholder indicating the need to read through a revision
	// which we have since read through, clear it.
	if pln != nil && pln.readThroughRev != 0 && pln.readThroughRev <= r.route.Revision {
		pln = nil
	}

	// If |pln| is a valid pipeline but is built on a non-equivalent & older Route,
	// tear it down to begin a new one.
	if pln != nil && !pln.route.Equivalent(&r.route) && pln.route.Revision < r.route.Revision {
		if pln.closeSend(r.spoolCh); pln.sendErr() != nil {
			log.WithField("err", pln.sendErr()).Warn("tearing down pipeline: failed to closeSend")
		}

		// Block for, and read peer EOFs in a goroutine. This lets us start building the
		// new pipeline concurrently with the old one completing shutdown.
		go func(pln *pipeline) {
			<-pln.readBarrierCh

			if pln.gatherEOF(); pln.recvErr() != nil {
				log.WithField("err", pln.recvErr()).Warn("tearing down pipeline: failed to gatherEOF")
			}
		}(pln)

		pln = nil
	}

	if pln == nil {
		pln, err = r.startPipeline(ctx, dialer)
	}

	if err != nil {
		r.pipelineCh <- nil
		return nil, 0, err
	} else if pln.readThroughRev != 0 {
		r.pipelineCh <- pln
		return nil, pln.readThroughRev, nil
	}
	return pln, 0, nil
}

func (r *replicaImpl) startPipeline(ctx context.Context, dialer dialer) (*pipeline, error) {
	var pln *pipeline

	if spool, err := r.acquireSpool(ctx, true); err != nil {
		return nil, err
	} else {
		pln = newPipeline(r.ctx, r.route, spool, dialer)
	}

	var proposal = pln.spool.Fragment.Fragment

	for {
		pln.scatter(&pb.ReplicateRequest{
			Journal:     pln.spool.Journal,
			Route:       &pln.route,
			Proposal:    &proposal,
			Acknowledge: true,
		})
		var rollToOffset, readThroughRev, err = pln.gatherSync(proposal)

		if err != nil {
			pln.closeSend(r.spoolCh)
			pln.gatherEOF()
			return nil, err
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
			// Peer has a non-equivalent Route at a later Etcd revision. Close the
			// pipeline, and set its |readThroughRev| as an indication to other RPCs
			// of the revision which must first be read through before attempting
			// another pipeline.
			pln.closeSend(r.spoolCh)
			pln.gatherEOF()
			pln.readThroughRev = readThroughRev
		}
		return pln, nil
	}
}

/*
func (r *replica) maybeUpdateAssignmentRoute(etcd *clientv3.Client) {
	// |announced| is the Route currently recorded by this replica's Assignment.
	var announced = r.assignment.Decoded.(v3_allocator.Assignment).AssignmentValue.(*pb.Route)

	if r.route.Equivalent(announced) {
		return
	}

	// Copy |r.route|, stripping Endpoints (not required, but they're superfluous here).
	var next = r.route.Copy()
	next.Endpoints = nil

	// Attempt to update the current Assignment value. Two update attempts can
	// potentially race, if write transactions occur in close sequence and before
	// the local KeySpace is updated.
	go func(etcd *clientv3.Client, kv keyspace.KeyValue, next string) {
		var key = string(kv.Raw.Key)

		var _, err = etcd.Txn(context.Background()).If(
			clientv3.Compare(clientv3.ModRevision(key), "=", kv.Raw.ModRevision),
		).Then(
			clientv3.OpPut(key, next, clientv3.WithIgnoreLease()),
		).Commit()

		if err != nil {
			log.WithFields(log.Fields{"err": err, "key": key}).Warn("failed to update Assignment Route")
		}
	}(etcd, r.assignment, next.MarshalString())
}
*/
