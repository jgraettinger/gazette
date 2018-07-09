package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/LiveRamp/gazette/pkg/client"
	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type replica struct {
	journal pb.Journal
	// Context tied to processing lifetime of this replica by this broker.
	// Cancelled when this broker is no longer responsible for the replica.
	ctx    context.Context
	cancel context.CancelFunc
	// Index of all known Fragments of the replica.
	index *fragment.Index
	// spoolCh synchronizes access to the single Spool of the replica.
	spoolCh chan fragment.Spool
	// pipelineCh synchronizes access to the single pipeline of the replica.
	pipelineCh chan *pipeline
	// pulseCh is signaled on Etcd updates when the journal is determined
	// to be 1) a primary replica of the local broker, and 2) to have inconsistent
	// assignments in Etcd.
	pulseCh chan struct{}
}

func newReplica(journal pb.Journal) *replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var r = &replica{
		journal:    journal,
		ctx:        ctx,
		cancel:     cancel,
		index:      fragment.NewIndex(ctx),
		spoolCh:    make(chan fragment.Spool, 1),
		pipelineCh: make(chan *pipeline, 1),
	}

	r.spoolCh <- fragment.NewSpool(journal, struct {
		*fragment.Index
		*fragment.Persister
	}{r.index, sharedPersister})

	r.pipelineCh <- nil

	return r
}

func acquireSpool(ctx context.Context, r *replica, waitForRemoteLoad bool) (spool fragment.Spool, err error) {
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

func acquirePipeline(ctx context.Context, r *replica, hdr pb.Header, dialer client.Dialer) (*pipeline, int64, error) {
	var pln *pipeline

	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err() // |ctx| cancelled.
	case <-r.ctx.Done():
		return nil, 0, r.ctx.Err() // replica cancelled.
	case pln = <-r.pipelineCh:
		// Pass.
	}

	// Is |pln| is a placeholder indicating the need to read through a revision, which we've since read through?
	if pln != nil && pln.readThroughRev != 0 && pln.readThroughRev <= hdr.Etcd.Revision {
		pln = nil
	}

	// If |pln| is a valid pipeline but is built on a non-equivalent & older Route,
	// tear it down asynchronously and immediately begin a new one.
	if pln != nil && pln.readThroughRev == 0 &&
		!pln.Route.Equivalent(&hdr.Route) && pln.Etcd.Revision < hdr.Etcd.Revision {

		go func(pln *pipeline) {
			var waitFor, _ = pln.barrier()

			if pln.closeSend(); pln.sendErr() != nil {
				log.WithField("err", pln.sendErr()).Warn("tearing down pipeline: failed to closeSend")
			}
			<-waitFor

			if pln.gatherEOF(); pln.recvErr() != nil {
				log.WithField("err", pln.recvErr()).Warn("tearing down pipeline: failed to gatherEOF")
			}
		}(pln)

		pln = nil
	}

	var err error

	if pln == nil {
		// We must construct a new pipeline.
		var spool fragment.Spool
		spool, err = acquireSpool(ctx, r, true)

		if err == nil {
			pln = newPipeline(r.ctx, hdr, spool, r.spoolCh, dialer)
			err = pln.synchronize()
		}
	}

	if err != nil {
		r.pipelineCh <- nil // Release ownership, allow next acquirer to retry.
		return nil, 0, err
	} else if pln.readThroughRev != 0 {
		r.pipelineCh <- pln // Release placeholder for next acquirer to observe.
		return nil, pln.readThroughRev, nil
	}

	return pln, 0, nil
}

// maintenanceLoop performs periodic tasks of the replica, notably:
//  * Refreshing its remote fragment listings from configured stores.
//  * When primary, "pulsing" the journal to ensure its liveness, and the
//    consistency of Etcd-backed assignment routes.
func maintenanceLoop(r *replica, state *v3_allocator.State, dialer client.Dialer, etcd clientv3.KV) {
	var ks = state.KS
	var refreshTimer = time.NewTimer(0)
	var pulseTimer = time.NewTimer(0)

	defer ks.Mu.RUnlock()
	ks.Mu.RLock()

	for {
		var refresh, pulse time.Time

		// Wait until:
		//  * Replica context is cancelled.
		//  * Refresh or pulse timer interval elapses.
		//  * We're signaled to pulse the journal, ASAP.
		ks.Mu.RUnlock()
		select {
		case <-r.ctx.Done():
		case refresh = <-refreshTimer.C:
		case pulse = <-pulseTimer.C:
		case <-r.pulseCh:
			pulse = time.Now()
		}
		ks.Mu.RLock()

		if r.ctx.Err() != nil {
			return // Context has completed.
		}

		// Fetch current journal spec from the KeySpace.
		var item, ok = v3_allocator.LookupItem(ks, r.journal.String())

		if !ok || state.LocalMemberInd == -1 {
			// JournalSpec or local BrokerSpec is not present in the KeySpace.
			<-r.ctx.Done() // Expect context will be cancelled imminently.
			return
		}

		if !refresh.IsZero() {
			// Begin an async refresh of the journal's configured remote fragment stores.
			var spec = item.ItemValue.(*pb.JournalSpec)
			var subctx, _ = context.WithTimeout(r.ctx, 2*spec.Fragment.RefreshInterval)
			go refreshFragments(subctx, spec, r.index)

			refreshTimer.Reset(spec.Fragment.RefreshInterval)

		} else if !pulse.IsZero() {
			// Begin an async "pulse" of the journal, which performs a zero-length
			// write and, on success, updates Etcd assignments with the current Route.

			var assignments = ks.Prefixed(v3_allocator.ItemAssignmentsPrefix(ks, r.journal.String())).Copy()
			var member = ks.KeyValues[state.LocalMemberInd].Decoded.(v3_allocator.Member)
			var localID = member.MemberValue.(*pb.BrokerSpec).Id

			var rt pb.Route
			rt.Init(assignments)
			rt.AttachEndpoints(ks)

			if rt.Primary != -1 && rt.Brokers[rt.Primary] == localID {
				// Pulse only if we are the current journal primary.
				var subctx, _ = context.WithTimeout(r.ctx, 2*pulseInterval)
				go pulseJournal(subctx, r.journal, rt, localID, assignments, dialer, etcd)
			}

			pulseTimer.Reset(pulseInterval)
		}
	}
}

// signalPulse attempts to signal the replica maintenance loop that it should
// pulse the journal.
func signalPulse(r *replica) {
	select {
	case r.pulseCh <- struct{}{}:
	default:
		// Pass (non-blocking).
	}
}

func pulseJournal(ctx context.Context, name pb.Journal, rt pb.Route, localID pb.BrokerSpec_ID, assignments keyspace.KeyValues, dialer client.Dialer, etcd clientv3.KV) {
	var conn, err = dialer.Dial(ctx, localID, rt)
	if err != nil {
		log.WithFields(log.Fields{"err": err, "journal": name}).Error("failed to dial local broker")
		return
	}

	var app = client.NewAppender(ctx, pb.NewBrokerClient(conn), pb.AppendRequest{
		Journal:    name,
		DoNotProxy: true,
	})
	if err = app.Close(); err != nil {
		log.WithFields(log.Fields{
			"journal": name,
			"err":     err,
			"resp":    app.Response.String(),
		}).Warn("pulse commit failed")
		return
	}

	if !rt.Equivalent(&app.Response.Header.Route) {
		log.WithFields(log.Fields{
			"journal": name,
			"rt":      rt.String(),
			"resp":    app.Response.String(),
		}).Warn("pulse Route differs")
		return
	}

	// Strip |rt| Endpoints (they're not needed when marshalling |rt| as an assignment value).
	rt.Endpoints = nil
	var rtStr = rt.MarshalString()

	var cmp []clientv3.Cmp
	var ops []clientv3.Op

	for _, kv := range assignments {
		var key = string(kv.Raw.Key)
		cmp = append(cmp, clientv3.Compare(clientv3.ModRevision(key), "=", kv.Raw.ModRevision))
		ops = append(ops, clientv3.OpPut(key, rtStr, clientv3.WithIgnoreLease()))
	}

	var resp *clientv3.TxnResponse
	resp, err = etcd.Txn(context.Background()).If(cmp...).Then(ops...).Commit()

	if err == nil && !resp.Succeeded {
		err = fmt.Errorf("transaction failed")
	}
	if err != nil {
		log.WithFields(log.Fields{
			"journal": name,
			"err":     err,
		}).Warn("failed to update assignments")
	}
}

func refreshFragments(ctx context.Context, spec *pb.JournalSpec, fi *fragment.Index) {
	var set, err = fragment.WalkAllStores(ctx, spec.Name, spec.Fragment.Stores)

	if err != nil {
		fi.ReplaceRemote(set)
	} else {
		log.WithFields(log.Fields{
			"name":     spec.Name,
			"err":      err,
			"interval": spec.Fragment.RefreshInterval,
		}).Warn("failed to refresh remote fragments (will retry)")
	}
}

var (
	pulseInterval   = time.Minute
	sharedPersister *fragment.Persister
)

func SetSharedPersister(p *fragment.Persister) { sharedPersister = p }
