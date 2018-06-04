package broker

import (
	"context"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type resolver struct {
	ks       *keyspace.KeySpace
	cond     *keyspace.RevCond
	state    *v3_allocator.State
	replicas map[pb.Journal]*replica
}

func newResolver(ks *keyspace.KeySpace, cond *keyspace.RevCond, state *v3_allocator.State) *resolver {
	var r = &resolver{
		ks:       ks,
		cond:     cond,
		state:    state,
		replicas: make(map[pb.Journal]*replica),
	}
	ks.Mu.Lock()
	ks.Observers = append(ks.Observers, r.observe)
	ks.Mu.Unlock()

	return r
}

type resolveArgs struct {
	ctx context.Context
	// Journal to be dispatched.
	journal pb.Journal
	// Whether we may proxy to another broker.
	mayProxy bool
	// Whether we require the primary broker of the journal.
	requirePrimary bool
	// Whether we require that the journal be fully assigned, or will otherwise
	// tolerate fewer broker assignments than the desired journal replication.
	requireFullAssignment bool
	// Minimum Etcd Revision to have read through, before generating a DispatchResponse.
	minEtcdRevision int64
}

type resolution struct {
	// Header defines the selected broker ID, effective Etcd Revision,
	// and Journal Route of the resolution.
	pb.Header
	// JournalSpec of the Journal at the current Etcd Revision.
	journalSpec *pb.JournalSpec
	// remote is set to the Endpoint at which the BrokerID may be reached,
	// iff the selected BrokerID is not the local BrokerID.
	replica *replica
}

func (r *resolver) resolve(args resolveArgs) (res resolution, err error) {
	defer r.ks.Mu.RLock()
	r.ks.Mu.RUnlock()

	if args.minEtcdRevision > r.ks.Header.Revision {
		if err = r.cond.WaitForRevision(args.ctx, args.minEtcdRevision); err != nil {
			return
		}
	}
	// Extract Etcd Revision.
	res.Etcd = pb.FromEtcdResponseHeader(r.ks.Header)

	// Extract JournalSpec.
	if item, ok := v3_allocator.LookupItem(r.ks, args.journal.String()); ok {
		res.journalSpec = item.ItemValue.(*pb.JournalSpec)
	}
	// Extract Route.
	var assignments = r.ks.KeyValues.Prefixed(
		v3_allocator.ItemAssignmentsPrefix(r.ks, args.journal.String()))
	res.Route.Init(assignments)
	res.Route.AttachEndpoints(r.ks)

	var localID pb.BrokerSpec_ID
	if r.state.LocalMemberInd != -1 {
		localID = r.state.Members[r.state.LocalMemberInd].
			Decoded.(v3_allocator.Member).MemberValue.(*pb.BrokerSpec).Id
	}

	// Select a best, responsible BrokerID.
	if args.requirePrimary && res.Route.Primary != -1 {
		res.BrokerId = res.Route.Brokers[res.Route.Primary]
	} else if !args.requirePrimary && len(res.Route.Brokers) != 0 {
		res.BrokerId = res.Route.Brokers[res.Route.SelectReplica(localID)]
	}
	if res.BrokerId == localID {
		res.replica = r.replicas[args.journal]
	}

	// Select a response Status code.
	if res.journalSpec == nil {
		res.Status = pb.Status_JOURNAL_NOT_FOUND
	} else if res.BrokerId == (pb.BrokerSpec_ID{}) {
		if args.requirePrimary {
			res.Status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
		} else {
			res.Status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
		}
	} else if !args.mayProxy && res.BrokerId != localID {
		if args.requirePrimary {
			res.Status = pb.Status_NOT_JOURNAL_PRIMARY_BROKER
		} else {
			res.Status = pb.Status_NOT_JOURNAL_BROKER
		}
	} else if args.requireFullAssignment && len(res.Route.Brokers) < int(res.journalSpec.Replication) {
		res.Status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
	} else {
		res.Status = pb.Status_OK
	}

	return
}

func (r *resolver) observe() bool {
	var next = make(map[pb.Journal]*replica, len(r.state.LocalItems))

	for _, li := range r.state.LocalItems {
		var name = pb.Journal(li.Item.Decoded.(v3_allocator.Item).ID)

		if replica, ok := r.replicas[name]; ok {
			next[name] = replica
			delete(r.replicas, name)
		} else {
			next[name] = newReplica(name)
		}
	}
	var prev = r.replicas
	r.replicas = next

	for _, replica := range prev {
		replica.cancel()
	}
	return true
}
