package broker

import (
	"context"
	"fmt"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type resolver struct {
	state    *v3_allocator.State
	replicas map[pb.Journal]*replica
}

func newResolver(state *v3_allocator.State) *resolver {
	var r = &resolver{
		state:    state,
		replicas: make(map[pb.Journal]*replica),
	}
	state.KS.Mu.Lock()
	state.KS.Observers = append(state.KS.Observers, r.observeKeySpace)
	state.KS.Mu.Unlock()

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
	// Optional Header attached to the request from a proxying peer.
	proxyHeader *pb.Header
}

type resolution struct {
	status pb.Status
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
	var ks = r.state.KS

	if args.minEtcdRevision > ks.Header.Revision {
		if err = ks.WaitForRevision(args.ctx, args.minEtcdRevision); err != nil {
			return
		}
	}
	res.Etcd = pb.FromEtcdResponseHeader(ks.Header)

	// Extract JournalSpec.
	if item, ok := v3_allocator.LookupItem(ks, args.journal.String()); ok {
		res.journalSpec = item.ItemValue.(*pb.JournalSpec)
	}
	// Extract Route.
	var assignments = ks.KeyValues.Prefixed(
		v3_allocator.ItemAssignmentsPrefix(ks, args.journal.String()))
	res.Route.Init(assignments)
	res.Route.AttachEndpoints(ks)

	if r.state.LocalMemberInd == -1 {
		err = fmt.Errorf("local member key not found")
		return
	}
	var local = r.state.Members[r.state.LocalMemberInd].
		Decoded.(v3_allocator.Member).MemberValue.(*pb.BrokerSpec)

	// Select a best, responsible BrokerID.
	if args.requirePrimary && res.Route.Primary != -1 {
		res.BrokerId = res.Route.Brokers[res.Route.Primary]
	} else if !args.requirePrimary && len(res.Route.Brokers) != 0 {
		res.BrokerId = res.Route.Brokers[res.Route.SelectReplica(local.GetId())]
	}

	if res.BrokerId == local.Id {
		res.replica = r.replicas[args.journal]
	} else {
		res.Header.ProxyId = &local.Id
	}

	// Select a response Status code.
	if res.journalSpec == nil {
		res.status = pb.Status_JOURNAL_NOT_FOUND
	} else if res.BrokerId == (pb.BrokerSpec_ID{}) {
		if args.requirePrimary {
			res.status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
		} else {
			res.status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
		}
	} else if !args.mayProxy && res.BrokerId != local.Id {
		if args.requirePrimary {
			res.status = pb.Status_NOT_JOURNAL_PRIMARY_BROKER
		} else {
			res.status = pb.Status_NOT_JOURNAL_BROKER
		}
	} else if args.requireFullAssignment && len(res.Route.Brokers) < int(res.journalSpec.Replication) {
		res.status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
	} else {
		res.status = pb.Status_OK
	}

	return
}

func (r *resolver) observeKeySpace() {
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
	return
}
