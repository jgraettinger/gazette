package broker

import (
	"sync"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

// resolver enacts the routing decisions of an etcd allocator into a updated,
// local table of journals and their initialized replica instances. It powers
// resolution of journals to local replicas or remote peers.
type resolver struct {
	ks *keyspace.KeySpace
	id pb.BrokerSpec_ID

	newReplica func(pb.Journal) replica
	replicas   map[pb.Journal]replica // Guarded by |mu|.
	mu         sync.RWMutex

	*keyspace.RevCond
	broadcast func(revision int64)
}

func (r *resolver) KeySpace() *keyspace.KeySpace { return r.ks }
func (r *resolver) LocalKey() string             { return v3_allocator.MemberKey(r.ks, r.id.Zone, r.id.Suffix) }

// resolution is the result of resolving a journal to a Route and
// specific BrokerSpec_ID, which (if local) will have a replica instance.
type resolution struct {
	route   *pb.Route
	broker  pb.BrokerSpec_ID
	replica replica
}

// newResolver builds and returns an empty, initialized resolver.
func newResolver(ks *keyspace.KeySpace, id pb.BrokerSpec_ID, newReplica func(pb.Journal) replica) *resolver {
	return &resolver{
		ks:         ks,
		id:         id,
		newReplica: newReplica,
		replicas:   nil,
	}
}

// resolve a journal to a target broker resolution, which may be local
// or (if |mayProxy|) a proxy-able peer. If a resolution is not possible, a
// Status != Status_OK is returned indicating why resolution failed.
func (rtr *resolver) resolve(journal pb.Journal, requirePrimary bool, mayProxy bool) (res resolution, status pb.Status) {
	defer rtr.mu.RUnlock()
	rtr.mu.RLock()

	var ok bool

	if res.replica, ok = rtr.replicas[journal]; ok {
		// Journal is locally replicated.
		res.route = res.replica.getRoute()
		res.broker = rtr.id
	} else {

		rtr.ks.Mu.RLock()
		_, ok = v3_allocator.LookupItem(rtr.ks, journal.String())

		var assignments = rtr.ks.KeyValues.Prefixed(
			v3_allocator.ItemAssignmentsPrefix(rtr.ks, journal.String()))

		res.route = new(pb.Route)
		res.route.Init(assignments)
		res.route.AttachEndpoints(rtr.ks)

		rtr.ks.Mu.RUnlock()

		if !ok {
			status = pb.Status_JOURNAL_NOT_FOUND
			return
		}
	}

	if len(res.route.Brokers) == 0 {
		status = pb.Status_INSUFFICIENT_JOURNAL_BROKERS
		return
	} else if requirePrimary && res.route.Primary == -1 {
		status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
		return
	}

	// If the local replica can satisfy the request, we're done.
	// Otherwise, we must proxy to continue.
	if res.replica != nil &&
		(!requirePrimary || res.route.Brokers[res.route.Primary] == rtr.id) {
		return
	}
	res.replica = nil
	res.broker = pb.BrokerSpec_ID{}

	if !mayProxy {
		if requirePrimary {
			status = pb.Status_NOT_JOURNAL_PRIMARY_BROKER
		} else {
			status = pb.Status_NOT_JOURNAL_BROKER
		}
		return
	}

	if requirePrimary {
		res.broker = res.route.Brokers[res.route.Primary]
	} else {
		res.broker = res.route.Brokers[res.route.RandomReplica(rtr.id.Zone)]
	}
	return
}

// onAllocatorStateChange is an implementation of v3_allocator.StateCallback.
// It expects that KeySpace is already read-locked when called.
func (rtr *resolver) OnAllocState(state *v3_allocator.State) {
	rtr.mu.RLock()
	var prev = rtr.replicas
	rtr.mu.RUnlock()

	var next = make(map[pb.Journal]replica, len(state.LocalItems))
	var route pb.Route

	// Walk |items| and create or transition replicas as required to match.
	for _, la := range state.LocalItems {
		var name = pb.Journal(la.Item.Decoded.(v3_allocator.Item).ID)

		var assignment = la.Assignments[la.Index]
		route.Init(la.Assignments)

		var r, ok = prev[name]
		if !ok {
			r = rtr.newReplica(name)
		}
		next[name] = r.transition(rtr.ks, la.Item, assignment, route)
	}

	// Obtain write-lock to atomically swap out the |replicas| map,
	// and to signal any RPCs waiting on an etcd update.
	rtr.mu.Lock()
	rtr.replicas = next
	rtr.broadcast(state.KS.Header.Revision)
	rtr.mu.Unlock()

	// Cancel any prior replicas not included in |items|.
	for name, r := range prev {
		if _, ok := next[name]; !ok {
			r.cancel()
		}
	}
}
