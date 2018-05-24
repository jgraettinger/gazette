package broker

import (
	"context"
	"sync"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

// resolver enacts the routing decisions of an Etcd allocator into a updated,
// local table of journals and their initialized replica instances. It powers
// resolution of journals to local replicas or remote peers.
type resolver interface {
	// resolver resolves journals to a target broker resolution, which may be local
	// or (if |mayProxy|) a proxy-able peer. If a resolution is not possible, a
	// Status != Status_OK is returned indicating why resolution failed.
	resolve(journal pb.Journal, requirePrimary bool, mayProxy bool) (resolution, pb.Status)

	// waitForRevision blocks until the context is canceled, or the resolver's
	// routing table reflects an Etcd revision >= |revision|.
	waitForRevision(ctx context.Context, revision int64) error
}

// resolution is the result of resolving a journal to a Route and
// specific BrokerSpec_ID, which (if local) will have a replica instance.
type resolution struct {
	route   *pb.Route
	broker  pb.BrokerSpec_ID
	replica replica
}

type resolverImpl struct {
	ks *keyspace.KeySpace
	id pb.BrokerSpec_ID

	newReplica func(pb.Journal) replica
	replicas   map[pb.Journal]replica // Guarded by |mu|.
	updateCh   chan struct{}          // Guarded by |mu|.
	mu         sync.RWMutex
}

// newResolver builds and returns an empty, initialized resolverImpl.
func newResolver(ks *keyspace.KeySpace, id pb.BrokerSpec_ID, newReplica func(pb.Journal) replica) *resolverImpl {
	return &resolverImpl{
		ks:         ks,
		id:         id,
		newReplica: newReplica,
		replicas:   make(map[pb.Journal]replica),
		updateCh:   make(chan struct{}),
	}
}

func (rtr *resolverImpl) resolve(journal pb.Journal, requirePrimary bool, mayProxy bool) (res resolution, status pb.Status) {
	defer rtr.mu.RUnlock()
	rtr.mu.RLock()

	var ok bool

	if res.replica, ok = rtr.replicas[journal]; ok {
		// Journal is locally replicated.
		res.route = res.replica.route()
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

	if requirePrimary && res.route.Primary == -1 {
		status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
		return
	} else if len(res.route.Brokers) == 0 {
		status = pb.Status_NO_JOURNAL_BROKERS
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

func (rtr *resolverImpl) waitForRevision(ctx context.Context, rev int64) error {
	rtr.mu.RLock()
	for rtr.ks.Header.Revision < rev {
		var ch = rtr.updateCh
		rtr.mu.RUnlock()

		select {
		case <-ch:
			// KeySpace updated. Loop to check against |rev|.
		case <-ctx.Done():
			return ctx.Err()
		}
		rtr.mu.RLock()
	}
	rtr.mu.RUnlock()
	return nil
}

// UpdateLocalItems is an instance of v3_allocator.LocalItemsCallback.
// It expects that KeySpace is already read-locked when called.
func (rtr *resolverImpl) UpdateLocalItems(items []v3_allocator.LocalItem) {
	rtr.mu.RLock()
	var prev = rtr.replicas
	rtr.mu.RUnlock()

	var next = make(map[pb.Journal]replica, len(items))
	var route pb.Route

	// Walk |items| and create or transition replicas as required to match.
	for _, la := range items {
		var name = pb.Journal(la.Item.Decoded.(v3_allocator.Item).ID)

		var assignment = la.Assignments[la.Index]
		route.Init(la.Assignments)

		var r, ok = prev[name]
		if !ok {
			r = rtr.newReplica(name)
		}

		next[name] = r.transition(la.Item, assignment, route)
	}

	// Obtain write-lock to atomically swap out the |replicas| map,
	// and to signal any RPCs waiting on an Etcd update.
	rtr.mu.Lock()
	rtr.replicas = next
	close(rtr.updateCh)
	rtr.updateCh = make(chan struct{})
	rtr.mu.Unlock()

	// Cancel any prior replicas not included in |items|.
	for name, r := range prev {
		if _, ok := next[name]; !ok {
			r.cancel()
		}
	}
}
