package broker

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/client"
	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type Service struct {
	dialer   client.Dialer
	resolver *resolver
}

func NewService(dialer client.Dialer, state *v3_allocator.State) *Service {
	var s = &Service{
		dialer:   dialer,
		resolver: newResolver(state),
	}

	state.KS.Mu.Lock()
	state.KS.Observers = append(state.KS.Observers, s.observeKeySpace)
	state.KS.Mu.Unlock()

	return s
}

// observeKeySpace, by virtue of being a KeySpace.Observer, expects that the
// KeySpace.Mu Lock is held.
func (s *Service) observeKeySpace() {

	s.resolver.updateResolutions(
		// onNewReplica begins a long-lived journal index watch.
		func(name pb.Journal, replica *replica) {
			go replica.index.WatchStores(func() (*pb.JournalSpec, bool) {
				return getJournalSpec(s.resolver.state.KS, name)
			})
		},
		// onInconsistentPrimary begins an asynchronous attempt to acquire
		// a synchronized pipeline, and then update Etcd assignments.
		func(name pb.Journal, assignments keyspace.KeyValues) {
			go s.foo(name, s.resolver.state.KS.Header.Revision, assignments.Copy())
		},
	)
}

func (s *Service) foo(name pb.Journal, rev int64, assignments keyspace.KeyValues) {
	var ctx = context.Background()
	var rt pb.Route
	rt.Init(assignments)

	var member = s.resolver.state.KS.KeyValues[s.resolver.state.LocalMemberInd].Decoded.(v3_allocator.Member)

	var conn, err = s.dialer.DialEndpoint(ctx, member.MemberValue.(*pb.BrokerSpec).Endpoint)
	if err != nil {
		log.WithFields(log.Fields{"err": err, "journal": name}).Error("failed to resolve journal")

	}

	var res, err = s.resolver.resolve(resolveArgs{
		ctx:                   ctx,
		journal:               name,
		requirePrimary:        true,
		requireFullAssignment: true,
	})

	if err != nil {
		log.WithFields(log.Fields{"err": err, "journal": name}).Error("failed to resolve journal")
		return
	}

	if res.status != pb.Status_OK {
		if res.Header.Etcd.Revision > rev {
			return
		} else {
			panic(fmt.Sprintf("unexpected !OK status: (%s)", res.status))
		}
	}

	pln, _, err := acquirePipeline(ctx, res.replica, res.Header, s.dialer)

	if err != nil {
		log.WithFields(log.Fields{"err": err, "journal": name}).Warn("failed to acquire pipeline")
		return
	} else if pln == nil {
		return //
	}

	res.replica.pipelineCh <- pln

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
