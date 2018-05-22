package broker

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/fragment"
	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type replica struct {
	journal keyspace.KeyValue
	// Local assignment of |replica|, motivating this replica instance.
	assignment keyspace.KeyValue
	// Current broker routing topology of the replica.
	route pb.Route

	// The following fields are held constant over the lifetime of a replica:

	// Context tied to processing lifetime of this replica by this broker.
	// Cancelled when this broker is no longer responsible for the replica.
	ctx    context.Context
	cancel context.CancelFunc
	// Index of all known Fragments of the replica.
	index *fragment.Index
	// spoolCh synchronizes access to the single Spool of the replica.
	spoolCh chan fragment.Spool
	// plnHandoffCh allows an Append holding an in-flight pipeline, to hand
	// the pipeline off to another ready Append wanting to continue it.
	plnHandoffCh chan *pipeline
	// initialLoadCh is closed after the first remote fragment store listing,
	// and is used to gate Append requests (only) until the fragmentIndex
	// has been initialized with remote listings.
	initialLoadCh chan struct{}
}

func newReplica() *replica {
	var ctx, cancel = context.WithCancel(context.Background())

	var spoolCh = make(chan fragment.Spool, 1)
	spoolCh <- fragment.Spool{}

	return &replica{
		ctx:           ctx,
		cancel:        cancel,
		index:         fragment.NewIndex(ctx),
		spoolCh:       spoolCh,
		plnHandoffCh:  make(chan *pipeline),
		initialLoadCh: make(chan struct{}),
	}
}

// spec returns the replica's JournalSpec.
func (r *replica) spec() *pb.JournalSpec {
	return r.journal.Decoded.(v3_allocator.Item).ItemValue.(*pb.JournalSpec)
}

// isPrimary returns whether the replica is primary.
func (r *replica) isPrimary() bool {
	return r.assignment.Decoded.(v3_allocator.Assignment).Slot == 0
}

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
