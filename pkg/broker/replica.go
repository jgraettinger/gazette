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
	index *fragmentIndex
	// spoolCh synchronizes access to the single Spool of the replica.
	spoolCh chan fragment.Spool
	// txnHandoffCh allows an Append holding an in-flight transaction, to hand
	// the transaction off to another ready Append wanting to continue it.
	txnHandoffCh chan *transaction
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
		index:         newFragmentIndex(ctx),
		spoolCh:       spoolCh,
		txnHandoffCh:  make(chan *transaction),
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

// prepareSpool readies Spool for it's next write, by:
//  * Rolling the Spool forward if it's less than the maximum end offset
//    of the fragment index (eg, of a discovered remote Fragment).
//  * Rolling the Spool forward if it's ContentLength has reached a threshold.
//  * If not already, opening the Spool.
//  * If not already and the replica is primary, initializing the Spool for
//    incremental compression.
func prepareSpool(s *fragment.Spool, r *replica) error {
	if eo := r.index.endOffset(); eo > s.Fragment.End {
		s.Roll(r.spec(), eo)
	} else if s.Fragment.ContentLength() >= r.spec().FragmentLength {
		s.Roll(r.spec(), s.Fragment.End)
	}

	if s.Fragment.File == nil {
		if err := s.Open(); err != nil {
			return err
		}
	}

	// As primary, we are the presumptive broker to persist the Fragment.
	// Initialize a compressor to speculatively compress committed content as it
	// arrives. Compression can be expensive, and compressing the Fragment as it's
	// built effectively back-pressures the cost onto writers, ensuring we don't
	// accept writes faster than we can compress them.
	if r.isPrimary() && s.CompressedFile == nil {
		if err := s.InitCompressor(); err != nil {
			return err
		}
	}
	return nil
}
