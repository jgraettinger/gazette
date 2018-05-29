package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type EtcdContext struct {
	Client   *clientv3.Client
	KeySpace *keyspace.KeySpace
}

// NewBrokerKeySpace returns a KeySpace suitable for use with an Allocator.
// It decodes allocator Items as JournalSpec messages, Members as BrokerSpecs,
// and Assignments as Routes.
func NewEtcdContext(etcd *clientv3.Client, prefix string) *EtcdContext {
	return &EtcdContext{
		Client:   etcd,
		KeySpace: v3_allocator.NewAllocatorKeySpace(prefix, decoder{}),
	}
}

func (e *EtcdContext) CreateBrokerSpec(ctx context.Context, leaseID clientv3.LeaseID, spec *pb.BrokerSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	var key = v3_allocator.MemberKey(e.KeySpace, spec.Id.Zone, spec.Id.Suffix)

	for {
		var resp, err = e.Client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, spec.MarshalString(), clientv3.WithLease(leaseID))).
			Commit()

		if err != nil {
			return err
		} else if resp.Succeeded {
			return nil
		}

		log.WithField("key", key).Warn("waiting for a previous BrokerSpec to go away")

		select {
		case <-time.After(time.Second * 10):
		// Pass
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (e *EtcdContext) UpdateBrokerSpec(ctx context.Context, kv keyspace.KeyValue, spec *pb.BrokerSpec) error {
	var key = v3_allocator.MemberKey(e.KeySpace, spec.Id.Zone, spec.Id.Suffix)

	if err := spec.Validate(); err != nil {
		return err
	} else if key != string(kv.Raw.Key) {
		return fmt.Errorf("expected kv.Raw.Key to match MemberKey (%s vs %s)", string(kv.Raw.Key), key)
	}

	var resp, err = e.Client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.Raw.ModRevision)).
		Then(clientv3.OpPut(key, spec.MarshalString(), clientv3.WithIgnoreLease())).
		Commit()

	if err != nil {
		return err
	} else if !resp.Succeeded {
		return fmt.Errorf("update transaction failed")
	} else {
		return nil
	}
}

func (e *EtcdContext) UpsertJournalSpec(ctx context.Context, kv keyspace.KeyValue, spec *pb.JournalSpec) error {
	var key = v3_allocator.ItemKey(e.KeySpace, spec.Name.String())

	if err := spec.Validate(); err != nil {
		return err
	} else if kv.Raw.Key != nil && key != string(kv.Raw.Key) {
		return fmt.Errorf("expected kv.Raw.Key to match ItemKey (%s vs %s)", string(kv.Raw.Key), key)
	}

	var resp, err = e.Client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.Raw.ModRevision)).
		Then(clientv3.OpPut(key, spec.MarshalString())).
		Commit()

	if err != nil {
		return err
	} else if !resp.Succeeded {
		return fmt.Errorf("upsert transaction failed")
	} else {
		return nil
	}
}

func (e *EtcdContext) UpdateAssignmentRoute(ctx context.Context, kv keyspace.KeyValue, spec *pb.Route) error {
	var key = string(kv.Raw.Key)

	if key == "" {
		panic("invalid KeyValue")
	} else if err := spec.Validate(); err != nil {
		return err
	}

	var resp, err = e.Client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.Raw.ModRevision)).
		Then(clientv3.OpPut(key, spec.MarshalString(), clientv3.WithIgnoreLease())).
		Commit()

	if err != nil {
		return err
	} else if !resp.Succeeded {
		return fmt.Errorf("update transaction failed")
	} else {
		return nil
	}
}

// decoder is an instance of v3_allocator.AllocatorDecoder.
type decoder struct{}

func (d decoder) DecodeItem(id string, raw *mvccpb.KeyValue) (v3_allocator.ItemValue, error) {
	var s = new(pb.JournalSpec)

	if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	} else if s.Name.String() != id {
		return nil, pb.NewValidationError("JournalSpec Name doesn't match Item ID (%+v vs %+v)", s.Name, id)
	}
	return s, nil
}

func (d decoder) DecodeMember(zone, suffix string, raw *mvccpb.KeyValue) (v3_allocator.MemberValue, error) {
	var s = new(pb.BrokerSpec)

	if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	} else if s.Id.Zone != zone {
		return nil, pb.NewValidationError("BrokerSpec Zone doesn't match Member Zone (%+v vs %+v)", s.Id.Zone, zone)
	} else if s.Id.Suffix != suffix {
		return nil, pb.NewValidationError("BrokerSpec Suffix doesn't match Member Suffix (%+v vs %+v)", s.Id.Suffix, suffix)
	}
	return s, nil
}

func (d decoder) DecodeAssignment(itemID, memberZone, memberSuffix string, slot int, raw *mvccpb.KeyValue) (v3_allocator.AssignmentValue, error) {
	var s = &pb.Route{Primary: -1}

	if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}
