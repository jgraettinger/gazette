package broker

import (
	"context"
	"io"
	"time"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
	"github.com/pkg/errors"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type AppendSuite struct{}

func newEtcdContextFixture(c *gc.C, ctx context.Context, localAddr, peerAddr string) *EtcdContext {
	var etcd = NewEtcdContext(etcdCluster.RandClient(), "/root")

	var _, err = etcd.Client.Delete(ctx, "", clientv3.WithPrefix())
	c.Assert(err, gc.IsNil)

	var peerID = pb.BrokerSpec_ID{Zone: "peer", Suffix: "broker"}
	var localID = pb.BrokerSpec_ID{Zone: "local", Suffix: "broker"}

	// Create peer first, making it the allocation coordinator.
	// Our actual allocator won't make changes to our fixture.
	c.Check(etcd.CreateBrokerSpec(ctx, 0, &pb.BrokerSpec{
		Id:       peerID,
		Endpoint: pb.Endpoint("http://" + peerAddr),
	}), gc.IsNil)
	c.Check(etcd.CreateBrokerSpec(ctx, 0, &pb.BrokerSpec{
		Id:       localID,
		Endpoint: pb.Endpoint("http://" + localAddr),
	}), gc.IsNil)

	var fragSpec = pb.JournalSpec_Fragment{
		Length:           1024,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		RefreshInterval:  time.Second,
	}

	c.Check(etcd.UpsertJournalSpec(ctx, keyspace.KeyValue{}, &pb.JournalSpec{
		Name:        "a/journal",
		Replication: 2,
		Fragment:    fragSpec,
	}), gc.IsNil)

	c.Check(etcd.UpsertJournalSpec(ctx, keyspace.KeyValue{}, &pb.JournalSpec{
		Name:        "remote/journal",
		Replication: 2,
		Fragment:    fragSpec,
	}), gc.IsNil)

	var keys = []string{
		v3_allocator.AssignmentKey(etcd.KeySpace, v3_allocator.Assignment{
			ItemID:       "a/journal",
			MemberZone:   "local",
			MemberSuffix: "broker",
			Slot:         0,
		}),
		v3_allocator.AssignmentKey(etcd.KeySpace, v3_allocator.Assignment{
			ItemID:       "a/journal",
			MemberZone:   "peer",
			MemberSuffix: "broker",
			Slot:         1,
		}),
		v3_allocator.AssignmentKey(etcd.KeySpace, v3_allocator.Assignment{
			ItemID:       "remote/journal",
			MemberZone:   "peer",
			MemberSuffix: "broker",
			Slot:         0,
		}),
		v3_allocator.AssignmentKey(etcd.KeySpace, v3_allocator.Assignment{
			ItemID:       "remote/journal",
			MemberZone:   "local",
			MemberSuffix: "broker",
			Slot:         1,
		}),
	}

	for _, key := range keys {
		var resp, err = etcd.Client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, "")).
			Commit()
		c.Check(err, gc.IsNil)
		c.Check(resp.Succeeded, gc.Equals, true)
	}

	c.Check(etcd.KeySpace.Load(ctx, etcd.Client, 0), gc.IsNil)
	return etcd
}

func (s *AppendSuite) TestAppend(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var peer = newMockPeer(c, ctx)

	var resolver = newResolver(ks)

	var aServer = &Server{}
	var srv = newTestServer(c, ctx, aServer)

	_ = newEtcdContextFixture(c, ctx, srv.addr(), peer.addr())
}

func (s *AppendSuite) TestAppenderCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var spool = fragment.NewSpool("a/journal", rm)
	spool.Fragment.End = 24
	var pln = newPipeline(rm.ctx, rm.route, spool, rm)

	var spec = pb.JournalSpec_Fragment{
		Length:           16, // Current spool is over target length.
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		Stores:           []pb.FragmentStore{"s3://a-bucket/path"},
	}
	var appender = beginAppending(pln, spec)

	// Expect an updating proposal is scattered.
	c.Check(<-rm.brokerA.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Begin:            24,
			End:              24,
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_SNAPPY,
			BackingStore:     "s3://a-bucket/path",
		},
		Acknowledge: false,
	})
	<-rm.brokerC.replReqCh

	// Chunk one. Expect it's forwarded to peers.
	c.Check(appender.onRecv(&pb.AppendRequest{Content: []byte("foo")}, nil), gc.Equals, true)
	var req, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{
		Content:      []byte("foo"),
		ContentDelta: 0,
	})

	// Chunk two.
	c.Check(appender.onRecv(&pb.AppendRequest{Content: []byte("bar")}, nil), gc.Equals, true)
	req, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{
		Content:      []byte("bar"),
		ContentDelta: 3,
	})

	// Client EOF. Expect a commit proposal is scattered to peers.
	c.Check(appender.onRecv(nil, io.EOF), gc.Equals, false)

	var expect = &pb.Fragment{
		Journal:          "a/journal",
		Begin:            24,
		End:              30,
		Sum:              pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		BackingStore:     "s3://a-bucket/path",
	}
	req, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})

	c.Check(appender.reqFragment, gc.DeepEquals, &pb.Fragment{
		Journal: "a/journal",
		Begin:   24,
		End:     30,
		Sum:     pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
	})
	c.Check(appender.reqErr, gc.IsNil)

	// New request. This time, an updated proposal is not sent.
	appender = beginAppending(pln, spec)

	// First chunk.
	c.Check(appender.onRecv(&pb.AppendRequest{Content: []byte("baz")}, nil), gc.Equals, true)
	req, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("baz")})

	// Expect an invalid AppendRequest is treated as a client error.
	c.Check(appender.onRecv(&pb.AppendRequest{Journal: "/invalid"}, nil), gc.Equals, false)

	// Expect a rollback is scattered to peers.
	req, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})

	c.Check(appender.reqFragment, gc.IsNil)
	c.Check(appender.reqErr, gc.ErrorMatches, `Journal: cannot begin with '/' \(/invalid\)`)

	// New request. An updated proposal is still not sent, despite the spec being
	// different, because the spool is non-empty but not over the Fragment Length.
	spec.CompressionCodec = pb.CompressionCodec_GZIP
	appender = beginAppending(pln, spec)

	// A received client error triggers a rollback.
	c.Check(appender.onRecv(nil, errors.New("an error")), gc.Equals, false)

	req, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
	c.Check(req, gc.DeepEquals, &pb.ReplicateRequest{Proposal: expect, Acknowledge: true})
}

var _ = gc.Suite(&AppendSuite{})
