package broker

import (
	"io"

	gc "github.com/go-check/check"
	"github.com/pkg/errors"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type AppendSuite struct{}

/*
func newEtcdFixture(c *gc.C, peerAddr, localAddr string) (*clientv3.Client, *keyspace.KeySpace, pb.BrokerSpec) {
	var etcd = etcdCluster.RandClient()
	var ctx = context.Background()

	// Assert that previous tests have cleaned up after themselves.
	var resp, err = etcd.Get(ctx, "", clientv3.WithFromKey())
	c.Assert(err, gc.IsNil)
	c.Assert(resp.Kvs, gc.HasLen, 0)

	var peerID = pb.BrokerSpec_ID{Zone: "peer", Suffix: "broker"}
	var localID = pb.BrokerSpec_ID{Zone: "local", Suffix: "broker"}

	var ks = NewKeySpace("/root")

	// Create peer first, making it the allocation coordinator.
	// (Our actual allocator won't make changes to our fixture).
	_, err = etcd.Put(ctx,
		v3_allocator.MemberKey(ks, peerID.Zone, peerID.Suffix),
		pb.BrokerSpec{
			Id:       peerID,
			Endpoint: pb.Endpoint("http://" + peerAddr),
		}.MarshalString())

	c.Assert(err, gc.IsNil)

	var journals = map[pb.Journal][]pb.BrokerSpec_ID{
		"a/journal":      {localID, peerID},
		"remote/journal": {peerID, localID},
	}
	for journal, brokers := range journals {

		// Create JournalSpec.
		_, err = etcd.Put(ctx,
			v3_allocator.ItemKey(ks, journal.String()),
			pb.JournalSpec{
				Name:        "a/journal",
				Replication: 2,
				Fragment: pb.JournalSpec_Fragment{
					Length:           1024,
					CompressionCodec: pb.CompressionCodec_SNAPPY,
					RefreshInterval:  time.Second,
				},
			}.MarshalString())

		c.Assert(err, gc.IsNil)

		// Create broker assignments.
		for slot, id := range brokers {
			var key = v3_allocator.AssignmentKey(ks, v3_allocator.Assignment{
				ItemID:       journal.String(),
				MemberZone:   id.Zone,
				MemberSuffix: id.Suffix,
				Slot:         slot,
			})

			_, err = etcd.Put(ctx, key, "")
			c.Assert(err, gc.IsNil)
		}
	}
	return etcd, ks, pb.BrokerSpec{
		Id:       localID,
		Endpoint: pb.Endpoint("http://" + localAddr),
	}
}

func cleanupEtcdFixture(c *gc.C) {
	var _, err = etcdCluster.RandClient().Delete(context.Background(), "", clientv3.WithFromKey())
	c.Assert(err, gc.IsNil)
}

func (s *AppendSuite) TestAppend(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var srv = newTestServer(c)
	var peer = newMockPeer(c, ctx)

	var etcd, ks, spec = newEtcdFixture(c, peer.addr(), srv.addr())
	defer cleanupEtcdFixture(c)

	var session, err = concurrency.NewSession(etcd, concurrency.WithContext(ctx))
	c.Assert(err, gc.IsNil)

	svc, err := NewService(spec, ks, etcd, session.Lease())
	c.Assert(err, gc.IsNil)

	go func() {
		var client = pb.NewBrokerClient(srv.mustDial())
		var stream, err = client.Append(ctx)
		c.Assert(err, gc.IsNil)

		stream.Send(&pb.AppendRequest{
			Journal: "a/journal",
		})

		cancel()
	}()

	c.Check(svc.Run(ctx), gc.IsNil)
}
*/

func (s *AppendSuite) TestServeAppend(c *gc.C) {
	var rm = newReplicationMock(c)
	var srv = newMockServer(c, rm.ctx)

	defer rm.cancel()

	var client, err = pb.NewBrokerClient(srv.mustDial())
	c.Assert(err, gc.IsNil)

	stream, err := client.Append(rm.ctx)
	c.Assert(err, gc.IsNil)

	var rep = newReplicaImpl()
	rep.transition()

	rep.serveAppend(nil, <-srv.appendCh, rm)

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
