package broker

import (
	"context"
	"crypto/sha1"
	"time"

	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

func newEtcdFixture(c *gc.C, peerAddr string) (*clientv3.Client, *keyspace.KeySpace, pb.BrokerSpec_ID) {
	var etcd = etcdCluster.RandClient()
	var ctx = context.Background()

	// Assert that previous tests have cleaned up after themselves.
	var resp, err = etcd.Get(ctx, "", clientv3.WithPrefix())
	c.Assert(err, gc.IsNil)
	c.Assert(resp.Kvs, gc.HasLen, 0)

	var peerID = pb.BrokerSpec_ID{Zone: "peer", Suffix: "broker"}
	var localID = pb.BrokerSpec_ID{Zone: "local", Suffix: "broker"}

	var ks = NewKeySpace("/root")

	_, err = etcd.Put(ctx,
		v3_allocator.MemberKey(ks, peerID.Zone, peerID.Suffix),
		(&pb.BrokerSpec{
			Id:       peerID,
			Endpoint: pb.Endpoint("http://" + peerAddr),
		}).MarshalString())
	c.Assert(err, gc.IsNil)

	_, err = etcd.Put(ctx,
		v3_allocator.MemberKey(ks, localID.Zone, localID.Suffix),
		(&pb.BrokerSpec{
			Id:       localID,
			Endpoint: pb.Endpoint("http://[100::]"), // Black-hole address.
		}).MarshalString())
	c.Assert(err, gc.IsNil)

	var journals = map[pb.Journal][]pb.BrokerSpec_ID{
		"a/journal":      {localID, peerID},
		"remote/journal": {peerID},
		"no/brokers":     {},
		"no/primary":     {{}, localID, peerID},
	}
	for journal, brokers := range journals {

		// Create JournalSpec.
		_, err = etcd.Put(ctx,
			v3_allocator.ItemKey(ks, journal.String()),
			(&pb.JournalSpec{
				Name:        journal,
				Replication: 2,
				Fragment: pb.JournalSpec_Fragment{
					Length:           1024,
					CompressionCodec: pb.CompressionCodec_SNAPPY,
					RefreshInterval:  time.Second,
				},
			}).MarshalString())

		c.Assert(err, gc.IsNil)

		// Create broker assignments.
		for slot, id := range brokers {
			if id == (pb.BrokerSpec_ID{}) {
				continue
			}

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

	c.Assert(ks.Load(context.Background(), etcd, 0), gc.IsNil)
	return etcd, ks, localID
}

func cleanupEtcdFixture(c *gc.C) {
	var _, err = etcdCluster.RandClient().Delete(context.Background(), "", clientv3.WithPrefix())
	c.Assert(err, gc.IsNil)
}

type brokerFixture struct {
	ctx      context.Context
	client   pb.BrokerClient
	peer     *mockPeer
	resolver *resolver
}

func runBrokerTestCase(c *gc.C, tc func(brokerFixture)) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var peer = newMockPeer(c, ctx)
	var _, ks, localID = newEtcdFixture(c, peer.addr())
	defer cleanupEtcdFixture(c)

	var allocState, err = v3_allocator.NewState(ks,
		v3_allocator.MemberKey(ks, localID.Zone, localID.Suffix))
	c.Assert(err, gc.IsNil)

	var resolver = newResolver(ks, localID, func(journal pb.Journal) replica {
		return newReplicaImpl()
	})
	resolver.onAllocatorStateChange(allocState)

	var srv = newTestServer(c, ctx, &Service{resolver: resolver, dialer: newDialer(ks)})

	tc(brokerFixture{
		ctx:      ctx,
		client:   pb.NewBrokerClient(srv.mustDial()),
		peer:     peer,
		resolver: resolver,
	})
}

func sumOf(s string) pb.SHA1Sum {
	var d = sha1.Sum([]byte(s))
	return pb.SHA1SumFromDigest(d[:])
}
