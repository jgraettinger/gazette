package broker

import (
	"context"

	"github.com/coreos/etcd/mvcc/mvccpb"
	gc "github.com/go-check/check"
	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type DialerSuite struct{}

func (s *DialerSuite) TestDialer(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var peer1, peer2 = newMockPeer(c, ctx), newMockPeer(c, ctx)

	var ks = keyspace.KeySpace{
		Root: "/root",
		KeyValues: keyspace.KeyValues{
			{Raw: mvccpb.KeyValue{Key: []byte("/root/members/zone-a|member-1")},
				Decoded: v3_allocator.Member{Zone: "zone-a", Suffix: "member-1",
					MemberValue: &pb.BrokerSpec{Endpoint: pb.Endpoint("http://" + peer1.addr() + "/path")}}},
			{Raw: mvccpb.KeyValue{Key: []byte("/root/members/zone-b|member-2")},
				Decoded: v3_allocator.Member{Zone: "zone-b", Suffix: "member-2",
					MemberValue: &pb.BrokerSpec{Endpoint: pb.Endpoint("http://" + peer2.addr() + "/path")}}},
		},
	}

	var d = newDialer(&ks)
	var conn1a, conn1b, conn2 *grpc.ClientConn
	var err error

	// Dial first peer.
	conn1a, err = d.dial(ctx, pb.BrokerSpec_ID{Zone: "zone-a", Suffix: "member-1"})
	c.Check(conn1a, gc.NotNil)
	c.Check(err, gc.IsNil)

	// Dial second peer. Connection instances differ.
	conn2, err = d.dial(ctx, pb.BrokerSpec_ID{Zone: "zone-b", Suffix: "member-2"})
	c.Check(conn2, gc.NotNil)
	c.Check(err, gc.IsNil)
	c.Check(conn1a, gc.Not(gc.Equals), conn2)

	// Dial first peer again. Expect the connection is cached and returned again.
	conn1b, err = d.dial(ctx, pb.BrokerSpec_ID{Zone: "zone-a", Suffix: "member-1"})
	c.Check(err, gc.IsNil)
	c.Check(conn1a, gc.Equals, conn1b)

	// Expect an error for an ID not in the KeySpace.
	_, err = d.dial(ctx, pb.BrokerSpec_ID{Zone: "does-not", Suffix: "exist"})
	c.Check(err, gc.ErrorMatches, `no BrokerSpec found \(id: zone:"does-not" suffix:"exist" \)`)
}

var _ = gc.Suite(&DialerSuite{})
