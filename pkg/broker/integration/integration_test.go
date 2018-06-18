package integration

import (
	"context"
	"testing"

	"github.com/LiveRamp/gazette/pkg/broker"
	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	"github.com/LiveRamp/gazette/pkg/client"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/integration"
	gc "github.com/go-check/check"
)

type IntegrationSuite struct{}

func (s *IntegrationSuite) TestFoo(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var etcd = etcdCluster.RandClient()

	// Assert that previous tests have cleaned up after themselves.
	var resp, err = etcd.Get(ctx, "", clientv3.WithPrefix())
	c.Assert(err, gc.IsNil)
	c.Assert(resp.Kvs, gc.HasLen, 0)

	session, err := concurrency.NewSession(etcd)
	c.Assert(err, gc.IsNil)
	dialer, err := client.NewDialer(16)
	c.Assert(err, gc.IsNil)

	var ks = broker.NewKeySpace("/gazette/cluster")

	var brokers = []pb.BrokerSpec_ID{
		{"A", "1"},
		{"B", "2"},
		{"A", "3"},
		{"B", "4"},
	}

	var servers []*teststub.Server

	for _, b := range brokers {
		var key = v3_allocator.MemberKey(ks, b.Zone, b.Suffix)
		var state = v3_allocator.NewObservedState(ks, key)
		servers = append(servers, teststub.NewServer(c, ctx, broker.NewService(dialer, state)))
	}

	var members []*v3_allocator.Announcement



	var memberA1, err := v3_allocator.Announce(context.Background(),
		etcd, v3_allocator.MemberKey(memberKey, cfg.Spec.MarshalString(), session.Lease())


	var memberA1 = v3_allocator.MemberKey(ks, "A", "one")
	var memberA2 = v3_allocator.MemberKey(ks, "A", "two")
	var memberB3 = v3_allocator.MemberKey(ks, "B", "three")


}

func Test(t *testing.T) {
	etcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	gc.TestingT(t)
	etcdCluster.Terminate(t)
}

var (


	_           = gc.Suite(&IntegrationSuite{})
	etcdCluster *integration.ClusterV3
)
