package integration

import (
	"context"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	gc "github.com/go-check/check"
)

type IntegrationSuite struct{}

func (s *IntegrationSuite) TestFoo(c *gc.C) {
	var etcd = etcdCluster.RandClient()
	var ctx = context.Background()

	// Assert that previous tests have cleaned up after themselves.
	var resp, err = etcd.Get(ctx, "", clientv3.WithPrefix())
	c.Assert(err, gc.IsNil)
	c.Assert(resp.Kvs, gc.HasLen, 0)

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
