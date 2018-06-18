package integration

import (
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/LiveRamp/gazette/pkg/broker"
	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	"github.com/LiveRamp/gazette/pkg/client"
	"github.com/LiveRamp/gazette/pkg/keyspace"
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

	var ks = broker.NewKeySpace("/gazette/cluster")

	var brokerA = newTestServer(pb.BrokerSpec{
		Id:           pb.BrokerSpec_ID{Zone: "A", Suffix: "one"},
		JournalLimit: 5,
	})
	var brokerB = newTestServer(pb.BrokerSpec{
		Id:           pb.BrokerSpec_ID{Zone: "A", Suffix: "one"},
		JournalLimit: 5,
	})

	brokerA.start(c, ctx, ks, session, etcd)
	brokerB.start(c, ctx, ks, session, etcd)

	var journal = createTestJournal(c, ks, etcd)

	var r = client.NewReader(ctx, brokerB.srv.MustClient(), pb.ReadRequest{
		Journal: journal,
		Block:   true,
	})
	var a = client.NewAppender(ctx, brokerA.srv.MustClient(), pb.AppendRequest{
		Journal: journal,
	})

	go func() {
		var _, err = a.Write([]byte("hello, world!\n")
		c.Check(err, gc.IsNil)
		c.Check(a.Close(), gc.IsNil)

	}()

	var br = bufio.NewReader(r)

}

type testBroker struct {
	spec        pb.BrokerSpec
	key         string
	state       *v3_allocator.State
	srv         *teststub.Server
	annoucement *v3_allocator.Announcement
	stopped     chan struct{}
}

func newTestServer(spec pb.BrokerSpec) *testBroker {
	return &testBroker{spec: spec}
}

func (s *testBroker) start(c *gc.C, ctx context.Context, ks *keyspace.KeySpace, session *concurrency.Session, etcd *clientv3.Client) {
	dialer, err := client.NewDialer(16)
	c.Assert(err, gc.IsNil)

	s.key = v3_allocator.MemberKey(ks, s.spec.Id.Zone, s.spec.Id.Suffix)
	s.state = v3_allocator.NewObservedState(ks, s.key)
	s.srv = teststub.NewServer(c, ctx, broker.NewService(dialer, s.state))
	s.spec.Endpoint = s.srv.Endpoint()

	s.annoucement, err = v3_allocator.Announce(context.Background(),
		etcd, s.key, s.spec.MarshalString(), session.Lease())
	c.Assert(err, gc.IsNil)

	s.stopped = make(chan struct{})
	v3_allocator.Allocate(v3_allocator.AllocateArgs{
		Context: ctx,
		Etcd:    etcd,
		State:   s.state,
	})
	close(s.stopped)
}

func (s *testBroker) gracefulStop(c *gc.C, ctx context.Context) {
	s.spec.JournalLimit = 0
	c.Check(s.annoucement.Update(ctx, s.spec.MarshalString()), gc.IsNil)
	<-s.stopped
}

func createTestJournal(c *gc.C, ks *keyspace.KeySpace, etcd *clientv3.Client) pb.Journal {
	var spec = pb.JournalSpec{
		Name:        "foo/bar",
		Replication: 2,
		Fragment: pb.JournalSpec_Fragment{
			Length:           1 << 16,
			CompressionCodec: pb.CompressionCodec_GZIP,
			RefreshInterval:  time.Second,
		},
		Labels: pb.LabelSet{
			Labels: []pb.Label{
				{Name: "label-key", Value: "label-value"},
				{Name: "topic", Value: "foo"},
			},
		},
	}
	c.Check(spec.Validate(), gc.IsNil)

	var _, err = etcd.Put(context.Background(),
		v3_allocator.ItemKey(ks, spec.Name.String()),
		spec.MarshalString(),
	)
	c.Check(err, gc.IsNil)

	return spec.Name
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
