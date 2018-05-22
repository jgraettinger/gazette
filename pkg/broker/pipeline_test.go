package broker

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	pbmocks "github.com/LiveRamp/gazette/pkg/protocol/mocks"
	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type PipelineSuite struct{}

func (s *PipelineSuite) TestFoo(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.Stop()

	var pln = newPipeline(rm.ctx, rm.route, fragment.NewSpool("a/journal", rm), rm.connFn)
	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(pln.recvErr(), gc.IsNil)
}

type replicationMock struct {
	ctx    context.Context
	cancel context.CancelFunc

	route pb.Route

	brokerA, brokerC pbmocks.BrokerServer
	srvA, srvC       *grpc.Server

	replA, replB     chan pb.Broker_ReplicateServer
	finishA, finishB chan error

	connFn buildConnFn

	commits   []fragment.Fragment
	completes []fragment.Spool
}

func newReplicationMock(c *gc.C) *replicationMock {
	var m = &replicationMock{
		route: pb.Route{
			Primary: 1,
			Brokers: []pb.BrokerSpec_ID{
				{Zone: "A", Suffix: "1"},
				{Zone: "B", Suffix: "2"},
				{Zone: "C", Suffix: "3"},
			},
		},

		srvA: grpc.NewServer(),
		srvC: grpc.NewServer(),

		replA:   make(chan pb.Broker_ReplicateServer),
		replB:   make(chan pb.Broker_ReplicateServer),
		finishA: make(chan error),
		finishB: make(chan error),
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())

	pb.RegisterBrokerServer(m.srvA, &m.brokerA)
	pb.RegisterBrokerServer(m.srvC, &m.brokerC)

	lA, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, gc.IsNil)
	lC, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, gc.IsNil)

	go m.srvA.Serve(lA)
	go m.srvC.Serve(lC)

	m.connFn = func(id pb.BrokerSpec_ID) (*grpc.ClientConn, error) {
		switch id {
		case pb.BrokerSpec_ID{Zone: "A", Suffix: "1"}:
			return grpc.Dial(lA.Addr().String(), grpc.WithInsecure())
		case pb.BrokerSpec_ID{Zone: "C", Suffix: "2"}:
			return grpc.Dial(lC.Addr().String(), grpc.WithInsecure())
		default:
			return nil, fmt.Errorf("unexpected ID: %s", id)
		}
	}

	s0.mock.On("CurrentConsumerState", mock.Anything, &Empty{}).Return(
		buildConsumerStateFixture(s0.addr, s1.addr, s2.addr), nil).Once()

	return m
}

func (m *replicationMock) Stop() {
	m.cancel()
	m.srvA.GracefulStop()
	m.srvC.GracefulStop()
}

func (m *replicationMock) SpoolCommit(f fragment.Fragment) { m.commits = append(m.commits, f) }
func (m *replicationMock) SpoolComplete(s fragment.Spool)  { m.completes = append(m.completes, s) }

var _ = gc.Suite(&PipelineSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
