package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	gc "github.com/go-check/check"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type PipelineSuite struct{}

func (s *PipelineSuite) TestBasicLifeCycle(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = newPipeline(rm.ctx, rm.route, fragment.NewSpool("a/journal", rm), rm.connFn)

	var req = &pb.ReplicateRequest{Content: []byte("foobar")}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.reqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.reqCh, gc.DeepEquals, req)

	var proposal = pln.spool.Next()
	req = &pb.ReplicateRequest{Proposal: &proposal}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.reqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.reqCh, gc.DeepEquals, req)

	rm.brokerA.respCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	rm.brokerC.respCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

	pln.gatherOK()
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.recvResp, gc.DeepEquals, []pb.ReplicateResponse{{}, {}, {}})

	var spool = make(chan fragment.Spool, 1)
	pln.closeSend(spool)

	c.Check(<-rm.brokerA.reqCh, gc.IsNil) // Expect EOF.
	c.Check(<-rm.brokerC.reqCh, gc.IsNil) // Expect EOF.

	rm.brokerA.errCh <- nil // Send EOF.
	rm.brokerC.errCh <- nil // Send EOF.

	pln.gatherEOF()

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(pln.recvErr(), gc.IsNil)
}

func (s *PipelineSuite) TestPeerErrorCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = newPipeline(rm.ctx, rm.route, fragment.NewSpool("a/journal", rm), rm.connFn)

	var req = &pb.ReplicateRequest{Content: []byte("foo")}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.reqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.reqCh, gc.DeepEquals, req)

	// Have peer A return an error. Peer B returns a non-OK response status (where OK is expected).
	rm.brokerA.errCh <- errors.New("error!")
	rm.brokerC.respCh <- &pb.ReplicateResponse{Status: pb.Status_FRAGMENT_MISMATCH}

	// Expect pipeline retains the first recv error for each peer.
	pln.gatherOK()
	c.Check(pln.recvErrs[0], gc.ErrorMatches, `rpc error: code = Unknown desc = error!`)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected !OK response: status:FRAGMENT_MISMATCH `)

	// Expect recvErr decorates the first error with peer metadata.
	c.Check(pln.recvErr(), gc.ErrorMatches, `recv from zone:"A" suffix:"1" : rpc error: .*`)

	pln.spool.Fragment.File.Close() // Force a write error of the local Spool.

	req = &pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 3}
	pln.scatter(req)

	// Expect pipeline retains the first send error for each peer, including the local Spool.
	c.Check(pln.sendErrs[0], gc.ErrorMatches, `EOF`)
	c.Check(pln.sendErrs[1], gc.ErrorMatches, `write .*: file already closed`)
	c.Check(pln.sendErrs[2], gc.IsNil) // Send-side of connection is still valid (only recv is broken).

	c.Check(<-rm.brokerC.reqCh, gc.DeepEquals, req)

	// Expect sendErr decorates the first error with peer metadata.
	c.Check(pln.sendErr(), gc.ErrorMatches, `send to zone:"A" suffix:"1" : EOF`)

	var spoolCh = make(chan fragment.Spool, 1)
	pln.closeSend(spoolCh)

	// Finish shutdown by having brokerC receive and send EOF.
	c.Check(<-rm.brokerC.reqCh, gc.IsNil)
	rm.brokerC.errCh <- nil

	// Restart a new pipeline. Immediately send an EOF, and test handling of
	// an unexpected received message prior to peer EOF.
	pln = newPipeline(rm.ctx, rm.route, <-spoolCh, rm.connFn)
	pln.closeSend(spoolCh)

	c.Check(<-rm.brokerA.reqCh, gc.IsNil) // Read EOF.
	c.Check(<-rm.brokerC.reqCh, gc.IsNil) // Read EOF.

	rm.brokerA.errCh <- nil                                                   // Send EOF.
	rm.brokerC.respCh <- &pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE} // Unexpected response.
	rm.brokerC.errCh <- nil                                                   // Now, send EOF.

	pln.gatherEOF()
	c.Check(pln.recvErrs[0], gc.IsNil)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected response: status:WRONG_ROUTE `)
}

func (s *PipelineSuite) TestSyncCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = newPipeline(rm.ctx, rm.route, fragment.NewSpool("a/journal", rm), rm.connFn)

	var req = &pb.ReplicateRequest{
		Journal:     "a/journal",
		Route:       &pln.route,
		Proposal:    &pb.Fragment{Journal: "a/journal", Begin: 123, End: 123},
		Acknowledge: true,
	}
	pln.scatter(req)

	c.Check(<-rm.brokerA.reqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.reqCh, gc.DeepEquals, req)

	rm.brokerA.respCh <- &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Route:  &pb.Route{Revision: 4567},
	}
	rm.brokerC.respCh <- &pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{Begin: 567, End: 890},
	}

	// Expect the new Fragment offset and Etcd revision to read through are returned.
	var rollToOffset, readRev, err = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(890))
	c.Check(readRev, gc.Equals, int64(4567))
	c.Check(err, gc.IsNil)

	// Again. This time peers return success.
	req.Proposal = &pb.Fragment{Journal: "a/journal", Begin: 890, End: 890}
	pln.scatter(req)

	_, _ = <-rm.brokerA.reqCh, <-rm.brokerC.reqCh
	rm.brokerA.respCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	rm.brokerC.respCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

	rollToOffset, readRev, err = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(0))
	c.Check(readRev, gc.Equals, int64(0))
	c.Check(err, gc.IsNil)

	// Again. This time, peers return !OK status with invalid responses.
	pln.scatter(req)

	_, _ = <-rm.brokerA.reqCh, <-rm.brokerC.reqCh

	rm.brokerA.respCh <- &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Route:  &pb.Route{Revision: pln.route.Revision}, // Revision not greater than |pln|'s.
	}
	rm.brokerC.respCh <- &pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{Begin: 567, End: 889}, // End offset < proposal.
	}

	rollToOffset, readRev, err = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(0))
	c.Check(readRev, gc.Equals, int64(0))
	c.Check(err, gc.NotNil)

	c.Check(pln.recvErrs[0], gc.ErrorMatches, `unexpected WRONG_ROUTE revision: revision:\d+ .*`)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected FRAGMENT_MISMATCH: begin:567 end:889 .*`)
}

type replicationMock struct {
	ctx    context.Context
	cancel context.CancelFunc

	route            pb.Route
	brokerA, brokerC *mockPeer

	connFn buildConnFn

	commits   []fragment.Fragment
	completes []fragment.Spool
}

func newReplicationMock(c *gc.C) *replicationMock {
	var ctx, cancel = context.WithCancel(context.Background())

	var m = &replicationMock{
		ctx:    ctx,
		cancel: cancel,
		route: pb.Route{
			Primary: 1,
			Brokers: []pb.BrokerSpec_ID{
				{Zone: "A", Suffix: "1"},
				{Zone: "B", Suffix: "2"},
				{Zone: "C", Suffix: "3"},
			},
			Revision: 1234,
		},

		brokerA: newMockPeer(c, ctx),
		brokerC: newMockPeer(c, ctx),
	}

	m.connFn = func(id pb.BrokerSpec_ID) (*grpc.ClientConn, error) {
		switch id {
		case pb.BrokerSpec_ID{Zone: "A", Suffix: "1"}:
			return m.brokerA.dial()
		case pb.BrokerSpec_ID{Zone: "C", Suffix: "3"}:
			return m.brokerC.dial()
		default:
			return nil, fmt.Errorf("unexpected ID: %s", id)
		}
	}

	return m
}

func (m *replicationMock) SpoolCommit(f fragment.Fragment) { m.commits = append(m.commits, f) }
func (m *replicationMock) SpoolComplete(s fragment.Spool)  { m.completes = append(m.completes, s) }

type mockPeer struct {
	c        *gc.C
	ctx      context.Context
	listener net.Listener
	srv      *grpc.Server

	reqCh  chan *pb.ReplicateRequest
	respCh chan *pb.ReplicateResponse
	errCh  chan error
}

func (p *mockPeer) dial() (*grpc.ClientConn, error) {
	return grpc.Dial(p.listener.Addr().String(), grpc.WithInsecure())
}

func newMockPeer(c *gc.C, ctx context.Context) *mockPeer {
	var l, err = net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, gc.IsNil)

	var p = &mockPeer{
		c:        c,
		ctx:      ctx,
		listener: l,
		srv:      grpc.NewServer(),
		reqCh:    make(chan *pb.ReplicateRequest),
		respCh:   make(chan *pb.ReplicateResponse),
		errCh:    make(chan error),
	}

	pb.RegisterBrokerServer(p.srv, p)
	go p.srv.Serve(p.listener)

	go func() {
		<-ctx.Done()
		p.srv.GracefulStop()
	}()

	return p
}

func (p *mockPeer) addr() string { return p.listener.Addr().String() }

func (p *mockPeer) Replicate(srv pb.Broker_ReplicateServer) error {
	// Start a read loop of requests from |srv|.
	go func() {
		logrus.WithField("id", p.addr()).Info("read loop started")
		for done := false; !done; {
			var msg, err = srv.Recv()

			if err == io.EOF {
				msg, err, done = nil, nil, true
			} else if err != nil {
				done = true

				p.c.Check(err, gc.ErrorMatches, `rpc error: code = Canceled desc = context canceled`)
			}

			logrus.WithFields(logrus.Fields{"id": p.addr(), "msg": msg, "err": err, "done": done}).Info("read")

			select {
			case p.reqCh <- msg:
				// Pass.
			case <-p.ctx.Done():
				done = true
			}
		}
	}()

	for {
		select {
		case resp := <-p.respCh:
			p.c.Check(srv.Send(resp), gc.IsNil)
			logrus.WithFields(logrus.Fields{"id": p.addr(), "resp": resp}).Info("sent")
		case err := <-p.errCh:
			logrus.WithFields(logrus.Fields{"id": p.addr(), "err": err}).Info("closing")
			return err
		case <-p.ctx.Done():
			logrus.WithFields(logrus.Fields{"id": p.addr()}).Info("cancelled")
			return p.ctx.Err()
		}
	}
}

func (p *mockPeer) Read(*pb.ReadRequest, pb.Broker_ReadServer) error { panic("not implemented") }

func (p *mockPeer) Append(pb.Broker_AppendServer) error { panic("not implemented") }

var _ = gc.Suite(&PipelineSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
