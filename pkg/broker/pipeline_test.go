package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/coreos/etcd/integration"
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

	var pln = newPipeline(rm.ctx, rm.route, fragment.NewSpool("a/journal", rm), rm)

	var req = &pb.ReplicateRequest{Content: []byte("foobar")}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.replReqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.replReqCh, gc.DeepEquals, req)

	var proposal = pln.spool.Next()
	req = &pb.ReplicateRequest{Proposal: &proposal}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.replReqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.replReqCh, gc.DeepEquals, req)

	rm.brokerA.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	rm.brokerC.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

	pln.gatherOK()
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.recvResp, gc.DeepEquals, []pb.ReplicateResponse{{}, {}, {}})

	var spool = make(chan fragment.Spool, 1)
	pln.closeSend(spool)

	c.Check(<-rm.brokerA.replReqCh, gc.IsNil) // Expect EOF.
	c.Check(<-rm.brokerC.replReqCh, gc.IsNil) // Expect EOF.

	rm.brokerA.errCh <- nil // Send EOF.
	rm.brokerC.errCh <- nil // Send EOF.

	pln.gatherEOF()

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(pln.recvErr(), gc.IsNil)
}

func (s *PipelineSuite) TestPeerErrorCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = newPipeline(rm.ctx, rm.route, fragment.NewSpool("a/journal", rm), rm)

	var req = &pb.ReplicateRequest{Content: []byte("foo")}
	pln.scatter(req)

	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(<-rm.brokerA.replReqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.replReqCh, gc.DeepEquals, req)

	// Have peer A return an error. Peer B returns a non-OK response status (where OK is expected).
	rm.brokerA.errCh <- errors.New("error!")
	rm.brokerC.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_FRAGMENT_MISMATCH}

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

	c.Check(<-rm.brokerC.replReqCh, gc.DeepEquals, req)

	// Expect sendErr decorates the first error with peer metadata.
	c.Check(pln.sendErr(), gc.ErrorMatches, `send to zone:"A" suffix:"1" : EOF`)

	var spoolCh = make(chan fragment.Spool, 1)
	pln.closeSend(spoolCh)

	// Finish shutdown by having brokerC receive and send EOF.
	c.Check(<-rm.brokerC.replReqCh, gc.IsNil)
	rm.brokerC.errCh <- nil

	// Restart a new pipeline. Immediately send an EOF, and test handling of
	// an unexpected received message prior to peer EOF.
	pln = newPipeline(rm.ctx, rm.route, <-spoolCh, rm)
	pln.closeSend(spoolCh)

	c.Check(<-rm.brokerA.replReqCh, gc.IsNil) // Read EOF.
	c.Check(<-rm.brokerC.replReqCh, gc.IsNil) // Read EOF.

	rm.brokerA.errCh <- nil                                                       // Send EOF.
	rm.brokerC.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE} // Unexpected response.
	rm.brokerC.errCh <- nil                                                       // Now, send EOF.

	pln.gatherEOF()
	c.Check(pln.recvErrs[0], gc.IsNil)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected response: status:WRONG_ROUTE `)
}

func (s *PipelineSuite) TestGatherSyncCases(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var pln = newPipeline(rm.ctx, rm.route, fragment.NewSpool("a/journal", rm), rm)

	var req = &pb.ReplicateRequest{
		Journal:     "a/journal",
		Route:       &pln.route,
		Proposal:    &pb.Fragment{Journal: "a/journal", Begin: 123, End: 123},
		Acknowledge: true,
	}
	pln.scatter(req)

	c.Check(<-rm.brokerA.replReqCh, gc.DeepEquals, req)
	c.Check(<-rm.brokerC.replReqCh, gc.DeepEquals, req)

	rm.brokerA.replRespCh <- &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Route:  &pb.Route{Revision: 4567},
	}
	rm.brokerC.replRespCh <- &pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{Begin: 567, End: 890},
	}

	// Expect the new Fragment offset and Etcd revision to read through are returned.
	var rollToOffset, readRev = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(890))
	c.Check(readRev, gc.Equals, int64(4567))
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.sendErr(), gc.IsNil)

	// Again. This time peers return success.
	req.Proposal = &pb.Fragment{Journal: "a/journal", Begin: 890, End: 890}
	pln.scatter(req)

	_, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
	rm.brokerA.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
	rm.brokerC.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

	rollToOffset, readRev = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(0))
	c.Check(readRev, gc.Equals, int64(0))
	c.Check(pln.recvErr(), gc.IsNil)
	c.Check(pln.sendErr(), gc.IsNil)

	// Again. This time, peers return !OK status with invalid responses.
	pln.scatter(req)

	_, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh

	rm.brokerA.replRespCh <- &pb.ReplicateResponse{
		Status: pb.Status_WRONG_ROUTE,
		Route:  &pb.Route{Revision: pln.route.Revision}, // Revision not greater than |pln|'s.
	}
	rm.brokerC.replRespCh <- &pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &pb.Fragment{Begin: 567, End: 889}, // End offset < proposal.
	}

	rollToOffset, readRev = pln.gatherSync(*req.Proposal)
	c.Check(rollToOffset, gc.Equals, int64(0))
	c.Check(readRev, gc.Equals, int64(0))
	c.Check(pln.sendErr(), gc.IsNil)
	c.Check(pln.recvErr(), gc.NotNil)

	c.Check(pln.recvErrs[0], gc.ErrorMatches, `unexpected WRONG_ROUTE revision: revision:\d+ .*`)
	c.Check(pln.recvErrs[1], gc.IsNil)
	c.Check(pln.recvErrs[2], gc.ErrorMatches, `unexpected FRAGMENT_MISMATCH: begin:567 end:889 .*`)
}

func (s *PipelineSuite) TestPipelineStart(c *gc.C) {
	var rm = newReplicationMock(c)
	defer rm.cancel()

	var spool = fragment.NewSpool("a/journal", rm)
	spool.Fragment.End = 123
	spool.Fragment.Sum = pb.SHA1Sum{Part1: 999}

	var pln = newPipeline(rm.ctx, rm.route, spool, rm)

	go func() {
		// Read sync request.
		c.Check(<-rm.brokerA.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
			Journal:     "a/journal",
			Route:       &pln.route,
			Proposal:    &pb.Fragment{Journal: "a/journal", Begin: 0, End: 123, Sum: pb.SHA1Sum{Part1: 999}},
			Acknowledge: true,
		})
		_ = <-rm.brokerC.replReqCh

		// Peers disagree on Fragment End.
		rm.brokerA.replRespCh <- &pb.ReplicateResponse{
			Status:   pb.Status_FRAGMENT_MISMATCH,
			Fragment: &pb.Fragment{Begin: 567, End: 892},
		}
		rm.brokerC.replRespCh <- &pb.ReplicateResponse{
			Status:   pb.Status_FRAGMENT_MISMATCH,
			Fragment: &pb.Fragment{Begin: 567, End: 890},
		}

		// Next iteration. Expect proposal is updated to reflect largest offset.
		c.Check(<-rm.brokerA.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
			Journal:     "a/journal",
			Route:       &pln.route,
			Proposal:    &pb.Fragment{Journal: "a/journal", Begin: 892, End: 892},
			Acknowledge: true,
		})
		_ = <-rm.brokerC.replReqCh

		// Peers agree.
		rm.brokerA.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
		rm.brokerC.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

		// Next round.
		_, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh

		// Peer C response with a larger Etcd revision.
		rm.brokerA.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
		rm.brokerC.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE, Route: &pb.Route{Revision: 4567}}

		// Expect start() sends EOF.
		c.Check(<-rm.brokerA.replReqCh, gc.IsNil)
		c.Check(<-rm.brokerC.replReqCh, gc.IsNil)
		rm.brokerA.errCh <- nil
		rm.brokerC.errCh <- nil

		// Next round.
		_, _ = <-rm.brokerA.replReqCh, <-rm.brokerC.replReqCh
		rm.brokerA.errCh <- errors.New("an error")
		rm.brokerC.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}

		// Expect EOF.
		c.Check(<-rm.brokerA.replReqCh, gc.IsNil)
		c.Check(<-rm.brokerC.replReqCh, gc.IsNil)
		rm.brokerC.errCh <- nil // |brokerA| has already closed.
	}()

	c.Check(pln.start(nil), gc.IsNil)
	c.Check(pln.readThroughRev, gc.Equals, int64(0))

	// Next round. This time, the pipeline is closed and readThroughRev is set.
	var spoolCh = make(chan fragment.Spool, 1)
	c.Check(pln.start(spoolCh), gc.IsNil)
	c.Check(pln.readThroughRev, gc.Equals, int64(4567))

	// New pipeline & round. Peer returns an error, and it's passed through.
	pln = newPipeline(rm.ctx, rm.route, <-spoolCh, rm)
	c.Check(pln.start(spoolCh), gc.ErrorMatches, `recv from zone:"A" suffix:"1" : rpc error: .*`)
}

type replicationMock struct {
	ctx    context.Context
	cancel context.CancelFunc

	route            pb.Route
	brokerA, brokerC *mockPeer

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
	return m
}

func (m *replicationMock) dial(ctx context.Context, id pb.BrokerSpec_ID) (*grpc.ClientConn, error) {
	switch id {
	case pb.BrokerSpec_ID{Zone: "A", Suffix: "1"}:
		return m.brokerA.dial(ctx)
	case pb.BrokerSpec_ID{Zone: "C", Suffix: "3"}:
		return m.brokerC.dial(ctx)
	default:
		return nil, fmt.Errorf("unexpected ID: %s", id)
	}
}

func (m *replicationMock) SpoolCommit(f fragment.Fragment) { m.commits = append(m.commits, f) }
func (m *replicationMock) SpoolComplete(s fragment.Spool)  { m.completes = append(m.completes, s) }

type testServer struct {
	c        *gc.C
	ctx      context.Context
	listener net.Listener
	srv      *grpc.Server
}

func newTestServer(c *gc.C, ctx context.Context, srv pb.BrokerServer) *testServer {
	var l, err = net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, gc.IsNil)

	var p = &testServer{
		c:        c,
		ctx:      ctx,
		listener: l,
		srv:      grpc.NewServer(),
	}

	pb.RegisterBrokerServer(p.srv, srv)
	go p.srv.Serve(p.listener)

	go func() {
		<-ctx.Done()
		p.srv.GracefulStop()
	}()

	return p
}

func (s *testServer) addr() string { return s.listener.Addr().String() }

func (s *testServer) dial(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, s.listener.Addr().String(), grpc.WithInsecure())
}

func (s *testServer) mustDial() *grpc.ClientConn {
	var conn, err = s.dial(s.ctx)
	s.c.Assert(err, gc.IsNil)
	return conn
}

type mockPeer struct {
	*testServer

	replReqCh  chan *pb.ReplicateRequest
	replRespCh chan *pb.ReplicateResponse

	readReqCh  chan *pb.ReadRequest
	readRespCh chan *pb.ReadResponse

	appendReqCh  chan *pb.AppendRequest
	appendRespCh chan *pb.AppendResponse

	errCh chan error
}

func newMockPeer(c *gc.C, ctx context.Context) *mockPeer {
	var p = &mockPeer{
		replReqCh:    make(chan *pb.ReplicateRequest),
		replRespCh:   make(chan *pb.ReplicateResponse),
		readReqCh:    make(chan *pb.ReadRequest),
		readRespCh:   make(chan *pb.ReadResponse),
		appendReqCh:  make(chan *pb.AppendRequest),
		appendRespCh: make(chan *pb.AppendResponse),
		errCh:        make(chan error),
	}
	p.testServer = newTestServer(c, ctx, p)
	return p
}

func (p *mockPeer) Replicate(srv pb.Broker_ReplicateServer) error {
	// Start a read loop of requests from |srv|.
	go func() {
		logrus.WithField("id", p.addr()).Info("replicate read loop started")
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
			case p.replReqCh <- msg:
				// Pass.
			case <-p.ctx.Done():
				done = true
			}
		}
	}()

	for {
		select {
		case resp := <-p.replRespCh:
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

func (p *mockPeer) Read(req *pb.ReadRequest, srv pb.Broker_ReadServer) error {
	select {
	case p.readReqCh <- req:
		// Pass.
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	for {
		select {
		case resp := <-p.readRespCh:
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

func (p *mockPeer) Append(srv pb.Broker_AppendServer) error {
	// Start a read loop of requests from |srv|.
	go func() {
		logrus.WithField("id", p.addr()).Info("append read loop started")
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
			case p.appendReqCh <- msg:
				// Pass.
			case <-p.ctx.Done():
				done = true
			}
		}
	}()

	for {
		select {
		case resp := <-p.appendRespCh:
			logrus.WithFields(logrus.Fields{"id": p.addr(), "resp": resp}).Info("sending")
			return srv.SendAndClose(resp)
		case err := <-p.errCh:
			logrus.WithFields(logrus.Fields{"id": p.addr(), "err": err}).Info("closing")
			return err
		case <-p.ctx.Done():
			logrus.WithFields(logrus.Fields{"id": p.addr()}).Info("cancelled")
			return p.ctx.Err()
		}
	}
}

var (
	_           = gc.Suite(&PipelineSuite{})
	etcdCluster *integration.ClusterV3
)

func Test(t *testing.T) {
	etcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	gc.TestingT(t)
	etcdCluster.Terminate(t)
}
