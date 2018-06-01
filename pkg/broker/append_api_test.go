package broker

import (
	"errors"
	"io"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type AppendSuite struct{}

func (s *AppendSuite) TestSingle(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var stream, err = f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		var resolution, _ = f.resolver.resolve("a/journal", false, false)

		c.Check(stream.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
		s.expectPipelineSync(c, f.peer, resolution.route)

		c.Check(stream.Send(&pb.AppendRequest{Content: []byte("foo")}), gc.IsNil)
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("foo"), ContentDelta: 0})
		c.Check(stream.Send(&pb.AppendRequest{Content: []byte("bar")}), gc.IsNil)
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 3})

		// Close the Append RPC. Expect peer receives a commit request.
		c.Check(stream.CloseSend(), gc.IsNil)
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				CompressionCodec: pb.CompressionCodec_SNAPPY,
				Begin:            0,
				End:              6,
				Sum:              sumOf("foobar"),
			},
			Acknowledge: true,
		})
		f.peer.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK} // Acknowledge.

		resp, err := stream.CloseAndRecv()
		c.Check(err, gc.IsNil)

		c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
			Status: pb.Status_OK,
			Route:  resolution.route,
			Commit: &pb.Fragment{
				Journal: "a/journal",
				Begin:   0,
				End:     6,
				Sum:     sumOf("foobar"),
			},
		})
	})
}

func (s *AppendSuite) TestPipeline(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var resolution, _ = f.resolver.resolve("a/journal", false, false)

		// Build two raced Append requests.
		stream1, err := f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		stream2, err := f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		// |stream1| is sequenced first; expect it's replicated to the peer.
		c.Check(stream1.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
		s.expectPipelineSync(c, f.peer, resolution.route)
		c.Check(stream1.Send(&pb.AppendRequest{Content: []byte("foo")}), gc.IsNil)
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("foo"), ContentDelta: 0})

		c.Check(stream1.CloseSend(), gc.IsNil) // Expect client close triggers commit.
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				CompressionCodec: pb.CompressionCodec_SNAPPY,
				Begin:            0,
				End:              3,
				Sum:              sumOf("foo"),
			},
			Acknowledge: true,
		})

		// |stream2| follows. Expect it's also replicated, and no extra pipeline syncing is required.
		c.Check(stream2.Send(&pb.AppendRequest{Journal: "a/journal"}), gc.IsNil)
		c.Check(stream2.Send(&pb.AppendRequest{Content: []byte("bar")}), gc.IsNil)
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("bar"), ContentDelta: 0})
		c.Check(stream2.Send(&pb.AppendRequest{Content: []byte("baz")}), gc.IsNil)
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{Content: []byte("baz"), ContentDelta: 3})

		c.Check(stream2.CloseSend(), gc.IsNil) // Expect a commit.
		c.Check(<-f.peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
			Proposal: &pb.Fragment{
				Journal:          "a/journal",
				CompressionCodec: pb.CompressionCodec_SNAPPY,
				Begin:            0,
				End:              9,
				Sum:              sumOf("foobarbaz"),
			},
			Acknowledge: true,
		})

		// Peer finally acknowledges first commit. This unblocks |stream1|'s response.
		f.peer.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
		resp, err := stream1.CloseAndRecv()
		c.Check(err, gc.IsNil)

		c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
			Status: pb.Status_OK,
			Route:  resolution.route,
			Commit: &pb.Fragment{
				Journal: "a/journal",
				Begin:   0,
				End:     3,
				Sum:     sumOf("foo"),
			},
		})

		// Peer acknowledges second commit. |stream2|'s response is unblocked.
		f.peer.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK}
		resp, err = stream2.CloseAndRecv()
		c.Check(err, gc.IsNil)

		c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
			Status: pb.Status_OK,
			Route:  resolution.route,
			Commit: &pb.Fragment{
				Journal: "a/journal",
				Begin:   3,
				End:     9,
				Sum:     sumOf("barbaz"),
			},
		})

	})
}

func (s *AppendSuite) TestInvalidRequest(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var stream, err = f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		c.Check(stream.Send(&pb.AppendRequest{Journal: "/invalid/name"}), gc.IsNil)

		_, err = stream.CloseAndRecv()
		c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = Journal: cannot begin with '/' .*`)
	})
}

func (s *AppendSuite) TestNoPrimary(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var stream, err = f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		c.Check(stream.Send(&pb.AppendRequest{Journal: "no/primary"}), gc.IsNil)
		resp, err := stream.CloseAndRecv()

		var resolution, _ = f.resolver.resolve("no/primary", false, true)

		c.Check(err, gc.IsNil)
		c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
			Status: pb.Status_NO_JOURNAL_PRIMARY_BROKER,
			Route:  resolution.route,
		})
	})
}

func (s *AppendSuite) TestNoBrokers(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var stream, err = f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		c.Check(stream.Send(&pb.AppendRequest{Journal: "no/brokers"}), gc.IsNil)

		resp, err := stream.CloseAndRecv()
		c.Check(err, gc.IsNil)
		c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
			Status: pb.Status_NO_JOURNAL_BROKERS,
			Route:  &pb.Route{Primary: -1},
		})
	})
}

func (s *AppendSuite) TestNotFound(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var stream, err = f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		c.Check(stream.Send(&pb.AppendRequest{Journal: "no/brokers"}), gc.IsNil)

		resp, err := stream.CloseAndRecv()
		c.Check(err, gc.IsNil)
		c.Check(resp, gc.DeepEquals, &pb.AppendResponse{
			Status: pb.Status_NO_JOURNAL_BROKERS,
			Route:  &pb.Route{Primary: -1},
		})
	})

}

// TODO(johnny): Test startup retry case, whereby the peer returns a later revision.

func (s *AppendSuite) expectPipelineSync(c *gc.C, peer *mockPeer, route *pb.Route) {
	// Expect an initial request synchronizing the replication pipeline.
	c.Check(<-peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Journal: "a/journal",
		Route:   route,
		Proposal: &pb.Fragment{
			Journal: "a/journal",
		},
		Acknowledge: true,
	})
	peer.replRespCh <- &pb.ReplicateResponse{Status: pb.Status_OK} // Acknowledge.

	// Expect a non-ack'd command to roll the Spool to the SNAPPY codec (configured in the fixture).
	c.Check(<-peer.replReqCh, gc.DeepEquals, &pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
	})
}

func (s *AppendSuite) TestProxySuccess(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var stream, err = f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		// Expect initial request is proxied to the peer.
		c.Check(stream.Send(&pb.AppendRequest{Journal: "remote/journal"}), gc.IsNil)
		c.Check(<-f.peer.appendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "remote/journal"})

		// Expect client content and EOF are proxied.
		c.Check(stream.Send(&pb.AppendRequest{Content: []byte("foobar")}), gc.IsNil)
		c.Check(<-f.peer.appendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foobar")})

		c.Check(stream.CloseSend(), gc.IsNil)
		c.Check(<-f.peer.appendReqCh, gc.IsNil)

		// Expect peer's response is proxied back to the client.
		f.peer.appendRespCh <- &pb.AppendResponse{Commit: &pb.Fragment{Begin: 1234, End: 5678}}

		resp, err := stream.CloseAndRecv()
		c.Check(err, gc.IsNil)

		c.Check(resp, gc.DeepEquals, &pb.AppendResponse{Commit: &pb.Fragment{Begin: 1234, End: 5678}})
	})
}

func (s *AppendSuite) TestProxyError(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var stream, err = f.client.Append(f.ctx)
		c.Assert(err, gc.IsNil)

		// Expect initial request is proxied to the peer.
		c.Check(stream.Send(&pb.AppendRequest{Journal: "remote/journal"}), gc.IsNil)
		c.Check(<-f.peer.appendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "remote/journal"})

		f.peer.errCh <- errors.New("some kind of error")
		_, err = stream.CloseAndRecv()
		c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = some kind of error`)
	})
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
