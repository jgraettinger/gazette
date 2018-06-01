package broker

import (
	"errors"
	"io"

	gc "github.com/go-check/check"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type ReadSuite struct{}

func (s *ReadSuite) TestStreaming(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {

		// Make |chunkSize| small so we can test for chunking effects.
		defer func(cs int) { chunkSize = cs }(chunkSize)
		chunkSize = 5

		var resolution, _ = f.resolver.resolve("a/journal", false, false)
		var spool, err = resolution.replica.(*replicaImpl).acquireSpool(f.ctx, false)
		c.Check(err, gc.IsNil)

		stream, err := f.client.Read(f.ctx, &pb.ReadRequest{
			Journal:      "a/journal",
			Offset:       0,
			Block:        true,
			DoNotProxy:   true,
			MetadataOnly: false,
		})
		c.Assert(err, gc.IsNil)
		c.Check(stream.CloseSend(), gc.IsNil)

		spool.Apply(&pb.ReplicateRequest{Content: []byte("foobarbaz")})
		spool.Apply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:    pb.Status_OK,
			Offset:    0,
			WriteHead: 9,
			Route:     resolution.route,
			Fragment: &pb.Fragment{
				Journal: "a/journal",
				Begin:   0,
				End:     9,
				Sum:     sumOf("foobarbaz"),
			},
		})
		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:  pb.Status_OK,
			Offset:  0,
			Content: []byte("fooba"),
		})
		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:  pb.Status_OK,
			Offset:  5,
			Content: []byte("rbaz"),
		})

		// Commit more content. Expect the committed Fragment metadata is sent,
		// along with new commit content.
		spool.Apply(&pb.ReplicateRequest{Content: []byte("bing")})
		spool.Apply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:    pb.Status_OK,
			Offset:    9,
			WriteHead: 13,
			Fragment: &pb.Fragment{
				Journal: "a/journal",
				Begin:   0,
				End:     13,
				Sum:     sumOf("foobarbazbing"),
			},
		})
		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:  pb.Status_OK,
			Offset:  9,
			Content: []byte("bing"),
		})
	})
}

func (s *ReadSuite) TestMetadataAndNonBlocking(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var resolution, _ = f.resolver.resolve("a/journal", false, false)
		var spool, err = resolution.replica.(*replicaImpl).acquireSpool(f.ctx, false)
		c.Check(err, gc.IsNil)

		spool.Apply(&pb.ReplicateRequest{Content: []byte("foobarbaz")})
		spool.Apply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

		stream, err := f.client.Read(f.ctx, &pb.ReadRequest{
			Journal:      "a/journal",
			Offset:       3,
			Block:        false,
			MetadataOnly: false,
		})
		c.Assert(err, gc.IsNil)
		c.Check(stream.CloseSend(), gc.IsNil)

		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:    pb.Status_OK,
			Offset:    3,
			WriteHead: 9,
			Route:     resolution.route,
			Fragment: &pb.Fragment{
				Journal: "a/journal",
				Begin:   0,
				End:     9,
				Sum:     sumOf("foobarbaz"),
			},
		})
		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:  pb.Status_OK,
			Offset:  3,
			Content: []byte("barbaz"),
		})
		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:    pb.Status_OFFSET_NOT_YET_AVAILABLE,
			Offset:    9,
			WriteHead: 9,
		})

		_, err = stream.Recv()
		c.Check(err, gc.Equals, io.EOF)

		// Now, issue a blocking metadata-only request.
		stream, err = f.client.Read(f.ctx, &pb.ReadRequest{
			Journal:      "a/journal",
			Offset:       9,
			Block:        true,
			MetadataOnly: true,
		})
		c.Assert(err, gc.IsNil)
		c.Check(stream.CloseSend(), gc.IsNil)

		// Commit more content, unblocking our metadata request.
		spool.Apply(&pb.ReplicateRequest{Content: []byte("bing")})
		spool.Apply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

		expectReadResponse(c, stream, &pb.ReadResponse{
			Status:    pb.Status_OK,
			Offset:    9,
			WriteHead: 13,
			Route:     resolution.route,
			Fragment: &pb.Fragment{
				Journal: "a/journal",
				Begin:   0,
				End:     13,
				Sum:     sumOf("foobarbazbing"),
			},
		})

		// Expect no data is sent, and the stream is closed.
		_, err = stream.Recv()
		c.Check(err, gc.Equals, io.EOF)
	})
}

func (s *ReadSuite) TestProxySuccess(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var req = &pb.ReadRequest{
			Journal:      "remote/journal",
			Offset:       0,
			Block:        true,
			DoNotProxy:   false,
			MetadataOnly: false,
		}
		stream, err := f.client.Read(f.ctx, req)
		c.Assert(err, gc.IsNil)

		// Expect initial request is proxied to the peer.
		c.Check(<-f.peer.readReqCh, gc.DeepEquals, req)

		f.peer.readRespCh <- &pb.ReadResponse{Offset: 1234}
		f.peer.readRespCh <- &pb.ReadResponse{Offset: 5678}
		f.peer.errCh <- nil

		expectReadResponse(c, stream, &pb.ReadResponse{Offset: 1234})
		expectReadResponse(c, stream, &pb.ReadResponse{Offset: 5678})

		_, err = stream.Recv()
		c.Check(err, gc.Equals, io.EOF)
	})
}

func (s *ReadSuite) TestProxyNotAllowed(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var req = &pb.ReadRequest{
			Journal:    "remote/journal",
			Offset:     0,
			DoNotProxy: true,
		}
		stream, err := f.client.Read(f.ctx, req)
		c.Assert(err, gc.IsNil)

		var resolution, _ = f.resolver.resolve("remote/journal", false, true)

		expectReadResponse(c, stream, &pb.ReadResponse{
			Status: pb.Status_NOT_JOURNAL_BROKER,
			Route:  resolution.route,
		})
	})
}

func (s *ReadSuite) TestProxyError(c *gc.C) {
	runBrokerTestCase(c, func(f brokerFixture) {
		var req = &pb.ReadRequest{
			Journal: "remote/journal",
			Offset:  0,
		}
		stream, err := f.client.Read(f.ctx, req)
		c.Assert(err, gc.IsNil)

		c.Check(<-f.peer.readReqCh, gc.DeepEquals, req)
		f.peer.errCh <- errors.New("some kind of error")

		_, err = stream.Recv()
		c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = some kind of error`)
	})
}

func boxFragment(f pb.Fragment) *pb.Fragment { return &f }

func expectReadResponse(c *gc.C, stream pb.Broker_ReadClient, expect *pb.ReadResponse) {
	var resp, err = stream.Recv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expect)
}

var _ = gc.Suite(&ReadSuite{})
