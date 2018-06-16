package broker

import (
	"context"
	"errors"
	"io"

	gc "github.com/go-check/check"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type ReadSuite struct{}

// TODO(johnny): Test case covering remote fragment reads (not yet implemented).

func (s *ReadSuite) TestStreaming(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Make |chunkSize| small so we can test for chunking effects.
	defer func(cs int) { chunkSize = cs }(chunkSize)
	chunkSize = 5

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.BrokerSpec_ID{"local", "broker"})

	newTestJournal(c, ks, "a/journal", 2, broker.id)
	var res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "a/journal"})
	var spool, err = acquireSpool(ctx, res.replica, false)
	c.Check(err, gc.IsNil)

	stream, err := broker.MustClient().Read(ctx, &pb.ReadRequest{
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
		Header:    &res.Header,
		Offset:    0,
		WriteHead: 9,
		Fragment: &pb.Fragment{
			Journal: "a/journal",
			Begin:   0,
			End:     9,
			Sum:     pb.SHA1SumOf("foobarbaz"),
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
			Sum:     pb.SHA1SumOf("foobarbazbing"),
		},
	})
	expectReadResponse(c, stream, &pb.ReadResponse{
		Status:  pb.Status_OK,
		Offset:  9,
		Content: []byte("bing"),
	})

	cancel()
	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Canceled .*`)
}

func (s *ReadSuite) TestMetadataAndNonBlocking(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.BrokerSpec_ID{"local", "broker"})

	newTestJournal(c, ks, "a/journal", 2, broker.id)
	var res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "a/journal"})
	var spool, err = acquireSpool(ctx, res.replica, false)
	c.Check(err, gc.IsNil)

	spool.Apply(&pb.ReplicateRequest{Content: []byte("feedbeef")})
	spool.Apply(&pb.ReplicateRequest{Proposal: boxFragment(spool.Next())})

	stream, err := broker.MustClient().Read(ctx, &pb.ReadRequest{
		Journal:      "a/journal",
		Offset:       3,
		Block:        false,
		MetadataOnly: false,
	})
	c.Assert(err, gc.IsNil)
	c.Check(stream.CloseSend(), gc.IsNil)

	expectReadResponse(c, stream, &pb.ReadResponse{
		Status:    pb.Status_OK,
		Header:    &res.Header,
		Offset:    3,
		WriteHead: 8,
		Fragment: &pb.Fragment{
			Journal: "a/journal",
			Begin:   0,
			End:     8,
			Sum:     pb.SHA1SumOf("feedbeef"),
		},
	})
	expectReadResponse(c, stream, &pb.ReadResponse{
		Status:  pb.Status_OK,
		Offset:  3,
		Content: []byte("dbeef"),
	})
	expectReadResponse(c, stream, &pb.ReadResponse{
		Status:    pb.Status_OFFSET_NOT_YET_AVAILABLE,
		Offset:    8,
		WriteHead: 8,
	})

	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Now, issue a blocking metadata-only request.
	stream, err = broker.MustClient().Read(ctx, &pb.ReadRequest{
		Journal:      "a/journal",
		Offset:       8,
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
		Header:    &res.Header,
		Offset:    8,
		WriteHead: 12,
		Fragment: &pb.Fragment{
			Journal: "a/journal",
			Begin:   0,
			End:     12,
			Sum:     pb.SHA1SumOf("feedbeefbing"),
		},
	})

	// Expect no data is sent, and the stream is closed.
	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)
}

func (s *ReadSuite) TestProxyCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.BrokerSpec_ID{"local", "broker"})
	var peer = newMockBroker(c, ctx, ks, pb.BrokerSpec_ID{"peer", "broker"})

	newTestJournal(c, ks, "a/journal", 1, peer.id)
	var res, _ = broker.resolve(resolveArgs{ctx: ctx, journal: "a/journal", mayProxy: true})

	// Case: successfully proxies from peer.
	var req = &pb.ReadRequest{
		Journal:      "a/journal",
		Offset:       0,
		Block:        true,
		DoNotProxy:   false,
		MetadataOnly: false,
	}
	var stream, _ = broker.MustClient().Read(ctx, req)

	// Expect initial request is proxied to the peer, with attached Header, followed by client EOF.
	req.Header = &res.Header
	c.Check(<-peer.ReadReqCh, gc.DeepEquals, req)

	peer.ReadRespCh <- &pb.ReadResponse{Offset: 1234}
	peer.ReadRespCh <- &pb.ReadResponse{Offset: 5678}
	peer.ErrCh <- nil

	expectReadResponse(c, stream, &pb.ReadResponse{Offset: 1234})
	expectReadResponse(c, stream, &pb.ReadResponse{Offset: 5678})

	var _, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Case: proxy is not allowed.
	req = &pb.ReadRequest{
		Journal:    "a/journal",
		Offset:     0,
		DoNotProxy: true,
	}
	stream, _ = broker.MustClient().Read(ctx, req)

	expectReadResponse(c, stream, &pb.ReadResponse{
		Status: pb.Status_NOT_JOURNAL_BROKER,
		Header: boxHeaderBroker(res.Header, broker.id),
	})

	_, err = stream.Recv()
	c.Check(err, gc.Equals, io.EOF)

	// Case: remote broker returns an error.
	req = &pb.ReadRequest{
		Journal: "a/journal",
		Offset:  0,
	}
	stream, _ = broker.MustClient().Read(ctx, req)

	// Peer reads request, and returns an error.
	<-peer.ReadReqCh
	peer.ErrCh <- errors.New("some kind of error")

	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = some kind of error`)
}

func boxFragment(f pb.Fragment) *pb.Fragment { return &f }

func expectReadResponse(c *gc.C, stream pb.Broker_ReadClient, expect *pb.ReadResponse) {
	var resp, err = stream.Recv()
	c.Check(err, gc.IsNil)
	c.Check(resp, gc.DeepEquals, expect)
}

var _ = gc.Suite(&ReadSuite{})
