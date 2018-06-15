package client

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
)

type AppenderSuite struct{}

func (s *AppenderSuite) TestCommitSuccess(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var routes = make(map[pb.Journal]*pb.Route)
	var client = routeWrapper{broker.MustClient(), routes}

	var a = NewAppender(ctx, client, pb.AppendRequest{Journal: "a/journal"})

	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foo")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("bar")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})

		broker.AppendRespCh <- &pb.AppendResponse{
			Status: pb.Status_OK,
			Header: &pb.Header{
				BrokerId: pb.BrokerSpec_ID{Zone: "a", Suffix: "broker"},
				Route: pb.Route{
					Brokers: []pb.BrokerSpec_ID{{Zone: "a", Suffix: "broker"}},
					Primary: 0,
				},
				Etcd: pb.Header_Etcd{
					ClusterId: 12,
					MemberId:  34,
					Revision:  56,
					RaftTerm:  78,
				},
			},
			Commit: &pb.Fragment{Journal: "a/journal", Begin: 100, End: 106, Sum: pb.SHA1SumOf("foobar")},
		}
		broker.ErrCh <- nil
	}()

	var n, err = a.Write([]byte("foo"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	n, err = a.Write([]byte("bar"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	c.Check(a.Close(), gc.IsNil)
	c.Check(a.Response.Commit.Journal, gc.Equals, pb.Journal("a/journal"))

	// Expect Appender advised of the updated Route.
	c.Check(routes["a/journal"], gc.DeepEquals, &pb.Route{
		Brokers: []pb.BrokerSpec_ID{{Zone: "a", Suffix: "broker"}},
		Primary: 0,
	})
}

func (s *AppenderSuite) TestBrokerWriteError(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var routes = make(map[pb.Journal]*pb.Route)
	var client = routeWrapper{broker.MustClient(), routes}

	var a = NewAppender(ctx, client, pb.AppendRequest{Journal: "a/journal"})

	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		broker.ErrCh <- errors.New("an error")
	}()

	var n, err = a.Write([]byte("foo"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	for err == nil {
		time.Sleep(time.Millisecond)
		n, err = a.Write([]byte("x"))
	}
	// NOTE(johnny): For some reason, gRPC translates the broker error into an EOF on attempt Send.
	c.Check(err, gc.Equals, io.EOF)
	c.Check(n, gc.Equals, 0)

	// Expect Appender cleared any Route advisement.
	c.Check(routes["a/journal"], gc.IsNil)
	c.Check(routes, gc.HasLen, 1)
}

func (s *AppenderSuite) TestBrokerCommitError(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var routes = make(map[pb.Journal]*pb.Route)
	var client = routeWrapper{broker.MustClient(), routes}

	var a = NewAppender(ctx, client, pb.AppendRequest{Journal: "a/journal"})

	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("foo")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})

		broker.ErrCh <- errors.New("an error")
	}()

	var n, err = a.Write([]byte("foo"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 3)

	c.Check(a.Close(), gc.ErrorMatches, `rpc error: code = Unknown desc = an error`)

	// Expect Appender cleared any Route advisement.
	c.Check(routes["a/journal"], gc.IsNil)
	c.Check(routes, gc.HasLen, 1)
}

// TODO - test Validate failure
// TODO - test non-OK Status.

// TODO - test client-side abort

var _ = gc.Suite(&AppenderSuite{})
