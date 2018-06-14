package client

import (
	"context"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type RetrySuite struct{}

func (s *RetrySuite) TestFoo(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var client = routeWrapper{BrokerClient: broker.MustClient()}

	var rr = NewRetryReader(ctx, client, pb.ReadRequest{Journal: "a/journal", Offset: 100})
	c.Check(rr.Offset(), gc.Equals, int64(100))
	c.Check(rr.Journal(), gc.Equals, pb.Journal("a/journal"))

}

var _ = gc.Suite(&RetrySuite{})
