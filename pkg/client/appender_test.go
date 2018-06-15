package client

import (
	"context"

	"github.com/LiveRamp/gazette/pkg/protocol"
	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
)

type AppenderSuite struct{}

func (s *AppenderSuite) TestBasic(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	var a = NewAppender(ctx, broker.MustClient(), &protocol.AppendRequest{"a/journal"})

}

var _ = gc.Suite(&AppenderSuite{})
