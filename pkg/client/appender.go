package client

import (
	"context"
	"errors"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Append adapts an Append RPC to the io.WriteCloser interface. Its usages should
// should be limited to cases where the full and complete buffer to append is
// already available and can be immediately dispatched as, by design, an in-
// progress RPC prevents the broker from serving other Append RPCs concurrently.
type Appender struct {
	Request  pb.AppendRequest  // AppendRequest of the Append.
	Response pb.AppendResponse // AppendResponse sent by broker.

	ctx    context.Context
	client pb.BrokerClient        // Client against which Read is dispatched.
	stream pb.Broker_AppendClient // Server stream.
}

func NewAppender(ctx context.Context, client pb.BrokerClient, req pb.AppendRequest) *Appender {
	var a = &Appender{
		Request: req,
		ctx:     ctx,
		client:  client,
	}
	return a
}

func (a *Appender) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return // The broker interprets empty chunks as "commit".
	}

	// Lazy initialization: begin the Append RPC.
	if a.stream == nil {
		a.stream, err = a.client.Append(WithJournalHint(a.ctx, a.Request.Journal))

		if err == nil {
			err = a.stream.SendMsg(&a.Request)
		}
	}

	if err != nil {
		// Pass.
	} else if err = a.stream.SendMsg(&pb.AppendRequest{Content: p}); err != nil {
		// Pass.
	} else {
		n = len(p)
	}

	if err != nil {
		if u, ok := a.client.(RouteUpdater); ok {
			u.UpdateRoute(a.Request.Journal, nil) // Purge cached Route.
		}
	}
	return
}

// Close the Append to complete the transaction, committing previously
// written content. If Close returns without an error, Append.Response
// will hold the broker response.
func (a *Appender) Close() (err error) {
	// Send an empty chunk to signal commit of previously written content.
	if err = a.stream.SendMsg(&pb.AppendRequest{}); err != nil {
		// Pass.
	} else if err = a.stream.CloseSend(); err != nil {
		// Pass.
	} else if err = a.stream.RecvMsg(&a.Response); err != nil {
		// Pass.
	} else if err = a.Response.Validate(); err != nil {
		// Pass.
	} else if a.Response.Status != pb.Status_OK {
		err = errors.New(a.Response.Status.String())
	}

	if u, ok := a.client.(RouteUpdater); ok {
		if err == nil {
			u.UpdateRoute(a.Request.Journal, &a.Response.Header.Route)
		} else {
			u.UpdateRoute(a.Request.Journal, nil)
		}
	}
	return
}

// Abort the write, causing the broker to discard previously written content.
func (a *Appender) Abort() {
	// Abort is implied by sending EOF without a preceding empty chunk.
	_, _ = a.stream.CloseAndRecv()
}

var chunkSize = 1 << 17 // 128K.
