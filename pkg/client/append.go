package client

import (
	"context"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Append adapts an open Append RPC to the io.WriteCloser interface. Usages of
// an Append should be short-lived, as an in-progress RPC, by design, prevents
// the broker from serving other Append RPCs concurrently. Usages of Append
// should thus be limited to cases where the full and complete buffer to write
// is already available and can be immediately dispatched.
type Append struct {
	// Context of the Append. Notably, cancelling this context will also cancel
	// an Append RPC in progress.
	Context context.Context
	// Client used to dispatch the Append RPC.
	Client pb.BrokerClient
	// Journal which is to be appended to.
	Journal pb.Journal

	// Response holds the broker AppendResponse.
	Response pb.AppendResponse

	stream pb.Broker_AppendClient
}

func (w *Append) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return // The broker interprets empty chunks as "commit".
	}

	if w.stream == nil {
		w.stream, err = w.Client.Append(WithJournalHint(w.Context, w.Journal))

		if err == nil {
			err = w.stream.SendMsg(&pb.AppendRequest{Journal: w.Journal})
		}
	}

	if err != nil {
		// Pass.
	} else if err = w.stream.SendMsg(&pb.AppendRequest{Content: p}); err != nil {
		// Pass.
	} else {
		n = len(p)
	}

	if err != nil {
		if u, ok := w.Client.(RouteUpdater); ok {
			u.UpdateRoute(w.Journal, nil) // Purge cached Route.
		}
	}
	return
}

// Close the Append to complete the transaction, committing previously
// written content. If Close returns without an error, Append.Response
// will hold the broker response.
func (w *Append) Close() (err error) {
	// Send an empty chunk to signal commit of previously written content.
	if err = w.stream.SendMsg(&pb.AppendRequest{}); err != nil {
		// Pass.
	} else if err = w.stream.CloseSend(); err != nil {
		// Pass.
	} else if err = w.stream.RecvMsg(&w.Response); err != nil {
		// Pass.
	}

	if u, ok := w.Client.(RouteUpdater); ok {
		if err == nil {
			u.UpdateRoute(w.Journal, &w.Response.Header.Route)
		} else {
			u.UpdateRoute(w.Journal, nil)
		}
	}
	return
}

// Abort the write, causing the broker to discard previously written content.
func (w *Append) Abort() {
	// Abort is implicit by sending EOF without a preceding empty chunk.
	_, _ = w.stream.CloseAndRecv()
}

var chunkSize = 1 << 17 // 128K.
