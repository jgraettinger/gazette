package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"time"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

func NewReader(ctx context.Context, c pb.BrokerClient, journal pb.Journal) *Reader {
	return &Reader{}
}

// Reader wraps a BrokerClient and journal to provide callers with a long-lived
// journal Reader which transparently handles and retries errors, and will block
// as needed to await new journal content.
type Reader struct {
	Client pb.BrokerClient
	// Context of the Reader. Notably, cancelling this context will also cancel
	// a blocking Read RPC in progress.
	Context context.Context
	// Journal which is read.
	Journal pb.Journal
	// Offset of the next journal byte to be returned by Read.
	Offset int64
	// Whether the next dispatched Read RPC should block (the default).
	// If Block is false, than Read calls may return ErrNotYetAvailable.
	Block bool
	// Whether the broker should not proxy reads to other brokers or remote
	// Fragment stores. If true, the broker returns metadata for Reader to
	// retry the request directly. Clients should generally set this.
	DoNotProxy bool
	// UpdateRouteCache is an optional closure which informs the Route cache
	// of an updated Route entry for a Journal, or if Route is nil purges
	// any existing entry.
	UpdateRouteCache func(pb.Journal, *pb.Route)
	// Last ReadResponse read from the broker.
	Response pb.ReadResponse

	stream pb.Broker_ReadClient
	frag   io.ReadCloser
}

// AdjustedOffset returns the current journal offset, adjusted for content read
// by |br| (which wraps this Reader) but not yet consumed from |br|'s buffer.
func (r *Reader) AdjustedOffset(br *bufio.Reader) int64 { return r.Offset - int64(br.Buffered()) }

// Read returns the next bytes of journal content. If |Blocking| and no content
// is available, Read will block indefinitely until a journal Append occurs.
// Read will return a non-nil error in the following cases:
//  * If the Reader context is cancelled.
//  * If Blocking is false, and Status_OFFSET_NOT_YET_AVAILABLE is returned by the broker.
// All other errors are retried.
func (r *Reader) Read(p []byte) (n int, err error) {
	for i := 0; true; i++ {

		// Handle an error from a previous iteration. Since we're a retrying reader,
		// we consume and mask errors (possibly logging a warning), and manage
		// our own back-off timer.
		if err != nil {

			// Context cancellations are usually wrapped by augmenting errors as they
			// return up the call stack. If the context is Done, assume that is the
			// primary error.
			if r.Context.Err() != nil {
				err = r.Context.Err()
			}

			switch err {
			case io.EOF, context.DeadlineExceeded, context.Canceled:
				// Suppress logging for expected errors.
			default:
				if r.UpdateRouteCache != nil {
					r.UpdateRouteCache(r.Journal, nil) // Purge any current entry.
				}
				log.WithFields(log.Fields{"journal": r.Journal, "offset": r.Offset, "err": err}).
					Warn("read failure (will retry)")
			}

			// Wait for a back-off timer, or context cancellation.
			select {
			case <-r.Context.Done():
				return 0, r.Context.Err()
			case <-time.After(backoff(i)):
			}

			err = nil // Clear.
		}

		// Is there remaining Content in the last ReadResponse?
		if len(r.Response.Content) != 0 {
			n = copy(p, r.Response.Content)
			r.Response.Content = r.Response.Content[n:]
			r.Offset += int64(n)
			return
		}

		// Do we have an open, direct Fragment reader?
		if r.frag != nil {
			if n, err = r.frag.Read(p); err == nil {
				r.Offset += int64(n)
				return
			} else {
				_, r.frag = r.frag.Close(), nil
				continue
			}
		}

		// Iff we have an open server stream, read the next ReadResponse.
		if r.stream != nil {
			if err = r.stream.RecvMsg(&r.Response); err != nil {
				r.stream = nil
				continue
			}

			switch r.Response.Status {
			case pb.Status_OK, pb.Status_NOT_JOURNAL_BROKER:
				// Expected statuses.
			case pb.Status_OFFSET_NOT_YET_AVAILABLE:
				// If this Reader is in non-blocking mode, and the response indicates
				// the requested read operation would have to block, return
				// ErrNotYetAvailable to the client. Otherwise, we may have enabled Block
				// since the last RPC, in which case we should simply retry.
				if !r.Block {
					err = ErrNotYetAvailable
					return
				}
			default:
				err = errors.New(r.Response.Status.String())
			}

			if r.Response.Header != nil && r.UpdateRouteCache != nil {
				r.UpdateRouteCache(r.Journal, &r.Response.Header.Route)
			}

			if r.Response.Offset > r.Offset {
				// Offset jumps are uncommon, but still possible if fragments are removed from a journal.
				log.WithFields(log.Fields{"journal": r.Journal, "from": r.Offset, "to": r.Response.Offset}).
					Warn("offset jump")
				r.Offset = r.Response.Offset

				return // Return an n=0 read to allow the client to observe the updated Offset.
			}
			continue
		}

		// Do we have a Fragment location covering |Offset| which we can directly open?
		if r.Response.Fragment != nil &&
			r.Response.FragmentUrl != "" &&
			r.Response.Fragment.Begin <= r.Offset && r.Response.Fragment.End > r.Offset {

			r.frag, err = OpenFragmentURL(r.Offset, *r.Response.Fragment, r.Response.FragmentUrl)
			continue
		}

		// Begin a new Read RPC.
		r.stream, err = r.Client.Read(r.Context, &pb.ReadRequest{
			Journal:      r.Journal,
			Offset:       r.Offset,
			Block:        r.Block,
			DoNotProxy:   r.DoNotProxy,
			MetadataOnly: false,
		})
	}

	panic("not reached")
}

var (
	ErrNotYetAvailable = errors.New(pb.Status_OFFSET_NOT_YET_AVAILABLE.String())
)

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0, 1:
		return 0
	case 2:
		return time.Millisecond * 5
	case 3, 4, 5:
		return time.Second * time.Duration(attempt-1)
	default:
		return 5 * time.Second
	}
}
