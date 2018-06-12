package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"time"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Reader wraps a BrokerClient and journal to provide callers with a long-lived
// journal Reader which transparently handles and retries errors, and will block
// as needed to await new journal content.
type Reader struct {
	// Context of the Reader. Notably, cancelling this context will also cancel
	// a blocking Read RPC in progress.
	Context context.Context
	// Client used to dispatch Read RPCs.
	Client pb.BrokerClient
	// Journal which is to be read.
	Journal pb.Journal
	// Offset of the next journal byte to be returned by Read.
	Offset int64
	// Whether the next dispatched Read RPC should block (the default).
	// If Block is false, than Read calls may return ErrNotYetAvailable.
	Block bool

	// Last ReadResponse read from the broker.
	Response pb.ReadResponse

	stream       pb.Broker_ReadClient
	streamCancel context.CancelFunc
	frag         io.ReadCloser
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
				if u, ok := r.Client.(RouteUpdater); ok {
					u.UpdateRoute(r.Journal, nil) // Purge any current entry.
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
				r.stream, r.streamCancel = nil, nil
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

			if r.Response.Header != nil {
				if u, ok := r.Client.(RouteUpdater); ok {
					u.UpdateRoute(r.Journal, &r.Response.Header.Route)
				}
			}

			if r.Response.Offset > r.Offset {
				// Offset jumps are uncommon, but still possible if fragments are removed from a journal.
				log.WithFields(log.Fields{"journal": r.Journal, "from": r.Offset, "to": r.Response.Offset}).
					Warn("offset jump")
				r.Offset = r.Response.Offset
			}
			continue
		}

		// Do we have a Fragment location covering |Offset| which we can directly open?
		if r.Response.Fragment != nil &&
			r.Response.FragmentUrl != "" &&
			r.Response.Fragment.Begin <= r.Offset && r.Response.Fragment.End > r.Offset {

			r.frag, err = OpenFragmentURL(r.Context, r.Offset, *r.Response.Fragment, r.Response.FragmentUrl)
			continue
		}

		// If our BrokerClient is capable of directly routing to responsible brokers,
		// assume we don't want brokers to proxy to peers or remotely Fragments on
		// our behalf.
		var _, doNotProxy = r.Client.(RouteUpdater)

		// Begin a new Read RPC, using a cancel-able child context.
		var ctx, cancel = context.WithCancel(r.Context)

		r.stream, err = r.Client.Read(ctx, &pb.ReadRequest{
			Journal:      r.Journal,
			Offset:       r.Offset,
			Block:        r.Block,
			DoNotProxy:   doNotProxy,
			MetadataOnly: false,
		})
		r.streamCancel = cancel
	}

	panic("not reached")
}

// Reset closes and releases underlying readers opened by this Reader,
// returning the Reader to an initialized state. It must not be called
// concurrently with Read.
func (r *Reader) Reset() (err error) {
	if r.frag != nil {
		err = r.frag.Close()
		r.frag = nil
	}

	if r.stream != nil {
		r.streamCancel()

		// Read & discard pending messages, until our cancellation is handled.
		for _, err = r.stream.Recv(); err == nil; _, err = r.stream.Recv() {
		}
		if err == context.Canceled {
			err = nil
		}
		r.stream, r.streamCancel = nil, nil
	}
	return err
}

// Seek sets the offset for the next Read. It returns an error if (and only if)
// |whence| is io.SeekEnd, which is not supported.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = r.Offset + offset
	default:
		return r.Offset, errors.New("io.SeekEnd whence is not supported")
	}

	var delta = offset - r.Offset
	r.Offset = offset

	// Can the seek be satisfied by an open fragment reader?
	if r.frag != nil && offset < r.Response.Fragment.End {
		if _, err := io.CopyN(ioutil.Discard, r.frag, delta); err == nil {
			return offset, nil
		} else {
			log.WithFields(log.Fields{"delta": delta, "err": err}).
				Warn("failed to seek open fragment (will retry)")
		}
	}

	if err := r.Reset(); err != nil {
		log.WithFields(log.Fields{"err": err}).Warn("during Reader Reset (will retry)")
	}
	return offset, nil
}

// AdjustedSeek sets the offset for the next Read, accounting for buffered data and updating
// the buffer as needed.
func (r *Reader) AdjustedSeek(offset int64, whence int, br *bufio.Reader) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = r.AdjustedOffset(br) + offset
	default:
		return r.AdjustedOffset(br), errors.New("io.SeekEnd whence is not supported")
	}

	var delta = offset - r.AdjustedOffset(br)

	// Fast path: can we fulfill the seek by discarding a portion of buffered data?
	if delta >= 0 && delta <= int64(br.Buffered()) {
		br.Discard(int(delta))
		return offset, nil
	}

	// We must Seek the underlying reader, discarding and resetting the current buffer.
	var n, err = r.Seek(offset, io.SeekStart)
	br.Reset(r)
	return n, err
}

var ErrNotYetAvailable = errors.New(pb.Status_OFFSET_NOT_YET_AVAILABLE.String())

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
