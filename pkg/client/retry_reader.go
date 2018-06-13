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

// Reader wraps a BrokerClient and journal to provide callers with a long-lived
// journal Reader which transparently handles and retries errors, and will block
// as needed to await new journal content.
type RetryReader struct {
	// Reader is the current underlying Reader of the RetryReader. This instance
	// may change many times over the lifetime of a RetryReader, as Read RPCs
	// are restarted.
	Reader *Reader

	ctx    context.Context
	client pb.BrokerClient
}

func NewRetryReader(ctx context.Context, client pb.BrokerClient, req pb.ReadRequest) *RetryReader {
	// If our BrokerClient is capable of directly routing to responsible brokers,
	// we don't want brokers to proxy to peers or remote Fragments on our behalf.
	if _, ok := client.(RouteUpdater); ok {
		req.DoNotProxy = true
	}

	return &RetryReader{
		Reader: NewReader(ctx, client, req),
		ctx:    ctx,
		client: client,
	}
}

// Journal being read by this RetryReader.
func (rr *RetryReader) Journal() pb.Journal {
	return rr.Reader.Request.Journal
}

// Offset of the next Journal byte to be returned by Read.
func (rr *RetryReader) Offset() int64 {
	return rr.Reader.Offset
}

// Read returns the next bytes of journal content. It will return a non-nil
// error in the following cases:
//  * If the RetryReader context is cancelled.
//  * The broker returns OFFSET_NOT_YET_AVAILABLE (ErrOffsetNotYetAvailable)
//    for a non-blocking ReadRequest.
// All other errors are retried.
func (rr *RetryReader) Read(p []byte) (n int, err error) {
	for i := 0; true; i++ {

		if n, err = rr.Reader.Read(p); err == nil {
			return // Success.
		}

		// Our Read failed. Since we're a retrying reader, we consume and mask
		// errors (possibly logging a warning), manage our own back-off timer,
		// and restart the stream when ready for another attempt.

		// Context cancellations are usually wrapped by augmenting errors as they
		// return up the call stack. If our Reader's sub-context is Done, assume
		// that is the primary error.
		if rr.Reader.subCtx.Err() != nil {
			err = rr.Reader.subCtx.Err()
		}

		switch err {
		case io.EOF, context.DeadlineExceeded, context.Canceled:
			// Suppress logging for expected errors.
		case ErrOffsetNotYetAvailable:
			return
		default:
			log.WithFields(log.Fields{"journal": rr.Journal(), "offset": rr.Offset(), "err": err}).
				Warn("read failure (will retry)")
		}

		// Wait for a back-off timer, or context cancellation.
		select {
		case <-rr.ctx.Done():
			return 0, rr.ctx.Err()
		case <-time.After(backoff(i)):
		}

		rr.restartReaderAt(rr.Offset())
	}
	panic("not reached")
}

// Seek sets the offset for the next Read. It returns an error if (and only if)
// |whence| is io.SeekEnd, which is not supported.
func (rr *RetryReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = rr.Offset() + offset
	default:
		return rr.Offset(), errors.New("io.SeekEnd whence is not supported")
	}

	if _, err := rr.Reader.Seek(offset, io.SeekStart); err != nil {
		if err != ErrSeekRequiresNewReader {
			log.WithFields(log.Fields{"journal": rr.Journal(), "offset": offset, "err": err}).
				Warn("failed to seek open Reader (will retry)")
		}

		rr.Reader.Cancel()
		rr.restartReaderAt(offset)
	}
	return rr.Offset(), nil
}

// AdjustedOffset returns the current journal offset, adjusted for content read
// by |br| (which wraps this RetryReader) but not yet consumed from |br|'s buffer.
func (rr *RetryReader) AdjustedOffset(br *bufio.Reader) int64 { return rr.Reader.AdjustedOffset(br) }

// AdjustedSeek sets the offset for the next Read, accounting for buffered data and updating
// the buffer as needed.
func (rr *RetryReader) AdjustedSeek(offset int64, whence int, br *bufio.Reader) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = rr.AdjustedOffset(br) + offset
	default:
		return rr.AdjustedOffset(br), errors.New("io.SeekEnd whence is not supported")
	}

	var delta = offset - rr.AdjustedOffset(br)

	// Fast path: can we fulfill the seek by discarding a portion of buffered data?
	if delta >= 0 && delta <= int64(br.Buffered()) {
		br.Discard(int(delta))
		return offset, nil
	}

	// We must Seek the underlying reader, discarding and resetting the current buffer.
	var n, err = rr.Seek(offset, io.SeekStart)
	br.Reset(rr)
	return n, err
}

func (rr *RetryReader) restartReaderAt(offset int64) {
	var req = rr.Reader.Request
	req.Offset = offset

	rr.Reader = NewReader(rr.ctx, rr.client, req)
}

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
