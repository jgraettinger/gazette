package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"time"

	"github.com/LiveRamp/gazette/pkg/codecs"
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
	// Last Header received from a serving broker.
	Header *pb.Header
	// Last Fragment received.
	Fragment *pb.Fragment
	// Direct URL at which the last Fragment received may be read,
	// or "" if the Fragment is not directly read-able.
	FragmentURL string

	stream pb.Broker_ReadClient
	resp pb.ReadResponse

	frag io.ReadCloser
	fragCodec codecs.Decompressor
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
			switch err {
			case io.EOF, context.DeadlineExceeded, context.Canceled:
				// Suppress logging for expected errors.
			case ErrNotYetAvailable:
				if !r.Block {
					return
				} else {
					// This RetryReader was in non-blocking mode but has since switched
					// to blocking. Ignore this error and retry as a blocking operation.
				}
			default:
				if r.Context.Err() == nil {
					log.WithFields(log.Fields{"journal": r.Journal, "offset": r.Offset, "err": err}).
						Warn("Read failure (will retry)")
				}
			}

			// Wait for a back-off timer, or context cancellation.
			select {
			case <-r.Context.Done():
				return 0, r.Context.Err()
			case <-time.After(backoff(i)):
			}

			err = nil // Clear.
		}

		if r.stream == nil {

			// Check if we can directly open the fragment.
			if r.FragmentURL != "" && r.Fragment.GetEnd() > r.Offset && r.Fragment.GetBegin() <= r.Offset {

			}




			r.stream, err = r.Client.Read(r.Context, &pb.ReadRequest{
				Journal:      r.Journal,
				Offset:       r.Offset,
				Block:        r.Block,
				DoNotProxy:   false,
				MetadataOnly: false,
			})
			continue
		}

		err = r.stream.RecvMsg(&r.resp)

		if err == nil && r.resp.Status == pb.Status_OFFSET_NOT_YET_AVAILABLE {
			err = ErrNotYetAvailable
		} else if r.resp.Status != pb.Status_OK {
			err = errors.New(r.resp.Status.String())
		}

		if err != nil {
			continue
		}

		if r.resp.Header != nil {
			r.Header = r.resp.Header
		}
		if r.resp.Fragment != nil {
			r.Fragment = r.resp.Fragment
		}



			rr.LastResult, rr.MarkedReader.ReadCloser = rr.Getter.Get(args)
			if n, err = 0, rr.LastResult.Error; err != nil {
				rr.MarkedReader.ReadCloser = nil
				continue
			}

			if o := rr.MarkedReader.Mark.Offset; o != 0 && o != -1 && o != rr.LastResult.Offset {
				// Offset jumps should be uncommon, but are possible if data has
				// been removed from the middle of a journal.
				log.WithFields(log.Fields{"mark": rr.MarkedReader.Mark, "result": rr.LastResult}).
					Warn("offset jump")
			}
			rr.MarkedReader.Mark.Offset = rr.LastResult.Offset
		}

		if n, err = rr.MarkedReader.Read(p); err == nil {
			return
		} else {
			// Close to nil rr.MarkedReader.ReadCloser.
			rr.MarkedReader.Close()

			if n != 0 {
				// If data was returned with the error, squash |err|.
				if err != io.EOF {
					log.WithFields(log.Fields{"err": err, "n": n}).Warn("data read error (will retry)")
				}
				err = nil
				return
			}
		}
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
