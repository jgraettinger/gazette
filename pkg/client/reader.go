package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/LiveRamp/gazette/pkg/codecs"
	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type Reader struct {
	Request  pb.ReadRequest     // Initial ReadRequest.
	Response pb.ReadResponse    // Most recent ReadResponse from broker.
	Offset   int64              // Next Journal byte Offset to be returned by Read.
	Cancel   context.CancelFunc // Cancel the Reader.

	client pb.BrokerClient      // Client against which Read is dispatched.
	subCtx context.Context      // Child Context owned by Reader and canceled by |Cancel|.
	stream pb.Broker_ReadClient // Server stream.
	direct io.ReadCloser        // Directly opened Fragment URL.
}

func NewReader(ctx context.Context, client pb.BrokerClient, req pb.ReadRequest) *Reader {
	// Create a cancel-able child context to allow for canceling reads.
	var subCtx, cancel = context.WithCancel(ctx)

	var r = &Reader{
		Request: req,
		Offset:  req.Offset,
		Cancel:  cancel,
		client:  client,
		subCtx:  subCtx,
	}
	return r
}

func (r *Reader) Read(p []byte) (n int, err error) {
	// Lazy initialization: begin the Read RPC.
	if r.stream == nil {
		if r.stream, err = r.client.Read(r.subCtx, &r.Request); err == nil {
			n, err = r.Read(p) // Recurse to attempt read against opened |r.stream|.
		}
		return // Surface read or error.
	}

	// Read from an open, direct fragment reader.
	if r.direct != nil {
		if n, err = r.direct.Read(p); err != nil {
			_ = r.direct.Close()
		}
		r.Offset += int64(n)
		return
	}

	// Is there remaining content in the last ReadResponse?
	if d := int(r.Offset - r.Response.Offset); d < len(r.Response.Content) {
		n = copy(p, r.Response.Content[d:])
		r.Offset += int64(n)
		return
	}

	// Read and Validate the next frame.
	if err = r.stream.RecvMsg(&r.Response); err == nil {
		err = r.Response.Validate()
	}

	if err == io.EOF {
		// If the broker closed the stream but previously provided a direct fragment URL, attempt to open that URL directly.
		if r.Response.FragmentUrl != "" {
			if r.direct, err = OpenFragmentURL(r.subCtx, r.Offset, *r.Response.Fragment, r.Response.FragmentUrl); err == nil {
				n, err = r.Read(p) // Recurse to attempt read against opened |r.direct|.
			}
			return // Surface read or error.
		}
	} else if err != nil {
		// On a non-EOF error, purge any existing Route advisement for the journal.
		if u, ok := r.client.(RouteUpdater); ok {
			u.UpdateRoute(r.Request.Journal, nil)
		}
	} else /* err == nil */ {

		// If a Header was sent, advise on its advertised journal Route.
		if u, ok := r.client.(RouteUpdater); r.Response.Header != nil && ok {
			u.UpdateRoute(r.Request.Journal, &r.Response.Header.Route)
		}

		// Map a non-OK status into an |err|.
		switch r.Response.Status {
		case pb.Status_OK:
			// Pass.
		case pb.Status_NOT_JOURNAL_BROKER:
			err = ErrNotJournalBroker
		case pb.Status_OFFSET_NOT_YET_AVAILABLE:
			err = ErrOffsetNotYetAvailable
		default:
			err = errors.New(r.Response.Status.String())
		}
	}

	if err != nil {
		r.Cancel() // Abort the gRPC stream (if it's still alive).
		return
	}

	if r.Offset < r.Response.Offset {
		// Offset jumps are uncommon but possible if fragments are removed from a journal.
		log.WithFields(log.Fields{"journal": r.Request.Journal, "from": r.Offset, "to": r.Response.Offset}).
			Warn("offset jump")
		r.Offset = r.Response.Offset
	} else if r.Offset > r.Response.Offset {
		// This is a violation of the Read API contract.
		panic(fmt.Sprintf("invalid response offset (%d; expected >= %d)", r.Response.Offset, r.Offset))
	}

	return r.Read(p) // Recurse to attempt read against updated |r.Response|.
}

// AdjustedOffset returns the current journal offset, adjusted for content read
// by |br| (which wraps this Reader) but not yet consumed from |br|'s buffer.
func (r *Reader) AdjustedOffset(br *bufio.Reader) int64 { return r.Offset - int64(br.Buffered()) }

// Seek provides a limited form of seeking support. Specifically, iff a
// Fragment URL is being directly read, the Seek offset is ahead of the current
// Reader offset, and the Fragment also covers the desired Seek offset, then
// the interleaving portion of the Fragment will be read and discarded. In all
// other cases, Seek returns ErrSeekRequiresNewReader.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = r.Offset + offset
	default:
		return r.Offset, errors.New("io.SeekEnd whence is not supported")
	}

	if r.direct == nil || offset < r.Offset || offset >= r.Response.Fragment.End {
		return r.Offset, ErrSeekRequiresNewReader
	}

	var _, err = io.CopyN(ioutil.Discard, r, offset-r.Offset)
	return r.Offset, err
}

// OpenFragmentURL directly opens |fragment|, which must be available at URL
// |url|, and returns a ReadCloser which has been pre-seeked to |offset|.
func OpenFragmentURL(ctx context.Context, offset int64, fragment pb.Fragment, url string) (io.ReadCloser, error) {
	var req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	if fragment.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		// Require that the server send us un-encoded content, offloading
		// decompression onto the storage API. Go's standard `gzip` package is slow,
		// and we also see a parallelism benefit by offloading decompression work
		// onto cloud storage system.
		req.Header.Set("Accept-Encoding", "identity")
	} else if fragment.CompressionCodec == pb.CompressionCodec_GZIP {
		// Explicitly request gzip. Doing so disables the http client's transparent
		// handling for gzip decompression if the Fragment happened to be written with
		// "Content-Encoding: gzip", and it instead directly surfaces the compressed
		// bytes to us.
		req.Header.Set("Accept-Encoding", "gzip")
	}

	resp, err := HTTPClient.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("!OK fetching (%s, %q)", resp.Status, url)
	}

	decomp, err := codecs.NewCodecReader(resp.Body, fragment.CompressionCodec)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("building decompressor (%s, %q)", err, url)
	}

	var rc = &doubleCloser{
		Reader: decomp,
		a:      decomp,
		b:      resp.Body,
	}

	// Attempt to seek to |offset| within the fragment.
	var delta = offset - fragment.Begin
	if _, err := io.CopyN(ioutil.Discard, rc, delta); err != nil {
		rc.Close()
		return nil, fmt.Errorf("error seeking fragment (%s, %q)", err, url)
	}

	return rc, nil
}

type doubleCloser struct {
	io.Reader
	a, b io.Closer
}

func (dc *doubleCloser) Close() error {
	var errA, errB = dc.a.Close(), dc.b.Close()
	if errA != nil {
		return errA
	}
	return errB
}

var (
	// Map common broker error status into named errors.
	ErrOffsetNotYetAvailable = errors.New(pb.Status_OFFSET_NOT_YET_AVAILABLE.String())
	ErrNotJournalBroker      = errors.New(pb.Status_NOT_JOURNAL_BROKER.String())
	ErrSeekRequiresNewReader = errors.New("seek offset requires new Reader")

	// HTTPClient is used by OpenFragmentURL
	HTTPClient = http.DefaultClient
)
