package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/codecs"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Reader adapts a Read RPC to the io.Reader interface. It additionally supports
// directly reading Fragment URLs advertised but not proxied by the broker (eg,
// because DoNotProxy of the ReadRequest is true).
type Reader struct {
	Request  pb.ReadRequest  // ReadRequest of the Reader.
	Response pb.ReadResponse // Most recent ReadResponse from broker.

	ctx    context.Context
	client pb.BrokerClient      // Client against which Read is dispatched.
	stream pb.Broker_ReadClient // Server stream.
	direct io.ReadCloser        // Directly opened Fragment URL.
}

func NewReader(ctx context.Context, client pb.BrokerClient, req pb.ReadRequest) *Reader {
	var r = &Reader{
		Request: req,
		ctx:     ctx,
		client:  client,
	}
	return r
}

func (r *Reader) Read(p []byte) (n int, err error) {
	// Lazy initialization: begin the Read RPC.
	if r.stream == nil {
		if r.stream, err = r.client.Read(r.ctx, &r.Request); err == nil {
			n, err = r.Read(p) // Recurse to attempt read against opened |r.stream|.
		}
		return // Surface read or error.
	}

	// Read from an open, direct fragment reader.
	if r.direct != nil {
		if n, err = r.direct.Read(p); err != nil {
			_ = r.direct.Close()
		}
		r.Request.Offset += int64(n)
		return
	}

	// Is there remaining content in the last ReadResponse?
	if d := int(r.Request.Offset - r.Response.Offset); d < len(r.Response.Content) {
		n = copy(p, r.Response.Content[d:])
		r.Request.Offset += int64(n)
		return
	}

	// Read and Validate the next frame.
	if err = r.stream.RecvMsg(&r.Response); err == nil {
		if err = r.Response.Validate(); err != nil {
			err = pb.ExtendContext(err, "ReadResponse")
		} else if r.Response.Status == pb.Status_OK && r.Response.Offset < r.Request.Offset {
			err = pb.NewValidationError("invalid ReadResponse offset (%d; expected >= %d)",
				r.Response.Offset, r.Request.Offset) // Violation of Read API contract.
		}
	}

	// A note on resource leaks: an invariant of Read is that in invocations where
	// the returned error != nil, an error has also been read from |r.stream|,
	// implying that the gRPC transport has been torn down. The exception is if
	// response validation fails, which indicates a client / server API version
	// incompatibility and cannot happen in normal operation.

	if err == nil {

		if r.Request.Offset < r.Response.Offset {
			// Offset jumps are uncommon, but possible if fragments were removed.
			log.WithFields(log.Fields{
				"journal": r.Request.Journal,
				"from":    r.Request.Offset,
				"to":      r.Response.Offset,
			}).Warn("offset jump")
			r.Request.Offset = r.Response.Offset
		}

		// If a Header was sent, advise of its advertised journal Route.
		if u, ok := r.client.(RouteUpdater); r.Response.Header != nil && ok {
			u.UpdateRoute(r.Request.Journal, &r.Response.Header.Route)
		}

		n, err = r.Read(p) // Recurse to attempt read against updated |r.Response|.
		return

	} else if err != io.EOF {
		// We read an error _other_ than a graceful tear-down of the stream.
		// Purge any existing Route advisement for the journal.
		if u, ok := r.client.(RouteUpdater); ok {
			u.UpdateRoute(r.Request.Journal, nil)
		}
		return
	}

	// We read a graceful stream closure (err == io.EOF).

	// If the frame preceding EOF provided a fragment URL, open it directly.
	if r.Response.Status == pb.Status_OK && r.Response.FragmentUrl != "" {
		if r.direct, err = OpenFragmentURL(r.ctx, r.Request.Offset,
			*r.Response.Fragment, r.Response.FragmentUrl); err == nil {
			n, err = r.Read(p) // Recurse to attempt read against opened |r.direct|.
		}
		return
	}

	// Otherwise, map Status of the preceding frame into a more specific error.
	switch r.Response.Status {
	case pb.Status_OK:
		// Return err = io.EOF.
	case pb.Status_NOT_JOURNAL_BROKER:
		err = ErrNotJournalBroker
	case pb.Status_OFFSET_NOT_YET_AVAILABLE:
		err = ErrOffsetNotYetAvailable
	default:
		err = errors.New(r.Response.Status.String())
	}
	return
}

// AdjustedOffset returns the current journal offset, adjusted for content read
// by |br| (which wraps this Reader) but not yet consumed from |br|'s buffer.
func (r *Reader) AdjustedOffset(br *bufio.Reader) int64 {
	return r.Request.Offset - int64(br.Buffered())
}

// Seek provides a limited form of seeking support. Specifically, iff a
// Fragment URL is being directly read, the Seek offset is ahead of the current
// Reader offset, and the Fragment also covers the desired Seek offset, then a
// seek is performed by reading and discarding to the seeked offset. Seek will
// otherwise return ErrSeekRequiresNewReader.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = r.Request.Offset + offset
	default:
		return r.Request.Offset, errors.New("io.SeekEnd whence is not supported")
	}

	if r.direct == nil || offset < r.Request.Offset || offset >= r.Response.Fragment.End {
		return r.Request.Offset, ErrSeekRequiresNewReader
	}

	var _, err = io.CopyN(ioutil.Discard, r, offset-r.Request.Offset)
	return r.Request.Offset, err
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

	var fr = &fragReader{
		decomp:   decomp,
		raw:      resp.Body,
		fragment: fragment,
	}

	// Attempt to seek to |offset| within the fragment.
	var delta = offset - fragment.Begin
	if _, err := io.CopyN(ioutil.Discard, fr, delta); err != nil {
		fr.Close()
		return nil, fmt.Errorf("error seeking fragment (%s, %q)", err, url)
	}
	return fr, nil
}

type fragReader struct {
	decomp   io.ReadCloser
	raw      io.ReadCloser
	fragment pb.Fragment
}

func (fr *fragReader) Read(p []byte) (n int, err error) {
	n, err = fr.decomp.Read(p)
	fr.fragment.Begin += int64(n)

	if fr.fragment.Begin > fr.fragment.End {
		// Did we somehow read beyond Fragment.End?
		n -= int(fr.fragment.Begin - fr.fragment.End)
		err = ErrExpectedEOF
	} else if err == io.EOF && fr.fragment.Begin != fr.fragment.End {
		// Did we read EOF before the reaching Fragment.End?
		err = io.ErrUnexpectedEOF
	}
	return
}

func (fr *fragReader) Close() error {
	var errA, errB = fr.decomp.Close(), fr.raw.Close()
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
	ErrExpectedEOF           = errors.New("did not read EOF at expected Fragment.End")

	// HTTPClient is the http.Client used by OpenFragmentURL
	HTTPClient = http.DefaultClient
)
