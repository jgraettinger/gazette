package client

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/LiveRamp/gazette/pkg/codecs"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

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

	resp, err := http.DefaultClient.Do(req)
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

	/*
		var deltaF64 = float64(delta)
		metrics.GazetteReadBytesTotal.Add(deltaF64)
		metrics.GazetteDiscardBytesTotal.Add(deltaF64)
		return response.Body, nil // Success.
	*/

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
