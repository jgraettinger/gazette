package broker

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// read evaluates a client's Read RPC.
func read(r *replica, req *pb.ReadRequest, srv pb.Broker_ReadServer) error {
	var buffer = chunkBufferPool.Get().([]byte)
	defer chunkBufferPool.Put(buffer)

	var reader io.ReadCloser

	for i := 0; true; i++ {
		var resp, file, err = r.index.query(srv.Context(), req)
		if err != nil {
			return err
		}

		// Send the Route with the first response message (only). Read requests may
		// be long-lived: so long, in fact, that the Route could change multiple
		// times over the course of evaluating it.
		if i == 0 {
			resp.Route = &r.route
		}

		if err = srv.Send(resp); err != nil {
			return err
		}

		// Return after sending Metadata if the Fragment query failed,
		// or we were only asked to send metadata, or the Fragment is
		// remote and we're not instructed to proxy.
		if resp.Status != pb.Status_OK || req.MetadataOnly || file == nil && req.DoNotProxy {
			return nil
		}

		// Don't send metadata other than Offset, with chunks 2..N.
		*resp = pb.ReadResponse{
			Status: pb.Status_OK,
			Offset: resp.Offset,
		}

		if file != nil {
			reader = ioutil.NopCloser(io.NewSectionReader(
				file, resp.Offset-resp.Fragment.Begin, resp.Fragment.End-resp.Offset))
		} else if reader, err = fragment.Store.Open(*resp.Fragment, resp.Offset); err != nil {
			return err
		}

		// Loop over chunks read from |reader|, sending each to the client.
		var n int
		var readErr error

		for readErr == nil {
			if n, readErr = reader.Read(buffer); n == 0 {
				continue
			}
			resp.Content = buffer[:n]

			if err = srv.Send(resp); err != nil {
				return err
			}
			resp.Offset += int64(n)
		}

		if readErr != io.EOF {
			return readErr
		} else if err = reader.Close(); err != nil {
			return err
		}

		// Loop to query and read the next Fragment.
		req.Offset = resp.Offset
	}
	return nil
}

var (
	chunkSize       = 1 << 17 // 128K.
	chunkBufferPool = sync.Pool{New: func() interface{} { return make([]byte, chunkSize) }}
)
