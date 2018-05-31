package broker

import (
	"io"
	"io/ioutil"
	"sync"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Read dispatches the BrokerServer.Read API.
func (s *Service) Read(req *pb.ReadRequest, stream pb.Broker_ReadServer) error {
	if err := req.Validate(); err != nil {
		return err
	} else if err = s.resolver.waitForRevision(stream.Context(), 1); err != nil {
		return err
	}

	var res, status = s.resolver.resolve(req.Journal, false, !req.DoNotProxy)
	if status != pb.Status_OK {
		return stream.Send(&pb.ReadResponse{Status: status, Route: res.route})
	} else if res.replica == nil {
		return proxyRead(req, res.broker, stream, s.dialer)
	}

	if err := res.replica.serveRead(req, stream); err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Read")
		return err
	}
	return nil
}

// proxyRead forwards a ReadRequest to a resolved peer broker.
func proxyRead(req *pb.ReadRequest, to pb.BrokerSpec_ID, stream pb.Broker_ReadServer, dialer dialer) error {
	var conn, err = dialer.dial(stream.Context(), to)
	if err != nil {
		return err
	}
	client, err := pb.NewBrokerClient(conn).Read(stream.Context(), req)
	if err != nil {
		return err
	} else if err = client.CloseSend(); err != nil {
		return err
	}

	var resp = new(pb.ReadResponse)

	for {
		if err = client.RecvMsg(resp); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err = stream.Send(resp); err != nil {
			return err
		}
	}
}

// read evaluates a client's Read RPC.
func (r *replicaImpl) serveRead(req *pb.ReadRequest, srv pb.Broker_ReadServer) error {
	var buffer = chunkBufferPool.Get().([]byte)
	defer chunkBufferPool.Put(buffer)

	var reader io.ReadCloser

	for i := 0; true; i++ {
		var resp, file, err = r.index.Query(srv.Context(), req)
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

		if file != nil {
			reader = ioutil.NopCloser(io.NewSectionReader(
				file, resp.Offset-resp.Fragment.Begin, resp.Fragment.End-resp.Offset))
		} else {
			// if reader, err = fragment.Store.Open(*resp.Fragment, resp.Offset); err != nil {
			//return err
			// }
			panic("not yet implemented")
		}

		// Don't send metadata other than Offset in chunks 2..N of the Fragment.
		*resp = pb.ReadResponse{
			Status: pb.Status_OK,
			Offset: resp.Offset,
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
