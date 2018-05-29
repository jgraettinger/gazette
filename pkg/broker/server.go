package broker

import (
	"io"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Server implements pb.BrokerServer
type Server struct {
	resolver resolver
	dialer   dialer
}

// Read dispatches the BrokerServer.Read API.
func (srv *Server) Read(req *pb.ReadRequest, stream pb.Broker_ReadServer) error {
	if err := req.Validate(); err != nil {
		return err
	}

	var res, status = srv.resolver.resolve(req.Journal, false, !req.DoNotProxy)
	if status != pb.Status_OK {
		return stream.Send(&pb.ReadResponse{Status: status, Route: res.route})
	} else if res.replica == nil {
		return proxyRead(req, res.broker, stream, srv.dialer)
	}

	if err := res.replica.serveRead(req, stream); err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Read")
		return err
	}
	return nil
}

// Append dispatches the BrokerServer.Append API.
func (srv *Server) Append(stream pb.Broker_AppendServer) error {
	var req, err = stream.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	for done := false; !done && err == nil; {

		var res, status = srv.resolver.resolve(req.Journal, true, true)
		if status != pb.Status_OK {
			return stream.SendAndClose(&pb.AppendResponse{Status: status, Route: res.route})
		} else if res.replica == nil {
			return proxyAppend(req, res.broker, stream, srv.dialer)
		}

		var rev, err = res.replica.serveAppend(req, stream, srv.dialer)

		if err == nil {
			if rev == 0 {
				done = true
			} else {
				// A peer told us of a future & non-equivalent Route revision.
				// Wait for that revision and attempt the RPC again.
				err = srv.resolver.waitForRevision(stream.Context(), rev)
			}
		}
	}

	if err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Append")
		return err
	}
	return nil
}

// Replicate dispatches the BrokerServer.Replicate API.
func (srv *Server) Replicate(stream pb.Broker_ReplicateServer) error {
	var req, err = stream.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	} else if err = srv.resolver.waitForRevision(stream.Context(), req.Route.Revision); err != nil {
		return err
	}

	var res, status = srv.resolver.resolve(req.Journal, false, false)
	if status != pb.Status_OK {
		return stream.Send(&pb.ReplicateResponse{Status: status, Route: res.route})
	}

	if !res.route.Equivalent(req.Route) {
		return stream.Send(&pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE, Route: res.route})
	}

	if err := res.replica.serveReplicate(req, stream); err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Replicate")
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

// proxyAppend forwards an AppendRequest to a resolved peer broker.
func proxyAppend(req *pb.AppendRequest, to pb.BrokerSpec_ID, stream pb.Broker_AppendServer, dialer dialer) error {
	var conn, err = dialer.dial(stream.Context(), to)
	if err != nil {
		return err
	}
	client, err := pb.NewBrokerClient(conn).Append(stream.Context())
	if err != nil {
		return err
	}
	for {
		if err = client.SendMsg(req); err != nil {
			return err
		} else if err = stream.RecvMsg(req); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	if resp, err := client.CloseAndRecv(); err != nil {
		return err
	} else {
		return stream.SendAndClose(resp)
	}
}
