package broker

import (
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Replicate dispatches the BrokerServer.Replicate API.
func (s *Service) Replicate(stream pb.Broker_ReplicateServer) error {
	var req, err = stream.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	var res resolution
	res, err = s.resolver.resolve(resolveArgs{
		ctx:                   stream.Context(),
		journal:               req.Journal,
		mayProxy:              false,
		requirePrimary:        false,
		requireFullAssignment: true,
		proxyHeader:           req.Header,
	})
	if err != nil {
		return err
	}

	// Require that the request Route is equivalent to the Route we resolved to.
	if res.status == pb.Status_OK && !res.Header.Route.Equivalent(&req.Header.Route) {
		res.status = pb.Status_WRONG_ROUTE
	}
	if res.status != pb.Status_OK {
		return stream.Send(&pb.ReplicateResponse{
			Status: res.status,
			Header: &res.Header,
		})
	}

	var spool fragment.Spool
	if spool, err = acquireSpool(stream.Context(), res.replica, false); err != nil {
		return err
	}
	spool, err = serveReplicate(stream, req, spool)

	// Roll-back any uncommitted content, and release ownership of Spool.
	spool.MustApply(&pb.ReplicateRequest{Proposal: &spool.Fragment.Fragment})
	res.replica.spoolCh <- spool

	if err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Replicate")
	}
	return err
}

// serveReplicate evaluates a client's Replicate RPC against the local Spool.
func serveReplicate(stream pb.Broker_ReplicateServer, req *pb.ReplicateRequest, spool fragment.Spool) (fragment.Spool, error) {
	for {
		var resp, err = spool.Apply(req)
		if err != nil {
			return spool, err
		}

		if req.Acknowledge {
			if err = stream.SendMsg(&resp); err != nil {
				return spool, err
			}
		} else if resp.Status != pb.Status_OK {
			return spool, fmt.Errorf("no ack requested but status != OK: %s", &resp)
		}

		if req, err = stream.Recv(); err == io.EOF {
			return spool, nil
		} else if err != nil {
			return spool, err
		} else if err = req.Validate(); err != nil {
			return spool, err
		}
	}
}
