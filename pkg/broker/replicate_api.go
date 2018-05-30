package broker

import (
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Replicate dispatches the BrokerServer.Replicate API.
func (s *Service) Replicate(stream pb.Broker_ReplicateServer) error {
	var req, err = stream.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	} else if err = s.resolver.waitForRevision(stream.Context(), req.Route.Revision); err != nil {
		return err
	}

	var res, status = s.resolver.resolve(req.Journal, false, false)
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

func (r *replicaImpl) serveReplicate(req *pb.ReplicateRequest, stream pb.Broker_ReplicateServer) error {
	var spool, err = r.acquireSpool(stream.Context(), false)
	if err != nil {
		return err
	}

	var resp = new(pb.ReplicateResponse)
	for {
		if *resp, err = spool.Apply(req); err != nil {
			return err
		}

		if req.Acknowledge {
			if err = stream.Send(resp); err != nil {
				return err
			}
		} else if resp.Status != pb.Status_OK {
			return fmt.Errorf("no ack requested but status != OK: %s", resp)
		}

		if err = stream.RecvMsg(req); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else if err = req.Validate(); err != nil {
			return err
		}
	}

	return nil
}
