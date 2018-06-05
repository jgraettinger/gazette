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
	}

	resolution, err := s.resolver.resolve(resolveArgs{
		ctx:                   stream.Context(),
		journal:               req.Journal,
		mayProxy:              false,
		requirePrimary:        false,
		requireFullAssignment: true,
		minEtcdRevision:       maxInt64(1, req.Header.GetEtcd().Revision),
	})
	if err != nil {
		return err
	}

	// Require that the request Route is equivalent to the Route we resolved to.
	if resolution.status == pb.Status_OK &&
		!resolution.Header.Route.Equivalent(&req.Header.Route) {
		resolution.status = pb.Status_WRONG_ROUTE
	}
	if resolution.status != pb.Status_OK {
		return stream.Send(&pb.ReplicateResponse{
			Status: resolution.status,
			Header: &resolution.Header,
		})
	}

	if err := resolution.replica.serveReplicate(req, stream); err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Replicate")
		return err
	}
	return nil
}

func (r *replica) serveReplicate(req *pb.ReplicateRequest, stream pb.Broker_ReplicateServer) error {
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
