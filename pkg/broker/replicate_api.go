package broker

import (
	"fmt"
	"io"

	"github.com/LiveRamp/gazette/pkg/fragment"
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

	spool, err = serveReplicate(req, stream, spool)
	res.replica.spoolCh <- spool // Release ownership of Spool.

	if err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Replicate")
	}
	return err
}

func serveReplicate(req *pb.ReplicateRequest, stream pb.Broker_ReplicateServer, spool fragment.Spool) (fragment.Spool, error) {
	var err error

	var resp = new(pb.ReplicateResponse)
	for {
		if *resp, err = spool.Apply(req); err != nil {
			return spool, err
		}

		if req.Acknowledge {
			if err = stream.Send(resp); err != nil {
				return spool, err
			}
		} else if resp.Status != pb.Status_OK {
			return spool, fmt.Errorf("no ack requested but status != OK: %s", resp)
		}

		if err = stream.RecvMsg(req); err == io.EOF {
			return spool, nil
		} else if err != nil {
			return spool, err
		} else if err = req.Validate(); err != nil {
			return spool, err
		}
	}
}
