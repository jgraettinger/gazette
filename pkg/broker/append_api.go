package broker

import (
	"crypto/sha1"
	"hash"
	"io"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Append dispatches the BrokerServer.Append API.
func (s *Service) Append(stream pb.Broker_AppendServer) error {
	var req, err = stream.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	var rev int64 = 1

	for done := false; !done && err == nil; {
		// Wait for revision |rev| before attempting the RPC.
		if err = s.resolver.waitForRevision(stream.Context(), rev); err != nil {
			break
		}

		var res, status = s.resolver.resolve(req.Journal, true, true)
		if status != pb.Status_OK {
			err = stream.SendAndClose(&pb.AppendResponse{Status: status, Route: res.route})
			break
		} else if res.replica == nil {
			err = proxyAppend(req, res.broker, stream, s.dialer)
			break
		}

		// Iff |err| == nil && |rev| != 0, a peer told us of a future & non-
		// equivalent Route revision. Attempt the RPC again at that revision.
		if rev, err = res.replica.serveAppend(req, stream, s.dialer); err == nil && rev == 0 {
			done = true
		}
	}

	if err != nil {
		log.WithFields(log.Fields{"err": err, "req": req}).Warn("failed to serve Append")
		return err
	}
	return nil
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

func (r *replicaImpl) serveAppend(req *pb.AppendRequest, stream pb.Broker_AppendServer, dialer dialer) (int64, error) {
	var pln, rev, err = r.acquirePipeline(stream.Context(), dialer)
	if pln == nil {
		return rev, err
	}

	// We now have sole ownership of the *send* side of the pipeline.

	var appender = beginAppending(pln, r.spec().Fragment)
	for appender.onRecv(req, stream.RecvMsg(req)) {
	}
	var waitFor, closeAfter = pln.readBarrier()
	var plnSendErr = pln.sendErr()

	if plnSendErr == nil {
		r.pipelineCh <- pln // Release the send side of |pln|.
	} else {
		pln.closeSend(r.spoolCh)
		r.pipelineCh <- nil

		log.WithFields(log.Fields{"err": err, "journal": r.spec().Name}).
			Warn("pipeline send failed")
	}

	// There may be pipelined commits prior to this one, who have not yet read
	// their responses. Block until they do so, such that our responses are the
	// next to receive. Similarly, defer a close to signal to RPCs pipelined
	// after this one, that they may in turn read their responses. When this
	// completes, we have sole ownership of the *receive* side of |pln|.
	<-waitFor
	defer func() { close(closeAfter) }()

	pln.gatherOK()

	if plnSendErr != nil {
		pln.gatherEOF()
	}

	if pln.recvErr() != nil {
		log.WithFields(log.Fields{"err": pln.recvErr(), "journal": r.spec().Name}).
			Warn("pipeline receive failed")
	}

	if appender.reqErr != nil {
		return 0, appender.reqErr
	} else if plnSendErr != nil {
		return 0, plnSendErr
	} else if pln.recvErr() != nil {
		return 0, pln.recvErr()
	} else {
		return 0, stream.SendAndClose(&pb.AppendResponse{
			Status: pb.Status_OK,
			Route:  &pln.route,
			Commit: appender.reqFragment,
		})
	}
}

type appender struct {
	pln  *pipeline
	spec pb.JournalSpec_Fragment

	reqFragment *pb.Fragment
	reqSummer   hash.Hash
	reqErr      error
}

func beginAppending(pln *pipeline, spec pb.JournalSpec_Fragment) appender {
	// Potentially roll the Fragment forward prior to serving the append.
	// We expect this to always succeed and don't ask for an acknowledgement.
	var proposal, update = updateProposal(pln.spool.Fragment.Fragment, spec)

	if update {
		pln.scatter(&pb.ReplicateRequest{
			Proposal:    &proposal,
			Acknowledge: false,
		})
	}

	return appender{
		pln:  pln,
		spec: spec,

		reqFragment: &pb.Fragment{
			Journal: pln.spool.Fragment.Journal,
			Begin:   pln.spool.Fragment.End,
			End:     pln.spool.Fragment.End,
		},
		reqSummer: sha1.New(),
	}
}

func (a *appender) onRecv(req *pb.AppendRequest, err error) bool {
	if err == nil {
		err = req.Validate()
	}

	if err != nil {
		// Reached end-of-input for this Append stream.
		a.reqFragment.Sum = pb.SHA1SumFromDigest(a.reqSummer.Sum(nil))

		var proposal = new(pb.Fragment)
		if err == io.EOF {
			// Commit the Append, by scattering the next Fragment to be committed
			// to each peer. They will inspect & validate the Fragment locally,
			// and commit or return an error.
			*proposal = a.pln.spool.Next()
		} else {
			// A client-side read error occurred. The pipeline is still in a good
			// state, but any partial spooled content must be rolled back.
			*proposal = a.pln.spool.Fragment.Fragment

			a.reqErr = err
			a.reqFragment = nil
		}

		a.pln.scatter(&pb.ReplicateRequest{
			Proposal:    proposal,
			Acknowledge: true,
		})
		return false
	}

	// Forward content through the pipeline.
	a.pln.scatter(&pb.ReplicateRequest{
		Content:      req.Content,
		ContentDelta: a.reqFragment.ContentLength(),
	})
	_, _ = a.reqSummer.Write(req.Content) // Cannot error.
	a.reqFragment.End += int64(len(req.Content))

	return a.pln.sendErr() == nil
}

func updateProposal(cur pb.Fragment, spec pb.JournalSpec_Fragment) (pb.Fragment, bool) {
	// If the proposed Fragment is non-empty, but not yet at the target length,
	// don't propose changes to it.
	if cur.ContentLength() > 0 && cur.ContentLength() < spec.Length {
		return cur, false
	}

	var next = cur
	next.Begin = next.End
	next.CompressionCodec = spec.CompressionCodec

	if len(spec.Stores) != 0 {
		next.BackingStore = spec.Stores[0]
	} else {
		next.BackingStore = ""
	}
	return next, next != cur
}
