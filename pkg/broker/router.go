package broker

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/pkg/fragment"
	"github.com/coreos/etcd/clientv3"
	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

// Router manages routing of journals to brokers, including maintaining a index
// of locally-assigned replicas, and resolving other journals to broker peers.
// It implements the pb.BrokerServer interface.
type Router struct {
	ks        *keyspace.KeySpace
	id        pb.BrokerSpec_ID
	etcd      *clientv3.Client
	connCache *lru.Cache

	replicas map[pb.Journal]*replica // Guarded by |mu|.
	updateCh chan struct{}           // Guarded by |mu|.
	mu       sync.RWMutex
}

// NewRouter builds and returns an empty, initialized Router.
func NewRouter(ks *keyspace.KeySpace, id pb.BrokerSpec_ID, etcd *clientv3.Client) *Router {
	var cache, err = lru.New(1024)
	if err != nil {
		log.WithField("err", err).Panic("failed to create cache")
	}
	return &Router{
		ks:        ks,
		id:        id,
		etcd:      etcd,
		connCache: cache,
		replicas:  make(map[pb.Journal]*replica),
		updateCh:  make(chan struct{}),
	}
}

func (rtr *Router) Read(req *pb.ReadRequest, srv pb.Broker_ReadServer) error {
	if err := req.Validate(); err != nil {
		return err
	}

	var res, status = rtr.resolve(req.Journal, false, !req.DoNotProxy)
	if status != pb.Status_OK {
		return srv.Send(&pb.ReadResponse{Status: status, Route: res.route})
	} else if res.replica == nil {
		return rtr.proxyRead(req, res.broker, srv)
	}

	return read(res.replica, req, srv)
}

func waitForInitialIndexLoad(ctx context.Context, r *replica) error {
	select {
	case <-r.initialLoadCh:
		// Pass.
	case <-r.ctx.Done():
		return r.ctx.Err() // Journal cancelled.
	case <-ctx.Done():
		return ctx.Err() // Context cancelled.
	}
	return nil
}

func (rtr *Router) waitForRevision(ctx context.Context, rev int64) error {
	rtr.mu.RLock()
	for rtr.ks.Header.Revision < rev {
		var ch = rtr.updateCh
		rtr.mu.RUnlock()

		select {
		case <-ch:
			// KeySpace updated. Loop to check against |rev|.
		case <-ctx.Done():
			return ctx.Err()
		}

		rtr.mu.RLock()
	}
	rtr.mu.RUnlock()
	return nil
}

func (rtr *Router) Append(srv pb.Broker_AppendServer) error {
	var req, err = srv.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}
	return rtr.append(srv, req)
}

func (rtr *Router) append(srv pb.Broker_AppendServer, req *pb.AppendRequest) error {
	var res, status = rtr.resolve(req.Journal, true, true)
	if status != pb.Status_OK {
		return srv.SendAndClose(&pb.AppendResponse{Status: status, Route: res.route})
	} else if res.replica == nil {
		return rtr.proxyAppend(req, res.broker, srv)
	}

	var pln, rev, err = acquirePipeline(srv.Context(), res.replica, rtr.peerConn)

	if err != nil {
		log.WithFields(log.Fields{
			"err":     err,
			"journal": res.replica.spec().Name,
		}).Warn("failed to acquire pipeline")

		return err
	} else if rev != 0 {
		// If a peer told us of a future & non-equivalent Route revision,
		// wait for that revision and attempt the RPC again.
		if err = rtr.waitForRevision(srv.Context(), rev); err != nil {
			return err
		}
		return rtr.append(srv, req)
	}

	// We now have sole ownership of the *send* side of the pipeline.

	// Potentially roll the Fragment forward prior to serving the append.
	// We expect this to always succeed and don't ask for an acknowledgement.
	if proposal, ok := updateProposal(pln.spool.Fragment.Fragment,
		res.replica.spec().Fragment); ok {
		pln.scatter(&pb.ReplicateRequest{Proposal: &proposal})
	}

	var responseFrag, clientErr = proxyAppendRequest(srv, pln)
	var waitFor, closeAfter = pln.readBarrier()
	var plnSendErr = pln.sendErr()

	if plnSendErr == nil {
		res.replica.plnHandoffCh <- pln // Release the send side of |pln|.
	} else {
		pln.closeSend(res.replica.spoolCh)
		res.replica.plnHandoffCh <- nil

		log.WithFields(log.Fields{
			"err":     err,
			"journal": res.replica.spec().Name,
		}).Warn("pipeline send failed")
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
		log.WithFields(log.Fields{
			"err":     pln.recvErr(),
			"journal": res.replica.spec().Name,
		}).Warn("pipeline receive failed")
	}

	if clientErr != nil {
		return clientErr
	} else if plnSendErr != nil {
		return plnSendErr
	} else if pln.recvErr() != nil {
		return pln.recvErr()
	} else {
		return srv.SendAndClose(&pb.AppendResponse{
			Status: pb.Status_OK,
			Route:  &pln.route,
			Commit: responseFrag,
		})
	}
}

func (rtr *Router) Replicate(srv pb.Broker_ReplicateServer) error {
	var req, err = srv.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	} else if err = rtr.waitForRevision(srv.Context(), req.Route.Revision); err != nil {
		return err
	}

	var res, status = rtr.resolve(req.Journal, false, false)
	if status != pb.Status_OK {
		return srv.Send(&pb.ReplicateResponse{Status: status, Route: res.route})
	}

	if !res.route.Equivalent(req.Route) {
		return srv.Send(&pb.ReplicateResponse{Status: pb.Status_WRONG_ROUTE, Route: res.route})
	}

	var spool fragment.Spool
	if spool, err = acquireSpool(srv.Context(), res.replica); err != nil {
		return err
	}

	var resp = new(pb.ReplicateResponse)
	for {
		if *resp, err = spool.Apply(req); err != nil {
			return err
		}

		if req.Acknowledge {
			if err = srv.Send(resp); err != nil {
				return err
			}
		} else if resp.Status != pb.Status_OK {
			return fmt.Errorf("no ack requested but status != OK: %s", resp)
		}

		// We've successfully synchronized under this Route.
		if req.Proposal != nil && resp.Status == pb.Status_OK {
			res.replica.maybeUpdateAssignmentRoute(rtr.etcd)
		}

		if err = srv.RecvMsg(req); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else if err = req.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (rtr *Router) proxyRead(req *pb.ReadRequest, to pb.BrokerSpec_ID, srv pb.Broker_ReadServer) error {
	var conn, err = rtr.peerConn(to)
	if err != nil {
		return err
	}
	client, err := pb.NewBrokerClient(conn).Read(srv.Context(), req)
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
		} else if err = srv.Send(resp); err != nil {
			return err
		}
	}
}

func (rtr *Router) proxyAppend(req *pb.AppendRequest, to pb.BrokerSpec_ID, srv pb.Broker_AppendServer) error {
	var conn, err = rtr.peerConn(to)
	if err != nil {
		return err
	}
	client, err := pb.NewBrokerClient(conn).Append(srv.Context())
	if err != nil {
		return err
	}
	for {
		if err = client.SendMsg(req); err != nil {
			return err
		} else if err = srv.RecvMsg(req); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	if resp, err := client.CloseAndRecv(); err != nil {
		return err
	} else {
		return srv.SendAndClose(resp)
	}
}

// resolution is the result of resolving a journal to a route and
// target broker, which may be local.
type resolution struct {
	route   *pb.Route
	broker  pb.BrokerSpec_ID
	replica *replica
}

// resolve a journal to a target broker, which may be local or a proxy-able peer.
// If a resolution is not possible, a Status != Status_OK is returned indicating
// why resolution failed.
func (rtr *Router) resolve(journal pb.Journal, requirePrimary bool, mayProxy bool) (res resolution, status pb.Status) {
	defer rtr.mu.RUnlock()
	rtr.mu.RLock()

	var ok bool

	if res.replica, ok = rtr.replicas[journal]; ok {
		// Journal is locally replicated.
		res.route = &res.replica.route
		res.broker = rtr.id
	} else {

		rtr.ks.Mu.RLock()
		_, ok = v3_allocator.LookupItem(rtr.ks, journal.String())

		var assignments = rtr.ks.KeyValues.Prefixed(
			v3_allocator.ItemAssignmentsPrefix(rtr.ks, journal.String()))

		res.route = new(pb.Route)
		res.route.Init(assignments)
		res.route.AttachEndpoints(rtr.ks)

		rtr.ks.Mu.RUnlock()

		if !ok {
			status = pb.Status_JOURNAL_NOT_FOUND
			return
		}
	}

	if requirePrimary && res.route.Primary == -1 {
		status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
		return
	} else if len(res.route.Brokers) == 0 {
		status = pb.Status_NO_JOURNAL_BROKERS
		return
	}

	// If the local replica can satisfy the request, we're done.
	// Otherwise, we must proxy to continue.
	if res.replica != nil && (!requirePrimary || res.replica.isPrimary()) {
		return
	}
	res.replica = nil
	res.broker = pb.BrokerSpec_ID{}

	if !mayProxy {
		if requirePrimary {
			status = pb.Status_NOT_JOURNAL_PRIMARY_BROKER
		} else {
			status = pb.Status_NOT_JOURNAL_BROKER
		}
		return
	}

	if requirePrimary {
		res.broker = res.route.Brokers[res.route.Primary]
	} else {
		res.broker = res.route.RandomReplica(rtr.id.Zone)
	}
	return
}

// UpdateLocalItems is an instance of v3_allocator.LocalItemsCallback.
// It expects that KeySpace is already read-locked.
func (rtr *Router) UpdateLocalItems(items []v3_allocator.LocalItem) {
	rtr.mu.RLock()
	var prev = rtr.replicas
	rtr.mu.RUnlock()

	var next = make(map[pb.Journal]*replica, len(items))
	var route pb.Route

	// Walk |items| and create or transition replicas as required to match.
	for _, la := range items {
		var name = pb.Journal(la.Item.Decoded.(v3_allocator.Item).ID)

		var assignment = la.Assignments[la.Index]
		route.Init(la.Assignments)

		var r, ok = prev[name]
		if !ok {
			r = newReplica()
			go r.index.watchStores(rtr.ks, name, r.initialLoadCh)
		}

		var routeChanged = !r.route.Equivalent(&route)

		// Transition if the Item, local Assignment, or Route have changed.
		if r.journal.Raw.ModRevision != la.Item.Raw.ModRevision ||
			r.assignment.Raw.ModRevision != assignment.Raw.ModRevision ||
			routeChanged {

			var clone = new(replica)
			*clone = *r

			clone.journal = la.Item
			clone.assignment = assignment
			clone.route = route.Copy()
			clone.route.AttachEndpoints(rtr.ks)

			r = clone
		}

		if routeChanged && r.isPrimary() {
			// Issue an empty write to drive the quick convergence
			// of replica Route announcements in Etcd.
			if conn, err := rtr.peerConn(rtr.id); err == nil {
				go issueEmptyWrite(conn, name)
			} else {
				log.WithField("err", err).Error("failed to build loopback *grpc.ClientConn")
			}
		}
		next[name] = r
	}

	// Obtain write-lock to atomically swap out the |replicas| map,
	// and to signal any RPCs waiting on an Etcd update.
	rtr.mu.Lock()
	rtr.replicas = next
	close(rtr.updateCh)
	rtr.updateCh = make(chan struct{})
	rtr.mu.Unlock()

	// Cancel any prior replicas not included in |items|.
	for name, journal := range prev {
		if _, ok := next[name]; !ok {
			journal.cancel()
		}
	}
}

// peerConn obtains or builds a ClientConn for the given BrokerSpec_ID.
func (rtr *Router) peerConn(id pb.BrokerSpec_ID) (*grpc.ClientConn, error) {
	if v, ok := rtr.connCache.Get(id); ok {
		return v.(*grpc.ClientConn), nil
	}

	rtr.ks.Mu.RLock()
	var member, ok = v3_allocator.LookupMember(rtr.ks, id.Zone, id.Suffix)
	rtr.ks.Mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no BrokerSpec found for (%s)", id)
	}
	var spec = member.MemberValue.(*pb.BrokerSpec)
	var url = spec.Endpoint.URL()

	var conn, err = grpc.Dial(url.Host,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: time.Second * 30}),
		grpc.WithInsecure(),
	)
	if err == nil {
		rtr.connCache.Add(id, conn)
	}
	return conn, err
}

type buildConnFn func(id pb.BrokerSpec_ID) (*grpc.ClientConn, error)
