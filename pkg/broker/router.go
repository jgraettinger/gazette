package broker

import (
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type broker struct {
	ks   *keyspace.KeySpace
	spec pb.BrokerSpec

	connCache *lru.Cache

	journals   map[pb.Journal]*journal
	journalsMu sync.RWMutex
}

func NewBroker(ks *keyspace.KeySpace, spec pb.BrokerSpec) *broker {
	var cache, err = lru.New(1024)
	if err != nil {
		log.WithField("err", err).Panic("failed to create cache")
	}

	return &broker{
		ks:        ks,
		spec:      spec,
		connCache: cache,
		journals:  make(map[pb.Journal]*journal),
	}
}

func (b *broker) Read(req *pb.ReadRequest, srv pb.Broker_ReadServer) error {
	if err := req.Validate(); err != nil {
		return err
	}

	var res, status = b.resolve(req.Journal, false, !req.DoNotProxy)
	if status != pb.Status_OK {
		return srv.Send(&pb.ReadResponse{Status: status, Route: res.route})
	} else if res.replica == nil {
		return b.proxyRead(req, res.broker, srv)
	}

	return read(res.replica, req, srv)
}

func (b *broker) Append(srv pb.Broker_AppendServer) error {
	var req, err = srv.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	var res, status = b.resolve(req.Journal, true, true)
	if status != pb.Status_OK {
		return srv.SendAndClose(&pb.AppendResponse{Status: status, Route: res.route})
	} else if res.replica == nil {
		return b.proxyAppend(req, res.broker, srv)
	}

	return coordinate(res.replica, req, srv, b.peerConn)
}

func (b *broker) Replicate(srv pb.Broker_ReplicateServer) error {
	var req, err = srv.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	}

	var res, status = b.resolve(req.Journal, false, false)
	if status != pb.Status_OK {
		return srv.Send(&pb.ReplicateResponse{Status: status, Route: res.route})
	}

	return replicate(res.replica, req, srv)
}

func (b *broker) proxyRead(req *pb.ReadRequest, spec pb.BrokerSpec, srv pb.Broker_ReadServer) error {
	var conn, err = b.peerConn(spec.Id)
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

func (b *broker) proxyAppend(req *pb.AppendRequest, spec pb.BrokerSpec, srv pb.Broker_AppendServer) error {
	var conn, err = b.peerConn(spec.Id)
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

type resolution struct {
	spec    *pb.JournalSpec
	route   *pb.Route
	replica *journal
	broker  pb.BrokerSpec
}

func (b *broker) resolve(journal pb.Journal, requirePrimary bool, mayProxy bool) (res resolution, status pb.Status) {
	defer b.journalsMu.RUnlock()
	b.journalsMu.RLock()

	var ok bool

	if res.replica, ok = b.journals[journal]; ok {
		// Journal is locally replicated.
		res.spec = res.replica.spec
		res.route = &res.replica.route
		res.broker = b.spec
	} else {
		var item v3_allocator.Item

		b.ks.Mu.RLock()
		item, ok = v3_allocator.LookupItem(b.ks, journal.String())
		res.route = new(pb.Route)
		res.route.Extract(b.ks, journal)
		b.ks.Mu.RUnlock()

		if !ok {
			status = pb.Status_JOURNAL_NOT_FOUND
			return
		}
		res.spec = item.ItemValue.(*pb.JournalSpec)
	}

	if requirePrimary && res.route.Primary == -1 {
		status = pb.Status_NO_JOURNAL_PRIMARY_BROKER
		return
	} else if len(res.route.Brokers) == 0 {
		status = pb.Status_NO_JOURNAL_BROKERS
		return
	}

	// If the local journal can satisfy the request, we're done.
	// Otherwise, we must proxy to continue.
	if res.replica != nil && (!requirePrimary || res.replica.slot == 0) {
		return
	}
	res.replica = nil
	res.broker = pb.BrokerSpec{}

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
		res.broker = res.route.RandomReplica(b.spec.Id.Zone)
	}
	return
}

// UpdateLocalItems is an instance of v3_allocator.LocalItemsCallback.
// It expects that KeySpace is already read-locked.
func (b *broker) UpdateLocalItems(items []v3_allocator.LocalItem) {
	b.journalsMu.RLock()
	var prev = b.journals
	b.journalsMu.RUnlock()

	var next = make(map[pb.Journal]*journal, len(items))
	var route pb.Route

	// Walk |items| and create or transition journals as required to match.
	for _, la := range items {
		var spec = la.ItemValue.(*pb.JournalSpec)
		var assignment = la.Assignments[la.Index]
		route.Init(la.Assignments)

		var j, ok = prev[spec.Name]
		if !ok {
			j = newJournal()
			go j.index.watchStores(b.ks, spec.Name, j.initialLoadCh)
		}

		// Transition the journal if the JournalSpec, Route, or local Assignment
		// have changed. Note KeySpace builds a new JournalSpec instance with
		// each change, so testing pointer equality is sufficient.
		if j.spec != spec || !j.route.Equivalent(&route) || j.assignment.Raw.ModRevision != assignment.Raw.ModRevision {
			route.AttachEndpoints(b.ks)

			var transition = new(journal)
			*transition = *j

			transition.spec = spec
			transition.route = route
			transition.assignment = assignment

			j = transition
		}
		next[spec.Name] = j
	}

	b.journalsMu.Lock()
	b.journals = next
	b.journalsMu.Unlock()

	// Cancel any prior journals not included in |items|.
	for name, journal := range prev {
		if _, ok := next[name]; !ok {
			journal.cancel()
		}
	}
}

// peerConn obtains or builds a ClientConn for the given client |zone| & |name|.
func (b *broker) peerConn(id pb.BrokerSpec_ID) (*grpc.ClientConn, error) {
	if v, ok := b.connCache.Get(id); ok {
		return v.(*grpc.ClientConn), nil
	}

	b.ks.Mu.RLock()
	var member, ok = v3_allocator.LookupMember(b.ks, id.Zone, id.Suffix)
	b.ks.Mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no BrokerSpec found for (%s)", id)
	}
	var spec = member.MemberValue.(*pb.BrokerSpec)
	var u, _ = url.Parse(spec.Endpoint) // Previously validated; cannot fail.

	var conn, err = grpc.Dial(u.Host,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: time.Second * 30}),
		grpc.WithInsecure(),
	)
	if err == nil {
		b.connCache.Add(id, conn)
	}
	return conn, err
}

type buildConnFn func(id pb.BrokerSpec_ID) (*grpc.ClientConn, error)
