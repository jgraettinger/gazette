package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

// dialer returns a ClientConn for a peer identified by its BrokerSpec_ID.
type dialer interface {
	dial(ctx context.Context, id pb.BrokerSpec_ID) (*grpc.ClientConn, error)
}

type dialerImpl struct {
	ks    *keyspace.KeySpace
	cache *lru.Cache
}

// newDialer builds and returns a dialer which maps broker IDs to their
// BrokerSpec endpoints through the provided KeySpace.
func newDialer(ks *keyspace.KeySpace) dialer {
	var cache, err = lru.New(1024)
	if err != nil {
		log.WithField("err", err).Panic("failed to create cache")
	}
	return &dialerImpl{
		ks:    ks,
		cache: cache,
	}
}

func (d *dialerImpl) dial(ctx context.Context, id pb.BrokerSpec_ID) (*grpc.ClientConn, error) {
	if v, ok := d.cache.Get(id); ok {
		return v.(*grpc.ClientConn), nil
	}

	d.ks.Mu.RLock()
	var member, ok = v3_allocator.LookupMember(d.ks, id.Zone, id.Suffix)
	d.ks.Mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no BrokerSpec found (id: %s)", &id)
	}
	var spec = member.MemberValue.(*pb.BrokerSpec)
	var url = spec.Endpoint.URL()

	var conn, err = grpc.DialContext(ctx, url.Host,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: time.Second * 30}),
		grpc.WithInsecure(),
	)
	if err == nil {
		d.cache.Add(id, conn)
	}
	return conn, err
}
