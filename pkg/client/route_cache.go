package client

import (
	"context"

	"github.com/hashicorp/golang-lru"
	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type RouteCacheClient struct {
	service pb.BrokerClient
	zone    string
	dialer  Dialer
	routes  *lru.Cache
}

func (rc *RouteCacheClient) Read(ctx context.Context, in *pb.ReadRequest, opts ...grpc.CallOption) (pb.Broker_ReadClient, error) {
	var iRoute, ok = rc.routes.Get(in.Journal)
	if !ok {
		return rc.service.Read(ctx, in, opts...)
	}

	var route = iRoute.(pb.Route)
	var ind = route.SelectReplica(pb.BrokerSpec_ID{Zone: rc.zone})

	var client, err = rc.dialer.Dial(ctx, route.Brokers[ind], route)
	if err != nil {
		rc.routes.Remove(in.Journal)
		return nil, err
	}
	return pb.NewBrokerClient(client).Read(ctx, in, opts...)
}

func (rc *RouteCacheClient) Append(ctx context.Context, opts ...grpc.CallOption) (pb.Broker_AppendClient, error) {
	if iJournal := ctx.Value(journalHint); iJournal != nil {
		var journal = iJournal.(pb.Journal)

		if iRoute, ok := rc.routes.Get(journal); ok {
			var route = iRoute.(pb.Route)

			if client, err := rc.dialer.Dial(ctx, route.Brokers[route.Primary], route); err != nil {
				rc.routes.Remove(journal)
				return nil, err
			} else {
				return pb.NewBrokerClient(client).Append(ctx, opts...)
			}

		}
	}
}

func (rc *RouteCacheClient) Replicate(ctx context.Context, opts ...grpc.CallOption) (pb.Broker_ReplicateClient, error) {
	panic("not supported")
}

var journalHint = &struct{}{}

func WithJournalHint(ctx context.Context, journal pb.Journal) context.Context {
	return context.WithValue(ctx, journalHint, journal)
}
