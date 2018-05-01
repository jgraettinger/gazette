package protocol

import (
	"context"

	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/protocol/localrpc"
)

// BrokerServerToClient adapts a BrokerServer to be used as a BrokerClient,
// using a channel-based stream implementation.
func BrokerServerToClient(srv BrokerServer) BrokerClient { return brokerS2C{srv} }

type brokerS2C struct{ srv BrokerServer }

func (a brokerS2C) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (Broker_ReadClient, error) {
	var cs = localrpc.New(ctx, func(ss grpc.ServerStream) error {
		return a.srv.Read(in, &brokerReadServer{ss})
	})
	return &brokerReadClient{cs}, nil
}

func (a brokerS2C) Append(ctx context.Context, opts ...grpc.CallOption) (Broker_AppendClient, error) {
	var cs = localrpc.New(ctx, func(ss grpc.ServerStream) error {
		return a.srv.Append(&brokerAppendServer{ss})
	})
	return &brokerAppendClient{cs}, nil
}

func (a brokerS2C) Replicate(ctx context.Context, opts ...grpc.CallOption) (Broker_ReplicateClient, error) {
	var cs = localrpc.New(ctx, func(ss grpc.ServerStream) error {
		return a.srv.Replicate(&brokerReplicateServer{ss})
	})
	return &brokerReplicateClient{cs}, nil
}
