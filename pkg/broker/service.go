package broker

import (
	"github.com/coreos/etcd/clientv3"

	"github.com/LiveRamp/gazette/pkg/client"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type Service struct {
	resolver *resolver
	dialer   client.Dialer
}

func NewService(state *v3_allocator.State, dialer client.Dialer, lo pb.BrokerClient, etcd clientv3.KV) *Service {
	return &Service{
		dialer: dialer,
		resolver: newResolver(state, func(r *replica) {
			go maintenanceLoop(r, state.KS, lo, etcd)
		}),
	}
}
