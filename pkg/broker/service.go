package broker

import (
	"github.com/LiveRamp/gazette/pkg/client"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	"github.com/coreos/etcd/clientv3"
)

type Service struct {
	resolver *resolver
	dialer   client.Dialer
}

func NewService(state *v3_allocator.State, dialer client.Dialer, etcd clientv3.KV) *Service {
	return &Service{
		dialer:   dialer,
		resolver: newResolver(state, spawnMaintenanceLoop(state, dialer, etcd)),
	}
}

func spawnMaintenanceLoop(state *v3_allocator.State, dialer client.Dialer, etcd clientv3.KV) func(*replica) {
	return func(r *replica) {
		go maintenanceLoop(r, state, dialer, etcd)
	}
}
