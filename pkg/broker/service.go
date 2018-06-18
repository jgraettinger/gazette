package broker

import (
	"github.com/LiveRamp/gazette/pkg/client"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

type Service struct {
	dialer   client.Dialer
	resolver *resolver
}

func NewService(dialer client.Dialer, state *v3_allocator.State) *Service {
	return &Service{
		dialer:   dialer,
		resolver: newResolver(state),
	}
}
