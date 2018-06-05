package broker

type Service struct {
	dialer   dialer
	resolver *resolver
}

/*
func (s *Service) Run(ctx context.Context) error {
	for {
		if err := s.createSpec(); err == nil {
			break
		} else {
			log.WithFields(log.Fields{"err": err, "key": s.specKey}).
				Warn("failed to create BrokerSpec key; will retry")
		}

		select {
		case <-time.After(time.Second * 10):
			// Pass
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopCh:
			return nil
		}
	}

	var allocator = &v3_allocator.Allocator{
		KeySpace:      s.ks,
		LocalKey:      s.specKey,
		StateCallback: s.resolver.onAllocatorStateChange,
	}
	return allocator.Serve(ctx, s.etcd)
}

func (s *Service) SignalGracefulStop() error {
	close(s.stopCh)

	var zeroedSpec = s.spec
	zeroedSpec.JournalLimit = 0

	// Update the BrokerSpec iff it exists under our lease, and it hasn't been
	// updated since its initial creation.
	return txnSucceeds(
		s.etcd.Txn(context.Background()).
			If(
				clientv3.Compare(clientv3.LeaseValue(s.specKey), "=", s.lease),
				clientv3.Compare(clientv3.Version(s.specKey), "=", 1),
			).Then(clientv3.OpPut(s.specKey, zeroedSpec.MarshalString(), clientv3.WithIgnoreLease())).
			Commit())
}

func (s *Service) createSpec() error {
	// Create the BrokerSpec iff it doesn't exist.
	return txnSucceeds(
		s.etcd.Txn(context.Background()).
			If(clientv3.Compare(clientv3.ModRevision(s.specKey), "=", 0)).
			Then(clientv3.OpPut(s.specKey, s.spec.MarshalString(), clientv3.WithLease(s.lease))).
			Commit())
}

*/
