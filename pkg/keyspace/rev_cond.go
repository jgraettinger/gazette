package keyspace

import (
	"context"
)

// RevCond implements a synchronizing condition variable on the current Etcd
// Revision of a wrapped KeySpace.
type RevCond struct {
	ks *KeySpace
	ch chan struct{} // Signals waiting goroutines of an update.
}

// NewRevCond returns a new RevCond wrapping the KeySpace.
func NewRevCond(ks *KeySpace) *RevCond {
	var rc = &RevCond{
		ks: ks,
		ch: make(chan struct{}),
	}

	ks.Mu.Lock()
	ks.Observers = append(ks.Observers, rc.observe)
	ks.Mu.Unlock()

	return rc
}

// WaitForRevision blocks until the KeySpace Revision is at least |revision|,
// or until the context is done. A read lock of the KeySpace RWMutex must be
// held at invocation, and will be re-acquired before WaitForRevision returns.
func (rc *RevCond) WaitForRevision(ctx context.Context, revision int64) error {
	for {
		if rc.ks.Header.Revision >= revision {
			return nil
		} else if err := ctx.Err(); err != nil {
			return err
		}

		var ch = rc.ch

		rc.ks.Mu.RUnlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		rc.ks.Mu.RLock()
	}
}

// Wake goroutines blocked on rc.ch.
func (rc *RevCond) observe() bool {
	close(rc.ch)
	rc.ch = make(chan struct{})
	return true
}
