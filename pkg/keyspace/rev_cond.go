package keyspace

import (
	"context"
	"sync"
)

// RevCond implements a synchronizing condition variable over Etcd revisions.
// Its intended use is in composition with types which manage state derived
// from (and updated by) Etcd, which guard that state with a sync.RWMutex,
// and which also wish to provide a condition variable to their clients.
type RevCond struct {
	revision int64
	mu       *sync.RWMutex
	ch       chan struct{} // Signals waiting goroutines of a |revision| update.
}

// NewRevCond returns a new RevCond with sync.RWMutex |mu|, and a closure
// for broadcasting updated Etcd revisions.
func NewRevCond(mu *sync.RWMutex) (*RevCond, func(revision int64)) {
	var rc = &RevCond{
		mu: mu,
		ch: make(chan struct{}),
	}
	return rc, rc.broadcast
}

// WaitForRevision blocks until the RevCond revision is at least |revision|,
// or until the context is done. A read-lock of the RevCond RWMutex must be
// held at invocation, and will be re-acquired before WaitForRevision returns.
func (rc *RevCond) WaitForRevision(ctx context.Context, revision int64) error {
	for {
		if rc.revision >= revision {
			return nil
		} else if err := ctx.Err(); err != nil {
			return err
		}

		var ch = rc.ch

		rc.mu.RUnlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		rc.mu.RLock()
	}
}

// broadcast an updated |revision|. A write-lock of the RevCond RWMutex must be
// held at invocation. broadcast panics if |revision| is less than a previously
// broadcast revision.
func (rc *RevCond) broadcast(revision int64) {
	if revision < rc.revision {
		panic("invalid revision")
	}
	rc.revision = revision
	close(rc.ch)
	rc.ch = make(chan struct{})
}
