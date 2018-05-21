package fragment

import (
	"context"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

const (
	// When a covering fragment cannot be found, we allow serving a *greater*
	// fragment so long as it was last modified at least this long ago.
	offsetJumpAgeThreshold = 6 * time.Hour
)

type Index struct {
	ctx    context.Context // Context over the lifetime of the Index.
	set    Set             // All Fragments of the index (local and remote).
	local  Set             // Local Fragments only (having non-nil File).
	condCh chan struct{}   // Notifies blocked queries that |set| was updated.
	mu     sync.RWMutex    // Guards |set| and |condCh|.
}

// NewIndex returns a new, empty Index.
func NewIndex(ctx context.Context) *Index {
	return &Index{
		ctx:    ctx,
		condCh: make(chan struct{}),
	}
}

// Query the Index for a Fragment matching the ReadRequest.
func (fi *Index) Query(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, *os.File, error) {
	defer fi.mu.RUnlock()
	fi.mu.RLock()

	var resp = &pb.ReadResponse{
		Offset: req.Offset,
	}

	// Special handling for reads at the Journal Write head.
	if resp.Offset == -1 {
		resp.Offset = fi.set.EndOffset()
	}

	for {
		var ind, found = fi.set.LongestOverlappingFragment(resp.Offset)

		// If the requested offset isn't covered by the index, but we do have a
		// Fragment covering a *greater* offset, where that Fragment is also older
		// than a large time.Duration, then: skip forward the request offset to
		// the Fragment offset. This case allows us to recover from "holes" or
		// deletions in the offset space of a Journal, while not impacting races
		// which can occur between delayed persistence to the Fragment store
		// vs hand-off of Journals to new brokers (eg, a new broker which isn't
		// yet aware of a Fragment currently being uploaded, should block a read
		// of an offset covered by that Fragment until it becomes available).
		if !found && ind != len(fi.set) &&
			!fi.set[ind].ModTime.IsZero() &&
			fi.set[ind].ModTime.Before(timeNow().Add(-offsetJumpAgeThreshold)) {

			resp.Offset = fi.set[ind].Begin
			found = true
		}

		if found {
			resp.Status = pb.Status_OK
			resp.WriteHead = fi.set.EndOffset()
			resp.Fragment = new(pb.Fragment)
			*resp.Fragment = fi.set[ind].Fragment
			return resp, fi.set[ind].File, nil
		}

		if !req.Block {
			resp.Status = pb.Status_OFFSET_NOT_YET_AVAILABLE
			resp.WriteHead = fi.set.EndOffset()
			return resp, nil, nil
		}

		var condCh = fi.condCh
		var err error

		// Wait for |condCh| to signal, or for the request |ctx| or Index
		// Context to be cancelled.
		fi.mu.RUnlock()
		select {
		case <-condCh:
			// Pass.
		case <-ctx.Done():
			err = ctx.Err()
		case <-fi.ctx.Done():
			err = fi.ctx.Err()
		}
		fi.mu.RLock()

		if err != nil {
			return nil, nil, err
		}
	}
}

// endOffset returns the last (largest) End offset in the index.
func (fi *Index) EndOffset() int64 {
	defer fi.mu.RUnlock()
	fi.mu.RLock()

	return fi.set.EndOffset()
}

// addLocal adds local Fragment |frag| to the index.
func (fi *Index) addLocal(frag Fragment) {
	defer fi.mu.Unlock()
	fi.mu.Lock()

	fi.set, _ = fi.set.Add(frag)
	fi.local, _ = fi.local.Add(frag)
	fi.wakeBlockedQueries()
}

// replaceRemote replaces all remote Fragments in the index with |set|.
func (fi *Index) replaceRemote(set Set) {
	defer fi.mu.Unlock()
	fi.mu.Lock()

	// Remove local fragments which are also present in |set|. This removes
	// references to held File instances, allowing them to be finalized by the
	// garbage collector. As Fragment Files have only the single open file-
	// descriptor and no remaining hard links, this also releases associated
	// disk and OS page buffer resources. Note that we cannot directly Close
	// these Fragment Files (and must instead rely on GC to collect them),
	// as they may still be referenced by concurrent read requests.
	fi.local = SetDifference(fi.local, set)

	// Extend |set| with remaining local Fragments not already in |set|.
	for _, frag := range fi.local {
		var ok bool

		if set, ok = set.Add(frag); !ok {
			panic("expected local fragment to not be covered")
		}
	}

	fi.set = set
	fi.wakeBlockedQueries()
}

// wakeBlockedQueries wakes all queries waiting for an index update.
// fi.mu must already be held.
func (fi *Index) wakeBlockedQueries() {
	// Close |condCh| to signal to waiting readers that the index has updated.
	// Create a new condition channel for future readers to block on, while
	// awaiting the next index update.
	close(fi.condCh)
	fi.condCh = make(chan struct{})
}

// GetSpecFunc returns a JournalSpec if available, or returns false.
type GetSpecFunc func() (spec *pb.JournalSpec, ok bool)

// WatchStores periodically invokes |getSpec| to obtain a current JournalSpec,
// queries its configured remote fragment stores at the configured cadence for
// their Fragment listings, and updates the Index accordingly. It closes
// |signalCh| after the first successful load, and exits if |getSpec| returns
// !ok or if the Index context is cancelled.
func (fi *Index) WatchStores(getSpec GetSpecFunc, signalCh chan<- struct{}) {
	for {
		var spec, ok = getSpec()
		if !ok {
			return
		}
		var set, err = walkAllStores(fi.ctx, spec.Name, spec.Fragment.Stores)

		if err != nil {
			log.WithFields(log.Fields{"err": err, "name": spec.Name}).Warn("failed to load remote index")
		} else {
			fi.replaceRemote(set)

			if signalCh != nil {
				close(signalCh)
				signalCh = nil
			}
		}

		select {
		case <-time.After(spec.Fragment.RefreshInterval):
			// Pass.
		case <-fi.ctx.Done():
			return
		}
	}
}

// walkAllStores enumerates Fragments from each of |stores| into the returned Set,
// or returns an encountered error.
func walkAllStores(_ context.Context, name pb.Journal, stores []pb.FragmentStore) (Set, error) {
	var set Set

	for _, store := range stores {
		var fs cloudstore.FileSystem
		var err error

		// TODO(johnny): This should take a context.
		if fs, err = cloudstore.NewFileSystem(nil, string(store)); err != nil {
			return Set{}, err
		}

		err = fs.Walk(name.String()+"/", WalkFuncAdapter(func(frag pb.Fragment) error {
			set, _ = set.Add(Fragment{Fragment: frag})
			return nil
		}))
		_ = fs.Close() // TODO(johnny): Can we remove Close from the FileSystem API?

		if err != nil {
			return Set{}, err
		}
	}
	return set, nil
}

var timeNow = time.Now
