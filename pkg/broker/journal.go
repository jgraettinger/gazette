package broker

import (
	"context"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type journal struct {
	// The following fields are held constant over the lifetime of a journal:

	// Context tied to processing lifetime of this journal by this broker.
	// Cancelled when this broker is no longer responsible for the journal.
	ctx    context.Context
	cancel context.CancelFunc
	// Index of all known Fragments of the journal.
	index *fragmentIndex
	// spoolCh synchronizes access to the single Spool of the journal.
	spoolCh chan fragment.Spool
	// txnHandoffCh allows an Append holding an in-flight transaction, to hand
	// the transaction off to another ready Append wanting to continue it.
	txnHandoffCh chan *transaction
	// initialLoadCh is closed after the first remote fragment store listing,
	// and is used to gate Append requests (only) until the fragmentIndex
	// has been initialized with remote listings.
	initialLoadCh chan struct{}

	// Remaining fields are constant over the lifetime of a specific journal
	// instance, but may change as the journal instance is updated:

	// Current specification of the journal.
	spec *pb.JournalSpec
	// Current broker routing topology of the journal.
	route pb.Route
	// Current Assignment slot of the journal to this broker.
	slot int
}

func newJournal() *journal {
	var ctx, cancel = context.WithCancel(context.Background())

	var spoolCh = make(chan fragment.Spool, 1)
	spoolCh <- fragment.Spool{}

	return &journal{
		ctx:           ctx,
		cancel:        cancel,
		index:         newFragmentIndex(ctx),
		spoolCh:       spoolCh,
		txnHandoffCh:  make(chan *transaction),
		initialLoadCh: make(chan struct{}),
	}
}

// prepareSpool readies Spool for it's next write, by:
//  * Rolling the Spool forward if it's less than the maximum end offset
//    of the fragment index (eg, of a discovered remote Fragment).
//  * Rolling the Spool forward if it's ContentLength has reached a threshold.
//  * If not already, opening the Spool.
//  * If not already and the journal is primary, initializing the Spool for
//    incremental compression.
func prepareSpool(s *fragment.Spool, j *journal) error {
	if eo := j.index.endOffset(); eo > s.Fragment.End {
		s.Roll(j.spec, eo)
	} else if s.Fragment.ContentLength() >= j.spec.FragmentLength {
		s.Roll(j.spec, s.Fragment.End)
	}

	if s.Fragment.File == nil {
		if err := s.Open(); err != nil {
			return err
		}
	}

	// |slot == 0| makes us primary and the presumptive broker to persist the
	// Fragment. Initialize a compressor to speculatively compress committed
	// content as it arrives. Compression can be expensive, and compressing the
	// Fragment as it's built effectively back-pressures the cost onto Journal
	// writers, ensuring we don't accept writes faster than we can compress them.
	if j.slot == 0 && s.CompressedFile == nil {
		if err := s.InitCompressor(); err != nil {
			return err
		}
	}

	return nil
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
