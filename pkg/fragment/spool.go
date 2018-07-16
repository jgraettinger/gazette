package fragment

import (
	"crypto/sha1"
	"encoding"
	"fmt"
	"hash"
	"io"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/codecs"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Spool is a Fragment which is in the process of being created, backed by a
// local *os.File. As commits occur and the file extent is updated, the Spool
// Fragment is also be updated to reflect the new committed extent. At all
// times, the Spool Fragment is a consistent, valid Fragment.
type Spool struct {
	// Fragment at time of last commit.
	Fragment
	// Compressed form of the Fragment, compressed under Fragment.CompressionCodec.
	// If |Primary|, the Spool will incrementally compress writes as they commit.
	// Typically, only one broker replicating a journal will have Primary set
	// (eg, the primary broker), and that broker will immediately persist the Spool
	// once finished. Should a failure occur, other brokers can construct the
	// compressed form as needed from the backing Fragment.File.
	compressedFile File
	// Compressor of |compressedFile|.
	compressor codecs.Compressor

	delta    int64     // Delta offset of next byte to write, relative to Fragment.End.
	summer   hash.Hash // Running SHA1 of the Fragment.File, through |Fragment.End + delta|.
	sumState []byte    // SHA1 |summer| internal state at the last Fragment commit.

	observer SpoolObserver
}

// SpoolObserver is notified of important events in the Spool lifecycle.
type SpoolObserver interface {
	// SpoolCommit is called when the Spool Fragment is extended.
	SpoolCommit(Fragment)
	// SpoolComplete is called when the Spool has been completed.
	SpoolComplete(Spool)
}

// NewSpool returns an empty Spool of |journal|.
func NewSpool(journal pb.Journal, observer SpoolObserver) Spool {
	return Spool{
		Fragment: Fragment{Fragment: pb.Fragment{
			Journal:          journal,
			CompressionCodec: pb.CompressionCodec_NONE,
		}},
		summer:   sha1.New(),
		observer: observer,
	}
}

// Apply the ReplicateRequest to the Spool, returning any encountered error.
func (s *Spool) Apply(r *pb.ReplicateRequest) (pb.ReplicateResponse, error) {
	if r.Proposal != nil {
		return s.applyCommit(r), nil
	} else {
		return pb.ReplicateResponse{}, s.applyContent(r)
	}
}

// MustApply applies the ReplicateRequest, and panics if a !OK status is returned
// or error occurs. MustApply is a convenience for cases such as rollbacks, where
// the request is derived from the Spool itself and cannot reasonably fail.
func (s *Spool) MustApply(r *pb.ReplicateRequest) {
	if resp, err := s.Apply(r); err != nil {
		panic(err.Error())
	} else if resp.Status != pb.Status_OK {
		panic(resp.Status.String())
	}
}

// Next returns the next Fragment which can be committed by the Spool.
func (s *Spool) Next() pb.Fragment {
	var f = s.Fragment.Fragment
	f.End += s.delta

	// Empty fragments are special-cased to have Sum of zero (as technically, SHA1('') != <zero>).
	if f.Begin == f.End {
		f.Sum = pb.SHA1Sum{}
	} else {
		f.Sum = pb.SHA1SumFromDigest(s.summer.Sum(nil))
	}
	return f
}

// CodecReader returns a ReadCloser of Spool content, compressed under the
// Spool's CompressionCodec. To ensure associated resources are released,
// callers must consume the returned Reader until EOF or other error is
// returned. CodecReader panics if called before the Spool is completed.
func (s *Spool) CodecReader() io.Reader {
	if s.compressor != nil {
		panic("Spool not finalized")
	}

	if s.CompressionCodec != pb.CompressionCodec_NONE && s.compressedFile != nil {
		return io.NewSectionReader(s.compressedFile, 0, 100)
	}
	// Note that |File| could extend beyond ContentLength (eg,
	// because of a partial write which was then rolled-back).
	var r = io.NewSectionReader(s.File, 0, s.ContentLength())

	if s.CompressionCodec == pb.CompressionCodec_NONE {
		return r
	}
	// Fallback: we must compress, but |compressedFile| is not valid.
	// Return a Pipe which has compressed content written into it.
	var pr, pw = io.Pipe()

	go func() {
		if zw, err := codecs.NewCodecWriter(pw, s.CompressionCodec); err != nil {
			pw.CloseWithError(err)
		} else if _, err = io.Copy(zw, r); err != nil {
			_ = zw.Close()
			pw.CloseWithError(err)
		} else {
			pw.CloseWithError(zw.Close())
		}
	}()

	return pr
}

// String returns a debugging representation of the Spool.
func (s Spool) String() string {
	return fmt.Sprintf("Spool<Fragment: %s, Primary: %t, delta: %d>",
		s.Fragment.String(), s.Primary, s.delta)
}

func (s *Spool) applyCommit(r *pb.ReplicateRequest) pb.ReplicateResponse {
	// There are three commit cases which can succeed:
	//  1) Exact commit of current fragment.
	//  2) Exact commit of current fragment, extended by |delta|.
	//  3) Trivial commit of an empty Fragment at or beyond the current Fragment.End.
	//
	// One important error case is also handled: if the proposed Fragment is non-
	// empty but beyond our current Fragment.End, we still return a
	// FRAGMENT_MISMATCH but first roll to an empty Spool at the advertised End.
	// This case happens, eg, when a new peer is introduced to a route and must
	// "catch up" with recently written content. On receiving the MISMATCH error
	// the primary replica will restart the pipeline with an empty Proposal at
	// that offset, which this peer can now participate in.
	//
	// The case can also happen on recovery from network partitions, where some
	// replicas believe a commit occurred and others don't (note the Append
	// RPC itself will have failed in this case, forcing the client to retry).
	// Progress is made by agreeing to proceed from a new, empty Spool starting
	// at the maximal offset observed from any replica.

	// Case 1? "Undo" any partial content, by rolling back |delta| and |summer|.
	if s.Fragment.Fragment == *r.Proposal {
		s.delta = 0
		s.restoreSumState()
		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	// Case 2? Apply the |delta| bytes spooled since last commit.
	if next := s.Next(); next == *r.Proposal {

		if s.compressor != nil {
			// Build a reader over the new content, and run it through the Compressor.
			if _, err := io.Copy(s.compressor,
				io.NewSectionReader(s.File, s.Fragment.ContentLength(), s.delta)); err != nil {

				// |err| invalidates the compressor but does not fail the commit.
				log.WithFields(log.Fields{"proposal": *r.Proposal, "err": err}).Error("failed to compress")
				s.finalizeCompressor(true)
			}
		}

		s.Fragment.Fragment = next
		s.observer.SpoolCommit(s.Fragment)

		s.delta = 0
		s.saveSumState()

		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	// Case 3? Complete the current Fragment, and re-initialize to the new one.
	if r.Proposal.Journal == s.Fragment.Journal &&
		r.Proposal.Begin >= s.Fragment.End &&
		r.Proposal.ContentLength() == 0 &&
		r.Proposal.Sum.IsZero() {

		s.finalizeCompressor(false)
		if s.ContentLength() != 0 {
			s.observer.SpoolComplete(*s)
		}

		*s = Spool{
			Fragment: Fragment{Fragment: *r.Proposal},
			Primary:  s.Primary,
			summer:   sha1.New(),
			sumState: zeroedSHA1State,
			observer: s.observer,
		}
		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	// Is the Proposal.End beyond our own extents? Roll to that offset.
	if r.Proposal.Journal == s.Fragment.Journal && r.Proposal.End > s.Fragment.End+s.delta {

		s.finalizeCompressor(false)
		if s.ContentLength() != 0 {
			s.observer.SpoolComplete(*s)
		}
		*s = Spool{
			Fragment: Fragment{
				Fragment: pb.Fragment{
					Journal:          r.Proposal.Journal,
					Begin:            r.Proposal.End,
					End:              r.Proposal.End,
					CompressionCodec: r.Proposal.CompressionCodec,
					BackingStore:     r.Proposal.BackingStore,
				},
			},
			Primary:  s.Primary,
			summer:   sha1.New(),
			sumState: zeroedSHA1State,
			observer: s.observer,
		}
	}

	return pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &s.Fragment.Fragment,
	}
}

func (s *Spool) initCompressedFile() (err error) {
	for {
		if s.compressedFile == nil {
			s.compressedFile, err = newSpoolFile()
		}
		if s.compressor == nil && err == nil {
			s.compressedFile.Seek(0, io.SeekStart)
			s.compressor, err = codecs.NewCodecWriter(s.compressedFile, s.CompressionCodec)
		}

	}
}

func (s *Spool) applyContent(r *pb.ReplicateRequest, compress bool) error {
	if r.ContentDelta != s.delta {
		return pb.NewValidationError("invalid ContentDelta (%d; expected %d)", r.ContentDelta, s.delta)
	}

	// Lazily open the Fragment.File.
	if s.Fragment.File == nil {
		if s.ContentLength() != 0 {
			panic("Spool.Fragment not empty.")
		}
		for {
			var err error
			if s.Fragment.File, err = newSpoolFile(); !retryUntil(err, "opening Spool file") {
				break
			}
		}
	}

	// Iff we're primary and compression is enabled, lazily initialize a compressor.
	if compress &&
		s.Fragment.CompressionCodec != pb.CompressionCodec_NONE &&
		s.compressedFile == nil &&
		s.ContentLength() == 0 {

		for {
			var err error
			if s.compressedFile, err = newSpoolFile(); !retryUntil(err, "opening Spool compressedFile") {
				break
			}
		}

		// Log warnings (rather than error) if we fail to initialize a spool file
		// or compressor, as we can continue to write to the uncompressed spool file.
		if file, err := newSpoolFile(); err != nil {
			log.WithField("err", err).Warn("failed to open compressed spool file")
		} else if compressor, err := codecs.NewCodecWriter(file, s.CompressionCodec); err != nil {
			log.WithFields(log.Fields{"err": err, "codec": s.Fragment.CompressionCodec}).
				Warn("failed to init compressor")

			_ = file.Close()
		} else {
			s.compressedFile = file
			s.compressor = compressor
		}
	}

	if n, err := s.Fragment.File.WriteAt(r.Content, s.ContentLength()+s.delta); err != nil {
		return err
	} else if _, err = s.summer.Write(r.Content); err != nil {
		panic("SHA1.Write cannot fail: " + err.Error())
	} else {
		s.delta += int64(n)
	}

	return nil
}

// finalize closes and releases resources associated with incremental Fragment
// building. If |invalidate| is true or an error is encountered, |compressedFile|
// is additionally closed and released, as we cannot guarantee that |compressor|
// did not mix and write un-flushed content from a prior commit with content from
// a failed commit.
func (s *Spool) finalizeCompressor(invalidate bool) {
	if s.compressor != nil {
		if err := s.compressor.Close(); err != nil {
			log.WithFields(log.Fields{"err": err}).Error("failed to Close compressor")
			invalidate = true
		} else if s.compressedSize, err = s.compressedFile.Seek(0, io.SeekEnd); err != nil {
			log.WithFields(log.Fields{"err": err}).Error("failed to Seek compressedFile")
			invalidate = true
		}
		s.compressor = nil
	}

	if invalidate && s.compressedFile != nil {
		if err := s.compressedFile.Close(); err != nil {
			log.WithFields(log.Fields{"err": err}).Error("failed to Close compressedFile")
		}
		s.compressedFile = nil
	}
}

// saveSumState marshals internal state of |summer| into |sumState|.
func (s *Spool) saveSumState() {
	if state, err := s.summer.(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
		panic(err.Error()) // Cannot fail.
	} else {
		s.sumState = state
	}
}

// restoreSumState unmarshals |sumState| into |summer|.
func (s *Spool) restoreSumState() {
	if err := s.summer.(encoding.BinaryUnmarshaler).UnmarshalBinary(s.sumState); err != nil {
		panic(err.Error()) // Cannot fail.
	}
}

var zeroedSHA1State, _ = sha1.New().(encoding.BinaryMarshaler).MarshalBinary()
