package fragment

import (
	"crypto/sha1"
	"encoding"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
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
	// Should the Spool proactively compress content?
	EnableCompression bool

	// Compressed form of the Fragment, compressed under Fragment.CompressionCodec.
	// This field is somewhat speculative; only one broker (eg, the primary)
	// will compress writes as they commit. Under normal operation, this allows
	// completed spools to be quickly & cheaply persisted to the backing store.
	// Should a failure occur, other brokers can construct the compressed form
	// on-demand from the backing Fragment.File.
	compressedFile *os.File
	// Compressor of |compressedFile|.
	compressor codecs.Compressor

	delta    int64     // Delta offset of next byte to write, relative to Fragment.End.
	summer   hash.Hash // Running SHA1 of the Fragment.File, through |offset|.
	sumState []byte    // SHA1 |summer| internal state at the last Fragment commit.
	index    *Index    // Index into which committed, local Fragments are advertised.
}

// NewSpool returns an empty Spool of |journal|.
func NewSpool(journal pb.Journal, index *Index) Spool {
	return Spool{
		Fragment: Fragment{Fragment: pb.Fragment{Journal: journal}},
		index:    index,
		summer:   sha1.New(),
		sumState: zeroedSHA1State,
	}
}

// Next returns the next Fragment which can be committed by the Spool.
func (s *Spool) Next() pb.Fragment {
	var f = s.Fragment.Fragment
	f.End += s.delta
	f.Sum = pb.SHA1SumFromDigest(s.summer.Sum(nil))
	return f
}

// Apply the ReplicateRequest to the Spool, returning any encountered error.
func (s *Spool) Apply(r *pb.ReplicateRequest) (pb.ReplicateResponse, error) {
	if r.Proposal != nil {
		return s.applyCommit(r), nil
	} else {
		return pb.ReplicateResponse{}, s.applyContent(r)
	}
}

func (s *Spool) applyCommit(r *pb.ReplicateRequest) pb.ReplicateResponse {
	// There are three allowed commit cases:
	//  1) Exact commit of current fragment.
	//  2) Exact commit of current fragment, extended by |delta|.
	//  3) Trivial commit of an empty Fragment at or beyond the current Fragment.End.

	// Case 1? "Undo" any partial content, by rolling back |delta| and |summer|.
	if s.Fragment.Fragment == *r.Proposal {
		s.delta = 0
		s.restoreSumState()
		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	// Case 2? Apply the |delta| bytes spooled since last commit.
	if s.Next() == *r.Proposal {

		if s.compressor != nil {
			// Build a reader over the new content, and run it through the Compressor.
			if _, err := io.Copy(s.compressor,
				io.NewSectionReader(s.File, s.Fragment.ContentLength(), s.delta)); err != nil {

				// |err| invalidates the compressor but does not fail the commit.
				s.finalizeCompressor(true)
				log.WithFields(log.Fields{"proposal": *r.Proposal, "err": err}).Error("failed to compress")
			}
		}

		s.Fragment.End += s.delta
		s.Fragment.Sum = pb.SHA1SumFromDigest(s.summer.Sum(nil))
		s.index.addLocal(s.Fragment)

		s.delta = 0
		s.saveSumState()
		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	// Case 3? Persist the current Fragment, and re-initialize to the new one.
	if r.Proposal.Journal == s.Fragment.Journal &&
		r.Proposal.Begin >= s.Fragment.End &&
		r.Proposal.ContentLength() == 0 &&
		r.Proposal.Sum.IsZero() {

		s.finalizeCompressor(false)
		if s.ContentLength() != 0 && s.Fragment.BackingStore != "" {
			go Store.Persist(*s)
		}

		*s = Spool{
			Fragment: Fragment{
				Fragment: *r.Proposal,
			},
			index:    s.index,
			summer:   sha1.New(),
			sumState: zeroedSHA1State,
		}
		return pb.ReplicateResponse{Status: pb.Status_OK}
	}

	return pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &s.Fragment.Fragment,
	}
}

func (s *Spool) applyContent(r *pb.ReplicateRequest) error {
	if r.ContentDelta != s.delta {
		return fmt.Errorf("invalid ContentDelta %d (expected %d)", r.ContentDelta, s.delta)
	}

	if s.Fragment.File == nil {
		if s.ContentLength() != 0 {
			panic("Spool.Fragment not empty.")
		}

		if file, err := newSpoolFile(); err != nil {
			return err
		} else {
			s.Fragment.File = file
		}
	}

	if s.EnableCompression && s.compressedFile == nil && s.ContentLength() == 0 {
		if file, err := newSpoolFile(); err != nil {
			return err
		} else if compressor, err := codecs.NewCodecWriter(file, s.CompressionCodec); err != nil {
			_ = file.Close()
			return err
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

// newSpoolFile creates and returns a temporary file which has already had its
// one-and-only hard link removed from the file system. So long as the *os.File
// remains open, the OS will defer collecting the allocated inode and reclaiming
// disk resources, but the file becomes unaddressable and its resources released
// to the OS after an explicit call to Close, or if the os.File is garbage-
// collected (such that the runtime finalizer calls Close on our behalf).
func newSpoolFile() (*os.File, error) {
	if f, err := ioutil.TempFile("", "spool"); err != nil {
		return f, err
	} else {
		return f, os.Remove(f.Name())
	}
}

var zeroedSHA1State, _ = sha1.New().(encoding.BinaryMarshaler).MarshalBinary()
