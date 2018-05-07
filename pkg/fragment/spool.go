package fragment

import (
	"crypto/sha1"
	"encoding"
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

	nextEnd    int64
	nextSummer hash.Hash
	// SHA1State is the binary marshalling of the state of a SHA1 hash.Hash
	// at the completion of the last commit.
	sha1State []byte

	// Compressed form of the Fragment, compressed under Fragment.CompressionCodec.
	// This field is somewhat speculative; only one broker (eg, the primary)
	// will compress writes as they commit. Under normal operation, this allows
	// completed spools to be quickly & cheaply persisted to the backing store.
	// Should a failure occur, other brokers can construct the compressed form
	// on-demand from the backing Fragment.File.
	CompressedFile *os.File
	// Compressor of |compressedFile|.
	Compressor codecs.Compressor
}

// Roll closes and (if a store is configured) persists the Spool, and then
// "rolls" it forward to a zero-length Fragment at |offset|. The Spool must
// be opened before use. |offset| must not be less than the current Fragment
// End, or Roll panics. Also, |spec| must not reference a different Journal
// than that of the Spool, or Roll panics.
func (s *Spool) Roll(spec *pb.JournalSpec, offset int64) {
	if offset < s.Fragment.End {
		panic("invalid offset")
	} else if s.Fragment.Journal != "" && s.Fragment.Journal != spec.Name {
		panic("invalid spec Name")
	}

	var prev = *s
	prev.finalize(false)

	if prev.ContentLength() != 0 && len(spec.Fragment.Stores) != 0 {
		prev.BackingStore = spec.Fragment.Stores[0]
		go Store.Persist(prev)
	}

	*s = Spool{
		Fragment: Fragment{
			Fragment: pb.Fragment{
				Journal:          spec.Name,
				Begin:            offset,
				End:              offset,
				CompressionCodec: spec.Fragment.CompressionCodec,
			},
		},
	}
	return
}

// Open a Spool by creating a backing file. The Spool must be length-zero and
// not already open, or Open panics.
func (s *Spool) Open() error {
	if s.File != nil {
		panic("Spool.Open already called.")
	} else if s.ContentLength() != 0 {
		panic("Spool.Fragment not empty.")
	}

	var file, err = newSpoolFile()

	if err == nil {
		s.Fragment.File = file
		s.SHA1Summer = sha1.New()
		s.SHA1State, err = s.SHA1Summer.(encoding.BinaryMarshaler).MarshalBinary()
		s.Next = s.Fragment
	}
	return err
}

// InitCompressor prepares the Spool for incremental compression by creating a
// backing file and compression codec. The Spool must already be open, be zero-
// length, and not already initialized for compression.
func (s *Spool) InitCompressor() error {
	if s.File == nil {
		panic("Spool.Open must be called first")
	} else if s.CompressedFile != nil {
		panic("Spool.InitCompressor already called.")
	} else if s.ContentLength() != 0 {
		panic("Spool.Fragment not empty.")
	}

	if file, err := newSpoolFile(); err != nil {
		return err
	} else if compressor, err := codecs.NewCodecWriter(file, s.CompressionCodec); err != nil {
		_ = file.Close()
		return err
	} else {
		s.CompressedFile = file
		s.Compressor = compressor
		return nil
	}
}

func (s *Spool) Append(p []byte) error {

}

func (s *Spool) Rollback() {

}

// Next returns the Fragment which will be created by the next Commit.
func (s *Spool) Next() pb.Fragment {}

// Commit records |delta| new bytes beyond the current Fragment End, and
// already written to the backing Fragment.File, to the Spool. The Spool
// Fragment is updated with the new SHA1 sum and offset extent, and the
// committed content is optionally compressed. A returned error aborts the
// Commit, and callers should handle by closing the current Spool and not
// attempting to re-use it.
func (s *Spool) Commit() error {
	// Build a reader over the portion to be committed this transaction.
	var sr = io.NewSectionReader(s.File, s.ContentLength(), delta)
	var err error

	if s.Compressor == nil {
		// Run the |delta| through SHA1Summer (only).
		_, err = io.Copy(s.SHA1Summer, sr)
	} else {
		// Run the |delta| through both SHA1Summer and the Compressor.
		_, err = io.Copy(s.SHA1Summer, io.TeeReader(sr, s.Compressor))
	}

	if err != nil {
		// This error invalidates the Spool, as partial content has been read by
		// SHA1Summer and (if set) the Compressor.
		s.finalize(true)
		return err
	}

	s.Sum = pb.SHA1SumFromDigest(s.SHA1Summer.Sum(nil))
	s.End += delta
	return nil
}

// finalize closes and releases resources associated with incremental Fragment
// building. If |invalidate| is true or an error is encountered, |CompressedFile|
// is additionally closed and released, as we cannot guarantee that |Compressor|
// did not mix and write un-flushed content from a prior commit with content from
// a failed commit.
func (s *Spool) finalize(invalidate bool) {
	s.SHA1Summer = nil

	if s.Compressor != nil {
		if err := s.Compressor.Close(); err != nil {
			log.WithFields(log.Fields{"err": err}).Error("failed to Close compressor")
			invalidate = true
		}
		s.Compressor = nil
	}

	if invalidate && s.CompressedFile != nil {
		if err := s.CompressedFile.Close(); err != nil {
			log.WithFields(log.Fields{"err": err}).Error("failed to Close compressedFile")
		}
		s.CompressedFile = nil
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
