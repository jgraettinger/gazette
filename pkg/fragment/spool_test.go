package fragment

import (
	"errors"
	"io"
	"io/ioutil"
	"os"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/codecs"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type SpoolSuite struct{}

func (s *SpoolSuite) TestNextCases(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	// Case: Newly-initialized spool.
	c.Check(spool.Next(), gc.DeepEquals, pb.Fragment{
		Journal:          "a/journal",
		CompressionCodec: pb.CompressionCodec_NONE,
	})

	// Case: Zero-length fragment.
	var resp, _ = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            100,
			End:              100,
			CompressionCodec: pb.CompressionCodec_SNAPPY,
			BackingStore:     "s3://a-bucket",
		}})
	c.Check(resp.Status, gc.Equals, pb.Status_OK)

	c.Check(spool.Next(), gc.DeepEquals, pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              100,
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		BackingStore:     "s3://a-bucket",
	})

	// Case: Fragment with applied content, ready to be committed.
	var _, err = spool.Apply(&pb.ReplicateRequest{
		Content:      []byte("some"),
		ContentDelta: 0,
	})
	c.Check(err, gc.IsNil)

	_, err = spool.Apply(&pb.ReplicateRequest{
		Content:      []byte(" content"),
		ContentDelta: 4,
	})
	c.Check(err, gc.IsNil)

	c.Check(spool.Next(), gc.DeepEquals, pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              112,
		Sum:              pb.SHA1SumOf("some content"),
		CompressionCodec: pb.CompressionCodec_SNAPPY,
		BackingStore:     "s3://a-bucket",
	})
}

func (s *SpoolSuite) TestNoCompression(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	spool.Primary = true
	runReplicateSequence(c, &spool, pb.CompressionCodec_NONE)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(obv.commits, gc.HasLen, 2)

	c.Check(obv.completes[0].compressedFile, gc.IsNil)
	c.Check(obv.completes[0].compressor, gc.IsNil)

	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_NONE),
		gc.Equals, "an initial write final write")
}

func (s *SpoolSuite) TestCompressionAndPrimary(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	spool.Primary = true
	runReplicateSequence(c, &spool, pb.CompressionCodec_GZIP)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(obv.commits, gc.HasLen, 2)

	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_GZIP),
		gc.Equals, "an initial write final write")
}

func (s *SpoolSuite) TestCompressionNotPrimary(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	spool.Primary = false
	runReplicateSequence(c, &spool, pb.CompressionCodec_GZIP)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(obv.commits, gc.HasLen, 2)

	c.Check(obv.completes[0].compressedFile, gc.IsNil)
	c.Check(obv.completes[0].compressor, gc.IsNil)

	c.Check(contentString(c, obv.completes[0], pb.CompressionCodec_GZIP),
		gc.Equals, "an initial write final write")
}

func (s *SpoolSuite) TestRejectRollBeforeCurrentEnd(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(c, &spool, pb.CompressionCodec_NONE)

	// Expect offsets prior to the current End (28) fail.
	var resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            17 + 11 - 1,
			End:              17 + 11 - 1,
			CompressionCodec: pb.CompressionCodec_NONE,
		}})

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &spool.Fragment.Fragment,
	})
	c.Check(err, gc.IsNil)

	// Expect offsets beyond the current End succeed.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            17 + 11 + 1,
			End:              17 + 11 + 1,
			CompressionCodec: pb.CompressionCodec_NONE,
		}})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{Status: pb.Status_OK})
	c.Check(err, gc.IsNil)
}

func (s *SpoolSuite) TestRejectNextMismatch(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foobar")})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	// Incorrect End offset.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              5,
			Sum:              pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
			CompressionCodec: pb.CompressionCodec_NONE,
		}})

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &spool.Fragment.Fragment,
	})
	c.Check(err, gc.IsNil)

	// Incorrect SHA1 Sum.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              6,
			Sum:              pb.SHA1Sum{Part1: 0xFFFFFFFFFFFFFFFF, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
			CompressionCodec: pb.CompressionCodec_NONE,
		}})

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{
		Status:   pb.Status_FRAGMENT_MISMATCH,
		Fragment: &spool.Fragment.Fragment,
	})
	c.Check(err, gc.IsNil)

	// Correct Next Fragment.
	resp, err = spool.Apply(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              6,
			Sum:              pb.SHA1Sum{Part1: 0x8843d7f92416211d, Part2: 0xe9ebb963ff4ce281, Part3: 0x25932878},
			CompressionCodec: pb.CompressionCodec_NONE,
		}})

	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{Status: pb.Status_OK})
	c.Check(err, gc.IsNil)
}

func (s *SpoolSuite) TestCompressorErrorOnCommit(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	spool.Primary = true
	runReplicateSequence(c, &spool, pb.CompressionCodec_GZIP)

	var resp, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foobar")})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	c.Check(spool.compressor, gc.NotNil)
	c.Check(spool.compressedFile, gc.NotNil)

	spool.compressedFile.Close() // Force a |compressedFile| write error.

	var next = spool.Next()
	resp, err = spool.Apply(&pb.ReplicateRequest{Proposal: &next})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	// Expect compressor is invalidated, but content is still committed.
	c.Check(spool.compressor, gc.IsNil)
	c.Check(spool.compressedFile, gc.IsNil)

	c.Check(contentString(c, spool, pb.CompressionCodec_GZIP),
		gc.Equals, "foobar")
}

func (s *SpoolSuite) TestCompressorErrorOnFinalize(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	spool.Primary = true
	runReplicateSequence(c, &spool, pb.CompressionCodec_GZIP)

	var resp, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foobar")})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	// Commit "foobar".
	var next = spool.Next()
	resp, err = spool.Apply(&pb.ReplicateRequest{Proposal: &next})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	spool.compressedFile.Close() // Force a |compressedFile| write error.

	// Roll to a new Spool Fragment.
	next.Begin, next.Sum = next.End, pb.SHA1Sum{}
	resp, err = spool.Apply(&pb.ReplicateRequest{Proposal: &next})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	// We now see the prior Spool as completed. Expect its |compressedFile|
	// was cleared, but content was still committed and is readable.
	c.Check(obv.completes, gc.HasLen, 2)
	c.Check(obv.completes[1].compressedFile, gc.IsNil)

	c.Check(contentString(c, obv.completes[1], pb.CompressionCodec_GZIP),
		gc.Equals, "foobar")
}

func (s *SpoolSuite) TestStreamedCompressorReadErr(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)
	runReplicateSequence(c, &spool, pb.CompressionCodec_GZIP)

	c.Check(obv.completes, gc.HasLen, 1)
	c.Check(obv.completes[0].compressedFile, gc.IsNil)

	obv.completes[0].Fragment.File.Close() // Force read error.

	var zrc = obv.completes[0].CodecReader()
	var _, err = io.Copy(ioutil.Discard, zrc)

	// Expect the underlying file read error was propagated through the pipe.
	c.Check(err, gc.ErrorMatches, `read .*: file already closed`)
	c.Check(zrc.Close(), gc.IsNil)
}

func (s *SpoolSuite) TestContentDeltaMismatch(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 0,
		Content:      []byte("foo"),
	})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	_, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 2,
		Content:      []byte("bar"),
	})
	c.Check(err, gc.ErrorMatches, `invalid ContentDelta \(2; expected 3\)`)
}

func (s *SpoolSuite) TestSpoolFileWriteError(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	var resp, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 0,
		Content:      []byte("foo"),
	})
	c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{})
	c.Check(err, gc.IsNil)

	spool.Fragment.File.Close() // Force a write error.

	// Spool file errors are unrecoverable; expect its surfaced via |err|.
	_, err = spool.Apply(&pb.ReplicateRequest{
		ContentDelta: 3,
		Content:      []byte("bar"),
	})
	c.Check(err, gc.ErrorMatches, `write .*: file already closed`)
}

func (s *SpoolSuite) TestFailToOpenSpoolFile(c *gc.C) {
	var obv testSpoolObserver
	var spool = NewSpool("a/journal", &obv)

	// Temporarily swap out |newSpoolFile| with a failing fixture.
	defer func(f func() (*os.File, error)) { newSpoolFile = f }(newSpoolFile)
	newSpoolFile = func() (*os.File, error) { return nil, errors.New("spool open error") }

	// Spool file errors are unrecoverable; expect its surfaced via |err|.
	var _, err = spool.Apply(&pb.ReplicateRequest{Content: []byte("foobar")})
	c.Check(err, gc.ErrorMatches, `spool open error`)
}

func contentString(c *gc.C, s Spool, codec pb.CompressionCodec) string {
	var zrc = s.CodecReader()

	var rc, err = codecs.NewCodecReader(zrc, codec)
	c.Assert(err, gc.IsNil)

	b, err := ioutil.ReadAll(rc)
	c.Check(err, gc.IsNil)

	c.Check(rc.Close(), gc.IsNil)
	c.Check(zrc.Close(), gc.IsNil)

	return string(b)
}

func runReplicateSequence(c *gc.C, s *Spool, codec pb.CompressionCodec) {
	var seq = []pb.ReplicateRequest{
		// Commit 0 (roll spool).
		{Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              0,
			Sum:              pb.SHA1Sum{},
			CompressionCodec: codec,
			BackingStore:     "s3://a-bucket",
		}},
		{
			Content:      []byte("an init"),
			ContentDelta: 0,
		},
		{
			Content:      []byte("ial write "),
			ContentDelta: 7,
		},
		// Commit 1: "an initial write"
		{Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              17,
			Sum:              pb.SHA1Sum{Part1: 0x2fb7dcccaa048a26, Part2: 0xaa3f3a6205a4ea6d, Part3: 0xfc0636e6},
			CompressionCodec: codec,
			BackingStore:     "s3://a-bucket",
		}},
		// Content which is rolled back.
		{
			Content:      []byte("WHO"),
			ContentDelta: 0,
		},
		{
			Content:      []byte("OPS!"),
			ContentDelta: 3,
		},
		// Roll back to commit 1.
		{Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              17,
			Sum:              pb.SHA1Sum{Part1: 0x2fb7dcccaa048a26, Part2: 0xaa3f3a6205a4ea6d, Part3: 0xfc0636e6},
			CompressionCodec: codec,
			BackingStore:     "s3://a-bucket",
		}},
		{
			Content:      []byte("final write"),
			ContentDelta: 0,
		},
		// Commit 2: "final write"
		{Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            0,
			End:              17 + 11,
			Sum:              pb.SHA1Sum{Part1: 0x61c71d3ba5e95d5d, Part2: 0xdbfc254ba7708df9, Part3: 0x5aeb0169},
			CompressionCodec: codec,
			BackingStore:     "s3://a-bucket",
		}},
		// Content which is streamed but never committed.
		{
			Content:      []byte("extra "),
			ContentDelta: 0,
		},
		{
			Content:      []byte("partial content"),
			ContentDelta: 6,
		},
		// Commit 3: roll spool forward, completing prior spool.
		{Proposal: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            17 + 11,
			End:              17 + 11,
			Sum:              pb.SHA1Sum{},
			CompressionCodec: codec,
			BackingStore:     "s3://a-bucket",
		}},
	}
	for _, req := range seq {
		var resp, err = s.Apply(&req)
		c.Check(err, gc.IsNil)
		c.Check(resp, gc.DeepEquals, pb.ReplicateResponse{Status: pb.Status_OK})
	}
}

type testSpoolObserver struct {
	commits   []Fragment
	completes []Spool
}

func (o *testSpoolObserver) SpoolCommit(f Fragment) { o.commits = append(o.commits, f) }
func (o *testSpoolObserver) SpoolComplete(s Spool)  { o.completes = append(o.completes, s) }

var _ = gc.Suite(&SpoolSuite{})
