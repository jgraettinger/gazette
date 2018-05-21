package fragment

/*
import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type SpoolSuite struct {
	localDir string
}

func (s *SpoolSuite) SetUpTest(c *gc.C) {
	var err error
	s.localDir, err = ioutil.TempDir("", "spool-suite")
	c.Assert(err, gc.IsNil)
}

func (s *SpoolSuite) TearDownTest(c *gc.C) {
	os.RemoveAll(s.localDir)
}

func (s *SpoolSuite) TestMultipleWriteAndCommitFixture(c *gc.C) {
	var spool, err = newSpool(s.localDir, "journal/name", 12345)
	c.Check(err, gc.IsNil)

	var path1 = filepath.Join(s.localDir, "journal/name/"+
		"0000000000003039-0000000000003039-0000000000000000000000000000000000000000.raw")

	_, err = os.Stat(path1)
	c.Check(err, gc.IsNil)

	n, err := spool.Write([]byte("an initial write"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 16)
	c.Check(spool.delta, gc.Equals, int64(16))

	n, err = spool.Write([]byte("another write"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 13)
	c.Check(spool.delta, gc.Equals, int64(16+13))

	// Commit a partial portion of the two writes. Offsets and checksum
	// should reflect just the committed portion.
	c.Check(spool.commit(20), gc.IsNil)
	c.Check(spool.End, gc.Equals, int64(12345+20))
	c.Check(spool.delta, gc.Equals, int64(0))
	c.Check(spool.Sum, gc.DeepEquals, pb.SHA1Sum{
		Part1: 0xe38af7da34a33b5a, Part2: 0x882d9ccd2aca4c7f, Part3: 0xca153b41})

	var path2 = filepath.Join(s.localDir, "journal/name/"+
		"0000000000003039-000000000000304d-e38af7da34a33b5a882d9ccd2aca4c7fca153b41.raw")
	c.Check(spool.localPath(), gc.Equals, path2)

	_, err = os.Stat(path1)
	c.Check(os.IsNotExist(err), gc.Equals, true)
	_, err = os.Stat(path2)
	c.Check(err, gc.IsNil)

	// A Write which is fully aborted.
	n, err = spool.Write([]byte("WHOOPS"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 6)
	c.Check(spool.delta, gc.Equals, int64(6))

	// Verify the end and checksum are unchanged after abort.
	c.Check(spool.commit(0), gc.IsNil)
	c.Check(spool.End, gc.Equals, int64(12345+20))
	c.Check(spool.delta, gc.Equals, int64(0))
	c.Check(spool.Sum, gc.DeepEquals, pb.SHA1Sum{
		Part1: 0xe38af7da34a33b5a, Part2: 0x882d9ccd2aca4c7f, Part3: 0xca153b41})

	// A final Write and nCommit. Verify it starts at the previous nCommit end.
	_, err = spool.Write([]byte("a final write"))
	c.Check(err, gc.IsNil)
	c.Check(spool.delta, gc.Equals, int64(13))

	c.Check(spool.commit(13), gc.IsNil)
	c.Check(spool.End, gc.Equals, int64(12345+20+13))
	c.Check(spool.Sum, gc.DeepEquals, pb.SHA1Sum{
		Part1: 0x033ccc6d15aa3332, Part2: 0x24ec965337ba2cf2, Part3: 0x329b9142})

	var path3 = filepath.Join(s.localDir, "journal/name/"+
		"0000000000003039-000000000000305a-033ccc6d15aa333224ec965337ba2cf2329b9142.raw")
	c.Check(spool.localPath(), gc.Equals, path3)

	_, err = os.Stat(path1)
	c.Check(os.IsNotExist(err), gc.Equals, true)
	_, err = os.Stat(path2)
	c.Check(os.IsNotExist(err), gc.Equals, true)
	_, err = os.Stat(path3)
	c.Check(err, gc.IsNil)

	content, _ := ioutil.ReadFile(path3)
	c.Check(string(content), gc.Equals, "an initial writeanota final write")
}

func (s *SpoolSuite) TestFixtureChecksumEquivalence(c *gc.C) {
	spool, err := newSpool(s.localDir, "journal/name", 12345)
	c.Check(err, gc.IsNil)

	// Write equivalent data to TestCommitFlow in a single Write and transaction.
	// Verify the same resulting checksum and sizes as that test.
	n, err := spool.Write([]byte("an initial writeanota final write"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 33)
	c.Check(spool.commit(33), gc.IsNil)

	path := filepath.Join(s.localDir, "journal", "name",
		"0000000000003039-000000000000305a-033ccc6d15aa333224ec965337ba2cf2329b9142.raw")
	c.Check(spool.localPath(), gc.Equals, path)
}

func (s *SpoolSuite) TestWriteAndCommitSequence(c *gc.C) {
	spool, err := newSpool(s.localDir, "journal/name", 12345)
	c.Check(err, gc.IsNil)

	var expect bytes.Buffer
	for _, i := range rand.Perm(255) {
		buffer := bytes.Repeat([]byte{byte(i)}, i+10)

		n, err := spool.Write(buffer)
		c.Check(err, gc.IsNil)
		c.Check(n, gc.Equals, i+10)

		expect.Write(buffer[:i])
		c.Check(spool.commit(int64(i)), gc.Equals, nil)
	}

	var actual bytes.Buffer
	var r = fragment.FileReaderFromOffset(spool.Fragment, spool.Begin)
	io.Copy(&actual, r)
	c.Check(actual.Bytes(), gc.DeepEquals, expect.Bytes())
}

func (s *SpoolSuite) TestWriteErrorHandling(c *gc.C) {
	var spool, err = newSpool(s.localDir, "journal/name", 12345)
	c.Check(err, gc.IsNil)

	spool.Write([]byte("initial nCommit"))
	c.Check(spool.commit(14), gc.IsNil)
	contentPath := spool.ContentPath()

	spool.Write([]byte("first write"))
	spool.File.Close() // Close out from under, such that writes fail.

	n, err := spool.Write([]byte("failed write"))
	c.Check(err, gc.Not(gc.IsNil))
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, spool.err)

	// |err| continues to be returned by |Write| and |nCommit|
	_, err = spool.Write([]byte("another failed write"))
	c.Check(err, gc.Equals, spool.err)
	c.Check(spool.commit(5), gc.Equals, spool.err)

	c.Check(spool.ContentPath(), gc.Equals, contentPath) // No change.
}

func (s *SpoolSuite) TestCommitErrorHandling(c *gc.C) {
	var spool, err = newSpool(s.localDir, "journal/name", 12345)
	c.Check(err, gc.IsNil)

	spool.Write([]byte("initial nCommit"))
	c.Check(spool.commit(14), gc.IsNil)
	contentPath := spool.ContentPath()

	spool.Write([]byte("first write"))
	spool.File.Close() // Close out from under, such that writes fail.

	err = spool.commit(5)
	c.Check(err, gc.Not(gc.IsNil))
	c.Check(err, gc.Equals, spool.err)

	c.Check(spool.ContentPath(), gc.Equals, contentPath) // No change.
}

/*
func (s *SpoolSuite) TestPersistence(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)

	c.Assert(spool.Write([]byte("committed content")), gc.IsNil)
	c.Assert(spool.Commit(), gc.IsNil)
	c.Assert(spool.Write([]byte("extra non-persisted write")), gc.IsNil)

	_, err := os.Stat(spool.LocalPath())
	c.Assert(err, gc.IsNil) // Precondition: local file exists.

	context := &MockStorageContext{}
	c.Assert(spool.Persist(context), gc.IsNil)
	spool.Delete()

	_, err = os.Stat(spool.LocalPath())
	c.Assert(os.IsNotExist(err), gc.Equals, true) // File removed.

	c.Assert(context.RecordedCreate[0].Name, gc.Equals, spool.ContentPath())
	c.Assert(context.RecordedCreate[0].ContentEncoding, gc.Equals, "gzip")
	c.Assert(context.RecordedWrites.Bytes(), gc.DeepEquals,
		gzipped("committed content"))
}

func (s *SpoolSuite) TestUploadErrorHandling(c *gc.C) {
	spool := NewSpool(s.localDir, "journal/name", 12345)
	c.Assert(spool.Write([]byte("committed content")), gc.IsNil)
	c.Assert(spool.Commit(), gc.IsNil)

	err := errors.New("error!")
	{
		context := &MockStorageContext{}
		context.CreateReturn = err
		c.Assert(spool.Persist(context), gc.DeepEquals, err)
	}
	{
		context := &MockStorageContext{}
		context.WriteReturn = err
		c.Assert(spool.Persist(context), gc.DeepEquals, err)
	}
	{
		context := &MockStorageContext{}
		context.CloseReturn = err
		c.Assert(spool.Persist(context), gc.DeepEquals, err)
	}
	_, statErr := os.Stat(spool.LocalPath())
	c.Assert(statErr, gc.IsNil) // Local file still exists.
	{
		context := &MockStorageContext{}
		c.Assert(spool.Persist(context), gc.IsNil) // Successful upload.
		spool.Delete()
	}
	_, statErr = os.Stat(spool.LocalPath())
	c.Assert(os.IsNotExist(statErr), gc.Equals, true) // Was removed.
}

func (s *SpoolSuite) TestSpoolRecovery(c *gc.C) {
	var fixture1, fixture2 string
	{
		spool1 := NewSpool(s.localDir, "journal/name", 12345)
		c.Assert(spool1.Write([]byte("fixture one content")), gc.IsNil)
		c.Assert(spool1.Commit(), gc.IsNil)

		spool2 := NewSpool(s.localDir, "journal/name", spool1.LastCommit)
		c.Assert(spool2.Write([]byte("fixture two content")), gc.IsNil)
		c.Assert(spool2.Commit(), gc.IsNil)

		fixture1 = spool1.ContentPath()
		fixture2 = spool2.ContentPath()

		spool1.backingFile.Close() // Close out from under to release lock.
		spool2.backingFile.Close()
	}
	spool3 := NewSpool(s.localDir, "journal/name", 56789)
	c.Assert(spool3.Write([]byte("fixture three")), gc.IsNil)
	c.Assert(spool3.Commit(), gc.IsNil)

	recovered := RecoverSpools(s.localDir)

	c.Assert(recovered, gc.HasLen, 2)
	c.Assert(recovered[0].ContentPath(), gc.Equals, fixture1)
	c.Assert(recovered[1].ContentPath(), gc.Equals, fixture2)

	context := &MockStorageContext{}
	c.Assert(recovered[0].Persist(context), gc.IsNil)
	c.Assert(context.RecordedWrites.Bytes(), gc.DeepEquals,
		gzipped("fixture one content"))
}

func gzipped(content string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write([]byte(content))
	w.Close()
	return buf.Bytes()
}

var _ = gc.Suite(&SpoolSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
*/
