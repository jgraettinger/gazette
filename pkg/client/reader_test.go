package client

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	gc "github.com/go-check/check"
	"github.com/pkg/errors"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	"github.com/LiveRamp/gazette/pkg/codecs"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type ReaderSuite struct{}

func (s *ReaderSuite) TestOpenFragmentURLCases(c *gc.C) {
	var frag, url, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer installFileClient()()

	var ctx = context.Background()

	// Case: read entire fragment.
	var rc, err = OpenFragmentURL(ctx, frag.Begin, frag, url)
	c.Check(err, gc.IsNil)

	b, err := ioutil.ReadAll(rc)
	c.Check(err, gc.IsNil)
	c.Check(string(b), gc.Equals, "XXXXXhello, world!!!")

	// Case: read a portion of the fragment.
	rc, err = OpenFragmentURL(ctx, frag.Begin+5, frag, url)
	c.Check(err, gc.IsNil)

	b, err = ioutil.ReadAll(rc)
	c.Check(err, gc.IsNil)
	c.Check(string(b), gc.Equals, "hello, world!!!")

	// Case: stream ends before Fragment.End.
	frag.End += 1
	rc, err = OpenFragmentURL(ctx, frag.Begin+5, frag, url)
	c.Check(err, gc.IsNil)

	b, err = ioutil.ReadAll(rc)
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)
	c.Check(string(b), gc.Equals, "hello, world!!!")

	// Case: stream continues after Fragment.End.
	frag.End -= 4
	rc, err = OpenFragmentURL(ctx, frag.Begin+5, frag, url)
	c.Check(err, gc.IsNil)

	b, err = ioutil.ReadAll(rc)
	c.Check(err, gc.Equals, ErrExpectedEOF)
	c.Check(string(b), gc.Equals, "hello, world")

	// Case: decompression fails.
	frag.CompressionCodec = pb.CompressionCodec_SNAPPY
	rc, err = OpenFragmentURL(ctx, frag.Begin+5, frag, url)
	c.Check(err, gc.ErrorMatches, `error seeking fragment \(snappy: corrupt input.*`)
}

func (s *ReaderSuite) TestReaderCases(c *gc.C) {
	var frag, url, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer installFileClient()()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	go serveReadFixtures(c, broker,
		// Case 1: fixture returns fragment metadata & URL, then EOF.
		readFixture{fragment: &frag, fragmentUrl: url},
		// Case 2: fragment metadata but no URL.
		readFixture{fragment: &frag},
		// Case 3: wrong broker (and it's not instructed to proxy).
		readFixture{status: pb.Status_NOT_JOURNAL_BROKER},
		// Case 4: read some content, and then OFFSET_NOT_YET_AVAILABLE.
		readFixture{content: "prior content", status: pb.Status_OFFSET_NOT_YET_AVAILABLE},
		// Case 5: immediate OFFSET_NOT_YET_AVAILABLE.
		readFixture{status: pb.Status_OFFSET_NOT_YET_AVAILABLE},
		// Case 6: Status not explicitly mapped to an error.
		readFixture{status: pb.Status_INSUFFICIENT_JOURNAL_BROKERS},
		// Case 7: streamed content, with response offset jump 105 => 110.
		readFixture{content: "foo bar baz bing", offset: 110},
		// Case 8: partial streamed content, then a non-EOF error.
		readFixture{content: "partial read", err: errors.New("potato")},
		// Case 9: invalid offset jump 105 => 100.
		readFixture{content: "invalid", offset: 100},
		// Case 10: ReadResponse validation fails (invalid Fragment).
		readFixture{fragment: &pb.Fragment{End: 1, Begin: 0}},
	)

	// Case 1: fragment metadata & URL.
	var r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal", Offset: 105})
	var b, err = ioutil.ReadAll(r)

	// Expect fragment URL fixture is opened & read directly.
	c.Check(string(b), gc.Equals, "hello, world!!!")
	c.Check(err, gc.Equals, nil)

	// Case 2: fragment metadata, no URL.
	r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal", Offset: 105})
	b, err = ioutil.ReadAll(r)

	c.Check(b, gc.HasLen, 0)
	c.Check(err, gc.Equals, nil)
	c.Check(r.Response.Fragment, gc.DeepEquals, &frag)

	// Case 3: NOT_JOURNAL_BROKER => ErrNotJournalBroker.
	r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal", Offset: 105})
	b, err = ioutil.ReadAll(r)

	c.Check(b, gc.HasLen, 0)
	c.Check(err, gc.Equals, ErrNotJournalBroker)

	// Case 4: read content, then OFFSET_NOT_YET_AVAILABLE => ErrOffsetNotYetAvailable.
	r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal", Offset: 105})
	b, err = ioutil.ReadAll(r)

	c.Check(string(b), gc.Equals, "prior content")
	c.Check(err, gc.Equals, ErrOffsetNotYetAvailable)

	// Case 5: immediate OFFSET_NOT_YET_AVAILABLE => ErrOffsetNotYetAvailable.
	r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal", Offset: 105})
	b, err = ioutil.ReadAll(r)

	c.Check(b, gc.HasLen, 0)
	c.Check(err, gc.Equals, ErrOffsetNotYetAvailable)

	// Case 6: expect status is dynamically mapped to error.
	r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal", Offset: 105})
	b, err = ioutil.ReadAll(r)

	c.Check(b, gc.HasLen, 0)
	c.Check(err, gc.ErrorMatches, pb.Status_INSUFFICIENT_JOURNAL_BROKERS.String())

	// Case 7: streamed content with multiple small reads, offset jump & tracked Route update
	var routes = make(map[pb.Journal]*pb.Route)
	r = NewReader(ctx, routeWrapper{broker.MustClient(), routes},
		pb.ReadRequest{Journal: "a/journal", Offset: 105})

	_, err = r.Read(nil)
	c.Check(err, gc.IsNil)

	// Expect offset was skipped forward to Response.Offset.
	c.Check(r.Request.Offset, gc.Equals, int64(110))

	// Expect Reader advised of the updated Route.
	c.Check(routes["a/journal"], gc.DeepEquals, &pb.Route{
		Brokers: []pb.BrokerSpec_ID{{Zone: "a", Suffix: "broker"}},
		Primary: 0,
	})

	// Note the fixture content is split across two ReadResponses.
	// Consume both messages across four small Reads.
	b = make([]byte, 6)
	n, err := r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "foo ba")
	c.Check(r.Request.Offset, gc.Equals, int64(116))

	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "r ")
	c.Check(r.Request.Offset, gc.Equals, int64(118))

	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "baz bi")
	c.Check(r.Request.Offset, gc.Equals, int64(124))

	n, err = r.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "ng")
	c.Check(r.Request.Offset, gc.Equals, int64(126))

	// Case 8: content followed by a non-EOF error.
	r = NewReader(ctx, routeWrapper{broker.MustClient(), routes},
		pb.ReadRequest{Journal: "a/journal", Offset: 105})

	b, err = ioutil.ReadAll(r)
	c.Check(string(b), gc.Equals, "partial read")
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = potato`)

	// Expect Reader purged the Route advisement due to non-EOF close.
	c.Check(routes["a/journal"], gc.IsNil)

	// Case 9: invalid offset jump 105 => 100.
	delete(routes, "a/journal")

	r = NewReader(ctx, routeWrapper{broker.MustClient(), routes},
		pb.ReadRequest{Journal: "a/journal", Offset: 105})

	b, err = ioutil.ReadAll(r)
	c.Check(b, gc.HasLen, 0)
	c.Check(err, gc.ErrorMatches, `invalid ReadResponse offset \(100; expected >= 105\)`)

	// Expect Reader purged the Route advisement due to non-EOF close.
	c.Check(routes, gc.HasLen, 1) // Nil Route was re-added.
	c.Check(routes["a/journal"], gc.IsNil)

	// Case 10: ReadResponse validation fails (invalid fragment).
	delete(routes, "a/journal")

	r = NewReader(ctx, routeWrapper{broker.MustClient(), routes},
		pb.ReadRequest{Journal: "a/journal", Offset: 105})

	b, err = ioutil.ReadAll(r)
	c.Check(b, gc.HasLen, 0)
	c.Check(err, gc.ErrorMatches, `ReadResponse.Fragment.Journal: invalid length .*`)

	// Expect Reader purged the Route advisement due to non-EOF close.
	c.Check(routes, gc.HasLen, 1) // Nil Route was re-added.
	c.Check(routes["a/journal"], gc.IsNil)

}

func (s *ReaderSuite) TestBufferedOffsetAdjustment(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	go readFixture{content: "foobar\nbaz\n", offset: 100}.serve(c, broker)

	var r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal"})
	var br = bufio.NewReader(r)

	var b, err = br.ReadBytes('\n')
	c.Check(string(b), gc.Equals, "foobar\n")
	c.Check(err, gc.IsNil)

	// Expect the entire input reader was consumed.
	c.Check(r.Request.Offset, gc.Equals, int64(100+7+4))
	// Expect the adjusted mark reflects just the portion read from |br|.
	c.Check(r.AdjustedOffset(br), gc.Equals, int64(100+7))
}

func (s *ReaderSuite) TestReaderSeekCases(c *gc.C) {
	var frag, url, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer installFileClient()()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	// Start read fixture which returns fragment metadata & URL, then EOF,
	// causing Reader to directly open the fragment.
	go readFixture{fragment: &frag, fragmentUrl: url, offset: frag.Begin}.serve(c, broker)

	var r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal"})

	// Zero-byte read causes Reader to open Fragment.
	var _, err = r.Read(nil)
	c.Check(err, gc.IsNil)
	c.Check(r.direct, gc.NotNil)
	c.Check(r.Request.Offset, gc.Equals, frag.Begin)

	// Case: seeking forward works, so long as the Fragment covers the seek'd offset.
	offset, err := r.Seek(5, io.SeekCurrent)
	c.Check(offset, gc.Equals, frag.Begin+5)
	c.Check(err, gc.IsNil)

	offset, err = r.Seek(frag.Begin+6, io.SeekStart)
	c.Check(offset, gc.Equals, frag.Begin+6)
	c.Check(err, gc.IsNil)

	// Case: seeking backwards requires a new reader.
	offset, err = r.Seek(-1, io.SeekCurrent)
	c.Check(offset, gc.Equals, frag.Begin+6)
	c.Check(err, gc.Equals, ErrSeekRequiresNewReader)

	// Case: as does seeking beyond the current fragment End.
	offset, err = r.Seek(frag.End, io.SeekStart)
	c.Check(offset, gc.Equals, frag.Begin+6)
	c.Check(err, gc.Equals, ErrSeekRequiresNewReader)
}

// RouteWrapper implements the RouteUpdater interface, for inspections by tests.
type routeWrapper struct {
	pb.BrokerClient
	routes map[pb.Journal]*pb.Route
}

func (w routeWrapper) UpdateRoute(journal pb.Journal, route *pb.Route) { w.routes[journal] = route }

type readFixture struct {
	status pb.Status
	err    error

	// optional fields
	content     string
	offset      int64
	fragment    *pb.Fragment
	fragmentUrl string
}

func (f readFixture) serve(c *gc.C, broker *teststub.Broker) {
	var req = <-broker.ReadReqCh
	c.Check(req.Journal, gc.Equals, pb.Journal("a/journal"))

	// Start with a basic response template which may be customized
	var resp = &pb.ReadResponse{
		Status: pb.Status_OK,
		Header: &pb.Header{
			BrokerId: pb.BrokerSpec_ID{Zone: "a", Suffix: "broker"},
			Route: pb.Route{
				Brokers: []pb.BrokerSpec_ID{{Zone: "a", Suffix: "broker"}},
				Primary: 0,
			},
			Etcd: pb.Header_Etcd{
				ClusterId: 12,
				MemberId:  34,
				Revision:  56,
				RaftTerm:  78,
			},
		},
		Offset:    req.Offset,
		WriteHead: 1024,
		Fragment:  &pb.Fragment{Journal: "a/journal", Begin: 0, End: 1024},
	}

	if f.status != pb.Status_OK && len(f.content) == 0 {
		resp.Status = f.status
	}
	if f.offset != 0 {
		resp.Offset = f.offset
	}
	if f.fragment != nil {
		resp.Fragment = f.fragment
	}
	if f.fragmentUrl != "" {
		resp.FragmentUrl = f.fragmentUrl
	}

	broker.ReadRespCh <- resp

	if l := len(f.content); l != 0 {
		broker.ReadRespCh <- &pb.ReadResponse{Offset: resp.Offset, Content: []byte(f.content[:l/2])}
		broker.ReadRespCh <- &pb.ReadResponse{Offset: resp.Offset + int64(l/2), Content: []byte(f.content[l/2:])}

		if f.status != pb.Status_OK {
			broker.ReadRespCh <- &pb.ReadResponse{Status: f.status}
		}
	}
	broker.ErrCh <- f.err
}

func serveReadFixtures(c *gc.C, broker *teststub.Broker, fixtures ...readFixture) {
	for _, f := range fixtures {
		f.serve(c, broker)
	}
}

func installFileClient() (cleanup func()) {
	var t = &http.Transport{}
	t.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))

	var prevClient = HTTPClient
	HTTPClient = &http.Client{Transport: t}

	return func() { HTTPClient = prevClient }
}

func buildFragmentFixture(c *gc.C) (frag pb.Fragment, url string, cleanup func()) {
	const data = "XXXXXhello, world!!!"

	var dir, err = ioutil.TempDir("", "ReaderSuite")
	c.Assert(err, gc.IsNil)

	cleanup = func() {
		c.Check(os.RemoveAll(dir), gc.IsNil)
	}

	frag = pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              120,
		Sum:              pb.SHA1SumOf(data),
		CompressionCodec: pb.CompressionCodec_GZIP,
		BackingStore:     pb.FragmentStore("file://" + filepath.ToSlash(dir)),
	}
	url = string(frag.BackingStore) + "/" + frag.ContentName()

	var path = filepath.Join(dir, frag.ContentName())
	file, err := os.Create(path)
	c.Assert(err, gc.IsNil)

	comp, err := codecs.NewCodecWriter(file, pb.CompressionCodec_GZIP)
	c.Assert(err, gc.IsNil)
	_, err = comp.Write([]byte(data))
	c.Assert(err, gc.IsNil)
	c.Assert(comp.Close(), gc.IsNil)
	c.Assert(file.Close(), gc.IsNil)

	return
}

var _ = gc.Suite(&ReaderSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
