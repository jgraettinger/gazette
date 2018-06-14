package client

import (
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

func (s *ReaderSuite) TestStreamingAndRouteAdvisement(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var routes = make(map[pb.Journal]*pb.Route)

	var r = NewReader(ctx, routeWrapper{broker.MustClient(), routes}, pb.ReadRequest{Journal: "a/journal", Offset: 105})
	var ar = readAsync(r)

	var responseFixture = &pb.ReadResponse{
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
		Offset:    110,
		WriteHead: 200,
		Fragment:  &pb.Fragment{Journal: "a/journal", Begin: 110, End: 122},
	}

	// Expect to receive a ReadRequest.
	c.Check(<-broker.ReadReqCh, gc.DeepEquals, &pb.ReadRequest{Journal: "a/journal", Offset: 105})

	// Send ReadResponse fixture with Route header.
	broker.ReadRespCh <- responseFixture
	broker.ReadRespCh <- &pb.ReadResponse{Offset: 110, Content: []byte("hello,")}
	broker.ReadRespCh <- &pb.ReadResponse{Offset: 116, Content: []byte(" world")}
	broker.ErrCh <- nil // Send EOF.

	<-ar.doneCh
	c.Check(string(ar.buffer), gc.Equals, "hello, world")
	c.Check(ar.err, gc.Equals, io.EOF)
	c.Check(r.Request.Offset, gc.Equals, int64(121))

	// Expect Reader advised of the updated Route.
	c.Check(routes["a/journal"], gc.DeepEquals, &pb.Route{
		Brokers: []pb.BrokerSpec_ID{{Zone: "a", Suffix: "broker"}},
		Primary: 0,
	})

	// Next case: this time, a non-EOF error is returned.
	r = NewReader(ctx, routeWrapper{broker.MustClient(), routes}, pb.ReadRequest{Journal: "a/journal", Offset: 105})
	ar = readAsync(r)

	<-broker.ReadReqCh
	broker.ReadRespCh <- responseFixture
	broker.ReadRespCh <- &pb.ReadResponse{Offset: 110, Content: []byte("hello,")}
	broker.ErrCh <- errors.New("potato")

	<-ar.doneCh
	c.Check(string(ar.buffer), gc.Equals, "hello,")
	c.Check(ar.err, gc.ErrorMatches, `rpc error: code = Unknown desc = potato`)
	c.Check(r.Request.Offset, gc.Equals, int64(116))

	// Expect Reader purged the Route advisement.
	c.Check(routes["a/journal"], gc.IsNil)
}

func (s *ReaderSuite) TestReaderEOFCases(c *gc.C) {
	var frag, url, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer installFileClient()()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	var cases = []struct {
		pb.ReadResponse
		expectErr     error
		expectContent string
	}{
		{
			// broker sends OK with a Fragment & URL before EOF (eg, due to a
			// DoNotProxy or MetadataOnly ReadRequest). Expect a direct read occurs.
			ReadResponse: pb.ReadResponse{
				Status:      pb.Status_OK,
				Offset:      105,
				WriteHead:   120,
				Fragment:    &frag,
				FragmentUrl: url,
			},
			expectContent: "hello, world!!!",
			expectErr:     io.EOF,
		},
		{
			ReadResponse: pb.ReadResponse{Status: pb.Status_NOT_JOURNAL_BROKER},
			expectErr:    ErrNotJournalBroker,
		},
		{
			ReadResponse: pb.ReadResponse{Status: pb.Status_OFFSET_NOT_YET_AVAILABLE},
			expectErr:    ErrOffsetNotYetAvailable,
		},
		{
			ReadResponse: pb.ReadResponse{
				Status:    pb.Status_OK,
				Offset:    105,
				WriteHead: 120,
				Fragment:  &frag,
				// Missing FragmentUrl.
			},
			expectErr: io.EOF,
		},
	}

	for _, tc := range cases {
		var r = NewReader(ctx, broker.MustClient(), pb.ReadRequest{Journal: "a/journal", Offset: 105})
		var ar = readAsync(r)

		c.Check(<-broker.ReadReqCh, gc.DeepEquals, &pb.ReadRequest{Journal: "a/journal", Offset: 105})

		broker.ReadRespCh <- &tc.ReadResponse
		broker.ErrCh <- nil

		<-ar.doneCh
		c.Check(ar.err, gc.Equals, tc.expectErr)
		c.Check(string(ar.buffer), gc.Equals, tc.expectContent)
		c.Check(r.Request.Offset, gc.Equals, int64(105+len(tc.expectContent)))
	}
}

func (s *ReaderSuite) TestReaderSeekCases(c *gc.C) {
	var frag, url, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer installFileClient()()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var stream, err = broker.MustClient().Read(ctx, &pb.ReadRequest{})
	c.Check(err, gc.IsNil)

	rc, err := OpenFragmentURL(ctx, frag.Begin, frag, url)
	c.Check(err, gc.IsNil)

	var r = &Reader{
		Request:  pb.ReadRequest{Offset: frag.Begin},
		Response: pb.ReadResponse{Offset: frag.Begin, Fragment: &frag},
		stream:   stream,
		direct:   rc,
	}

	// Case: seeking forward into a covering, directly read Fragment works.
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

	// Case: as does seeking beyond the current fragment extent
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

type asyncRead struct {
	buffer []byte
	err    error
	doneCh chan struct{}
}

func readAsync(reader *Reader) *asyncRead {
	var ar = &asyncRead{
		doneCh: make(chan struct{}),
	}

	go func() {
		// Use multiple reads with a small buffer to exercise multiple reads per chunk.
		var p [2]byte
		var n int

		for ar.err == nil {
			n, ar.err = reader.Read(p[:])
			ar.buffer = append(ar.buffer, p[:n]...)
		}
		close(ar.doneCh)
	}()

	return ar
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
