package client

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/codecs"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type ReaderSuite struct{}

func (s *ReaderSuite) TestReaderCases(c *gc.C) {
	var frag, _, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer installFileClient()()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	//var routes = make(map[pb.Journal]*pb.Route)

	// Non-blocking proxy read.
	var ar = startAsyncRead(&Reader{
		Context: ctx,
		Client:  broker.MustClient(),
		Journal: "a/journal",
		Offset:  105,
		Block:   false,
	})

	var req = <-broker.ReadReqCh
	c.Check(req, gc.DeepEquals, &pb.ReadRequest{
		Journal:    "a/journal",
		Offset:     105,
		Block:      false,
		DoNotProxy: false,
	})

	broker.ReadRespCh <- &pb.ReadResponse{Offset: 105, Fragment: &frag}
	broker.ReadRespCh <- &pb.ReadResponse{Content: []byte("hello, world!!!")}
	broker.ReadRespCh <- &pb.ReadResponse{Status: pb.Status_OFFSET_NOT_YET_AVAILABLE}
	broker.ErrCh <- nil

	<-ar.doneCh
	c.Check(ar.err, gc.Equals, ErrWouldBlock)
	c.Check(ar.buffer.String(), gc.Equals, "hello, world!")
}

type routeWrapper struct {
	pb.BrokerClient
	routes map[pb.Journal]*pb.Route
}

func (w routeWrapper) UpdateRoute(journal pb.Journal, route *pb.Route) { w.routes[journal] = route }

type asyncRead struct {
	reader *Reader
	buffer bytes.Buffer
	err    error
	doneCh chan struct{}
}

func startAsyncRead(reader *Reader) *asyncRead {
	var ar = &asyncRead{
		reader: reader,
		doneCh: make(chan struct{}),
	}

	go func() {
		// Use CopyBuffer with a small buffer to exercise multiple reads per chunk.
		_, ar.err = io.CopyBuffer(&ar.buffer, reader, make([]byte, 3))
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
	const data = "hello, world!"

	var dir, err = ioutil.TempDir("", "ReaderSuite")
	c.Assert(err, gc.IsNil)

	cleanup = func() {
		c.Check(os.RemoveAll(dir), gc.IsNil)
	}

	frag = pb.Fragment{
		Journal:          "a/journal",
		Begin:            100,
		End:              120,
		Sum:              pb.SHA1SumOf("XXXXXhello, world!!!"),
		CompressionCodec: pb.CompressionCodec_GZIP,
		BackingStore:     pb.FragmentStore("file://" + dir),
	}

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
