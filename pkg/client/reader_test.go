package client

import (
	"context"
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

func (s *ReaderSuite) TestFoo(c *gc.C) {
	var frag, url, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer installFileClient()()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)

	var r = &Reader{
		Context: ctx,
		Client:  broker.MustClient(),
		Journal: "a/journal",
		Offset:  23456,
		Block:   true,
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
	const data = "hello, world!"

	var dir, err = ioutil.TempDir("", "ReaderSuite")
	c.Assert(err, gc.IsNil)

	cleanup = func() {
		c.Check(os.RemoveAll(dir), gc.IsNil)
	}

	frag = pb.Fragment{
		Journal:          "a/journal",
		Begin:            12345,
		End:              67890,
		Sum:              pb.SHA1SumOf("hello, world!"),
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
}

var _ = gc.Suite(&ReaderSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
