package broker

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/fragment"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type HTTPSuite struct{}

func (s *HTTPSuite) TestReadAndWrite(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.BrokerSpec_ID{"local", "broker"})
	var dialer, _ = newDialer(1)

	newTestJournal(c, ks, "a/journal", 1, broker.id)
	broker.replicas["a/journal"].index.ReplaceRemote(fragment.Set{}) // "Complete" remote load.

	var mux = NewHTTPGateway(*broker.resolver, dialer)

	var req, _ = http.NewRequest("GET", "/journal/name?offset=0&block=true", nil)
	var w = httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusPartialContent)
	c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
		fmt.Sprintf("bytes 12350-%v/%v", math.MaxInt64, math.MaxInt64))
	c.Check(w.HeaderMap.Get(WriteHeadHeader), gc.Equals, "12371")
	c.Check(w.HeaderMap.Get(FragmentNameHeader), gc.Equals,
		"000000000000303e-0000000000003053-0000000000000000000000000000000000000000")
	c.Check(w.HeaderMap.Get(FragmentLocationHeader), gc.Matches,
		"file:///.*/journal/name/000000000000303e-0000000000003053-"+
			"0000000000000000000000000000000000000000")
	c.Check(w.Body.String(), gc.Equals, "")
}

var _ = gc.Suite(&HTTPSuite{})
