package protocol

import (
	gc "github.com/go-check/check"
)

// RPCSuite tests RPC Request & Response validation cases by building instances
// broken in every conceivable way, and incrementally updating them until they
// pass validation.
type RPCSuite struct{}

func (s *RPCSuite) TestReadRequestValidation(c *gc.C) {
	var req = ReadRequest{
		Journal: "/bad",
		Offset:  -2,
	}
	c.Check(req.Validate(), gc.ErrorMatches, `Journal: cannot begin with '/': /bad`)
	req.Journal = "good"

	c.Check(req.Validate(), gc.ErrorMatches, `invalid Offset \(-2; expected -1 <= Offset <= MaxInt64`)
	req.Offset = -1

	c.Check(req.Validate(), gc.IsNil)

	// Block, DoNotProxy, and MetadataOnly have no validation.
}

func (s *RPCSuite) TestReadResponseValidationCases(c *gc.C) {
	var frag, _ = ParseContentPath("a/journal/00000000499602d2-000000008bd03835-0102030405060708090a0b0c0d0e0f1011121314.sz")
	frag.Journal = "/bad/name"

	var resp = ReadResponse{
		Status:      9101,
		Offset:      1234,
		WriteHead:   5678,
		Route:       &Route{Primary: 2, Brokers: []BrokerSpec_ID{{"zone", "name"}}},
		Fragment:    &frag,
		FragmentUrl: ":/bad/url",
	}
	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status .*`)
	resp.Status = Status_OK

	c.Check(resp.Validate(), gc.ErrorMatches, `Route: invalid Primary .*`)
	resp.Route.Primary = 0

	c.Check(resp.Validate(), gc.ErrorMatches, `Fragment.Journal: cannot begin with '/': /bad/name`)
	frag.Journal = "a/journal"

	c.Check(resp.Validate(), gc.ErrorMatches, `invalid Offset \(1234; expected 1234567890 <= offset < \d+\)`)
	resp.Offset = 1234567891

	c.Check(resp.Validate(), gc.ErrorMatches, `invalid WriteHead \(5678; expected >= 2345678901\)`)
	resp.WriteHead = 2345678901

	c.Check(resp.Validate(), gc.ErrorMatches, `FragmentUrl: parse :/bad/url: missing protocol scheme`)
	resp.FragmentUrl = "http://foo"

	c.Check(resp.Validate(), gc.IsNil) // Success.

	// Remove Fragment.
	resp.Fragment = nil
	resp.WriteHead = -1

	c.Check(resp.Validate(), gc.ErrorMatches, `invalid WriteHead \(-1; expected >= 0\)`)
	resp.WriteHead = 1234

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Offset without Fragment \(\d+\)`)
	resp.Offset = 0

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected FragmentUrl without Fragment \(http://foo\)`)
	resp.FragmentUrl = ""

	c.Check(resp.Validate(), gc.IsNil) // Success.

	// Set Content.
	resp.Content = []byte("foobar")
	resp.Fragment = &frag
	resp.Offset = 5678
	resp.FragmentUrl = "http://foo"
	resp.Status = Status_WRONG_ROUTE

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Status with Content \(WRONG_ROUTE\)`)
	resp.Status = Status_OK

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Offset with Content \(5678\)`)
	resp.Offset = 0

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected WriteHead with Content \(1234\)`)
	resp.WriteHead = 0

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Route with Content \(brokers:.*`)
	resp.Route = nil

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected Fragment with Content \(journal:.*`)
	resp.Fragment = nil

	c.Check(resp.Validate(), gc.ErrorMatches, `unexpected FragmentUrl with Content \(http://foo\)`)
	resp.FragmentUrl = ""

	c.Check(resp.Validate(), gc.IsNil)
}

func (s *RPCSuite) TestReplicateRequestValidationCases(c *gc.C) {
	var rt = &Route{Primary: 2, Brokers: []BrokerSpec_ID{{"zone", "name"}}}
	var req = ReplicateRequest{
		Journal:    "/bad",
		NextOffset: -1,
		Route:      rt,
	}

	c.Check(req.Validate(), gc.ErrorMatches, `invalid NextOffset \(-1; expected >= 0\)`)
	req.NextOffset = 1234
	c.Check(req.Validate(), gc.ErrorMatches, `Journal: cannot begin with '/': /bad`)
	req.Journal = "journal"
	c.Check(req.Validate(), gc.ErrorMatches, `Route: invalid Primary .*`)
	req.Route.Primary = 0

	c.Check(req.Validate(), gc.IsNil) // Success.

	req.Route = nil
	c.Check(req.Validate(), gc.ErrorMatches, `expected Route`)
	req.Route = rt

	req.Content = []byte("foo")

	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Journal with Content \(journal\)`)
	req.Journal = ""
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Route with Content \(brokers:.*\)`)
	req.Route = nil

	c.Check(req.Validate(), gc.IsNil) // Success.

	req.Route = rt
	req.Journal = "journal"
	req.Commit = -1

	c.Check(req.Validate(), gc.ErrorMatches, `invalid Commit \(-1; expected >= 0\)`)
	req.Commit = 5678

	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Journal with Commit \(journal\)`)
	req.Journal = ""
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected NextOffset with Commit \(1234\)`)
	req.NextOffset = 0
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Route with Commit \(brokers:.*\)`)
	req.Route = nil
	c.Check(req.Validate(), gc.ErrorMatches, `unexpected Content with Commit \(len 3\)`)
	req.Content = nil

	c.Check(req.Validate(), gc.IsNil) // Success.
}

func (s *RPCSuite) TestReplicateResponseValidationCases(c *gc.C) {
	var resp = ReplicateResponse{
		Status:    9101,
		WriteHead: -1,
		Route:     &Route{Primary: 2, Brokers: []BrokerSpec_ID{{"zone", "name"}}},
	}

	c.Check(resp.Validate(), gc.ErrorMatches, `Status: invalid status .*`)
	resp.Status = Status_OK
	c.Check(resp.Validate(), gc.ErrorMatches, `Route: invalid Primary .*`)
	resp.Route.Primary = 0
	c.Check(resp.Validate(), gc.ErrorMatches, `invalid WriteHead \(-1; expected >= 0\)`)
	resp.WriteHead = 1234

	c.Check(resp.Validate(), gc.IsNil) // Success.
}

func (s *RPCSuite) TestAppendRequestValidationCases(c *gc.C) {}

var _ = gc.Suite(&RPCSuite{})
