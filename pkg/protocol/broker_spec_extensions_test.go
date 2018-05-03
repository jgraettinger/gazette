package protocol

import (
	gc "github.com/go-check/check"
)

type BrokerSpecSuite struct{}

func (s *BrokerSpecSuite) TestIDValidationCases(c *gc.C) {
	var cases = []struct {
		id     BrokerSpec_ID
		expect string
	}{
		{BrokerSpec_ID{"a-zone", "a-name"}, ""}, // Success.
		{BrokerSpec_ID{"", "a-name"}, "Zone: invalid length .*"},
		{BrokerSpec_ID{"&*", "a-name"}, "Zone: not base64 .*"},
		{BrokerSpec_ID{"a-very-very-very-very-very-very-very-long-zone", "a-name"},
			"Zone: not base64 .*"},
		{BrokerSpec_ID{"a-zone", "ae"}, "Suffix: invalid length .*"},
		{BrokerSpec_ID{"a-zone", "&*($"}, "Suffix: not base64 .*"},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.id.Validate(), gc.IsNil)
		} else {
			c.Check(tc.id.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *BrokerSpecSuite) TestSpecValidationCases(c *gc.C) {
	var model = BrokerSpec{
		Id:           BrokerSpec_ID{"a-zone", "a-name"},
		Endpoint:     "http://foo",
		JournalLimit: 5,
	}
	c.Check(model.Validate(), gc.Equals, nil)
	c.Check(model.ItemLimit(), gc.Equals, 5)

	model.Id.Zone = ""
	c.Check(model.Validate(), gc.ErrorMatches, "Id.Zone: invalid length .*")

	model.Id.Zone = "a-zone"
	model.Endpoint = "invalid"
	c.Check(model.Validate(), gc.ErrorMatches, "Endpoint: not absolute: .*")

	model.Endpoint = "http://foo"
	model.JournalLimit = maxBrokerJournalLimit + 1
	c.Check(model.Validate(), gc.ErrorMatches, `invalid JournalLimit \(\d+; expected 0 <= JournalLimit <= \d+\)`)
}

var _ = gc.Suite(&BrokerSpecSuite{})
