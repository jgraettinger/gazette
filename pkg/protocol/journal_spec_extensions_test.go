package protocol

import (
	"time"

	gc "github.com/go-check/check"
)

type JournalSuite struct{}

func (s *JournalSuite) TestJournalValidationCases(c *gc.C) {
	var cases = []struct {
		j      Journal
		expect string
	}{
		{"a/valid/path/to/a/journal", ""}, // Success.
		{"/leading/slash", "cannot begin with '/'"},
		{"trailing/slash/", "must be a clean path: .*"},
		{"extra-middle//slash", "must be a clean path: .*"},
		{"not-$%|-base64", "not base64 alphabet: .*"},
		{"", `invalid length \(0; expected 4 <= .*`},
		{"zz", `invalid length \(2; expected 4 <= .*`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.j.Validate(), gc.IsNil)
		} else {
			c.Check(tc.j.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *JournalSuite) TestSpecFragmentValidationCases(c *gc.C) {
	var f = JournalSpec_Fragment{
		Length:           1 << 18,
		CompressionCodec: CompressionCodec_SNAPPY,
		Stores:           []FragmentStore{"s3://bucket/path", "gs://other-bucket/path"},
		RefreshInterval:  5 * time.Minute,
		Retention:        365 * 24 * time.Hour,
	}
	c.Check(f.Validate(), gc.IsNil)

}

var _ = gc.Suite(&JournalSuite{})
