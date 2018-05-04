package protocol

import (
	gc "github.com/go-check/check"
)

type FragmentStoreSuite struct{}

func (s *FragmentStoreSuite) TestValidation(c *gc.C) {
	var cases = []struct {
		fs     FragmentStore
		expect string
	}{
		{"s3://my-bucket/path?query", ""}, // Success.
		{":garbage: :garbage:", "parse .* missing protocol scheme"},
		{"foobar://baz", "invalid scheme: foobar"},
		{"/baz/bing", "not absolute: .*"},
		{"gs:///baz/bing", "missing bucket: .*"},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.fs.Validate(), gc.IsNil)
		} else {
			c.Check(tc.fs.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *FragmentStoreSuite) TestURLConversion(c *gc.C) {
	var fs FragmentStore = "s3://bucket/path"
	c.Check(fs.URL().Host, gc.Equals, "bucket")

	fs = "/baz/bing"
	c.Check(func() { fs.URL() }, gc.PanicMatches, "not absolute: .*")
}

var _ = gc.Suite(&FragmentStoreSuite{})
