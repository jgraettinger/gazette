package fragment

import (
	"errors"
	"os"
	"time"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/protocol"
)

type WalkFuncSuite struct{}

func (s *WalkFuncSuite) TestPathWalkFuncAdapater(c *gc.C) {
	var out []protocol.Fragment

	var f = WalkFuncAdapter(func(frag protocol.Fragment) error {
		out = append(out, frag)
		return nil
	}, "/strip-prefix/", "", "rename/from/", "foo/")

	var expect = protocol.Fragment{
		Journal: "foo/bar",
		Begin:   11112222,
		End:     33334444,
		Sum:     protocol.SHA1Sum{Part1: 111, Part2: 222, Part3: 333},
		ModTime: time.Unix(12345, 0),
	}

	// Expect a Fragment is parsed...
	c.Check(f("foo/bar/"+expect.ContentName(), mockFinfo{size: 123}, nil), gc.IsNil)
	// ... and that any matched path re-writes are applied to produce the Journal.
	c.Check(f("/strip-prefix/rename/from/bar/"+expect.ContentName(), mockFinfo{size: 123}, nil), gc.IsNil)

	c.Check(out, gc.DeepEquals, []protocol.Fragment{expect, expect})
	out = out[:0]

	// Directory files are ignored.
	c.Check(f("a/path/"+expect.ContentName(), mockFinfo{isDir: true, size: 123}, nil), gc.IsNil)
	// As are zero-length fragments.
	c.Check(f("a/path/"+expect.ContentName(), mockFinfo{size: 0}, nil), gc.IsNil)
	// And errors are passed through.
	c.Check(f("a/path/"+expect.ContentName(), mockFinfo{size: 123}, errors.New("err!")), gc.ErrorMatches, "err!")

	c.Check(out, gc.DeepEquals, []protocol.Fragment{}) // Verify ignored fragments were in fact ignored.
}

type mockFinfo struct {
	isDir bool
	size  int64
}

func (mfi mockFinfo) Name() string       { return "filename" }
func (mfi mockFinfo) Size() int64        { return mfi.size }
func (mfi mockFinfo) Mode() os.FileMode  { return 0 }
func (mfi mockFinfo) ModTime() time.Time { return time.Unix(12345, 0) }
func (mfi mockFinfo) IsDir() bool        { return mfi.isDir }
func (mfi mockFinfo) Sys() interface{}   { return nil }

var _ = gc.Suite(&WalkFuncSuite{})
