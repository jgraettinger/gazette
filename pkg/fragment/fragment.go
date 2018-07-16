package fragment

import (
	"io"

	"github.com/LiveRamp/gazette/pkg/protocol"
)

// Fragment wraps the protocol.Fragment type with a nil-able backing local File.
type Fragment struct {
	protocol.Fragment
	// Local uncompressed file of the Fragment, or nil iff the Fragment is remote.
	File File
}

type File interface {
	io.ReaderAt
	io.Seeker
	io.WriterAt
	io.Writer
	io.Closer
}

/*
func SignURL(f Fragment, duration time.Duration) (*url.URL, error) {
	if f.File != nil || f.BackingStore == "" {
		return nil, errors.New("not a remote fragment")
	}

	var fs, err = cloudstore.NewFileSystem(nil, string(f.BackingStore))
	if err != nil {
		return nil, err
	}
	defer fs.Close() // TODO(johnny): Remove this from cloudstore API.

	return fs.ToURL(f.ContentPath(), "GET", duration)
}

func (f Fragment) ReaderFromOffset(offset int64) (io.ReadCloser, error) {
	if f.File != nil {
		return ioutil.NopCloser(io.NewSectionReader(f.File, offset-f.Begin, f.End-offset)), nil
	}

	var fs, err = cloudstore.NewFileSystem(nil, string(f.BackingStore))
	if err != nil {
		return nil, err
	}
	file, err := fs.Open(f.ContentPath())
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(offset-f.Begin, 0)
	return file, err
}
*/
