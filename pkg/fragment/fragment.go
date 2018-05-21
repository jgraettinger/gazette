package fragment

import (
	"errors"
	"net/url"
	"os"
	"time"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
	"github.com/LiveRamp/gazette/pkg/protocol"
)

// Fragment wraps the protocol.Fragment type with a backing,
// local file (if one exists).
type Fragment struct {
	protocol.Fragment

	// Local uncompressed file of the Fragment, or nil iff the Fragment is remote.
	File *os.File
}

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

/*
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
