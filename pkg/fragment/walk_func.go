package fragment

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/protocol"
)

// WalkFuncAdapter builds and returns a filepath.WalkFunc which parses encountered
// files as Fragments, and passes each to the provided |callback|. Prefix |rewrites|
// may be included, as pairs of "from", "to" prefixes which are applied in order. For
// example, NewWalkFuncAdapter(cb, "/from/", "/foo/to/", "/foo/", "/") would rewrite
// path "/from/bar" => "/to/bar".
func WalkFuncAdapter(callback func(protocol.Fragment) error, rewrites ...string) filepath.WalkFunc {
	if len(rewrites)%2 != 0 {
		panic(fmt.Sprintf("invalid odd-length rewrites: %#v", rewrites))
	}

	return func(fpath string, finfo os.FileInfo, err error) error {
		if err != nil {
			return err
		} else if finfo.IsDir() {
			return nil
		}

		for i := 0; i != len(rewrites); i += 2 {
			if strings.HasPrefix(fpath, rewrites[i]) {
				fpath = path.Join(rewrites[i+1], fpath[len(rewrites[i]):])
			}
		}

		fragment, err := protocol.ParseContentPath(fpath)
		if err != nil {
			log.WithFields(log.Fields{"path": fpath, "err": err}).Warning("parsing fragment")
			return nil
		} else if finfo.Size() == 0 && fragment.ContentLength() > 0 {
			log.WithFields(log.Fields{"path": fpath}).Error("zero-length fragment")
			return nil
		}
		fragment.ModTime = finfo.ModTime()

		return callback(fragment)
	}
}
