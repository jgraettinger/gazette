package fragment

import (
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/protocol"
)

// WalkFuncAdapter builds and returns a filepath.WalkFunc which parses encountered
// files as Fragments, and passes each to the provided |callback|.
func WalkFuncAdapter(store protocol.FragmentStore, callback func(protocol.Fragment) error) filepath.WalkFunc {
	return func(fpath string, finfo os.FileInfo, err error) error {
		if err != nil {
			return err
		} else if finfo.IsDir() {
			return nil
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
		fragment.BackingStore = store

		return callback(fragment)
	}
}
