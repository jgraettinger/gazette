package fragment

import (
	"flag"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

var FileSystemStoreRoot = flag.String("fsStoreRoot", "/example/path/to/store",
	"Root path for the file:// Fragment store (eg, an NFS volume mount)")

func fsURL(ep *url.URL, fragment pb.Fragment) string {
	return "file://" + ep.Path + fragment.ContentPath()
}

func fsOpen(ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	var path = filepath.Join(*FileSystemStoreRoot, filepath.FromSlash(ep.Path+fragment.ContentPath()))
	var f, err = os.Open(path)
	return f, err
}

func fsPersist(ep *url.URL, spool Spool) error {
	// Offloaded decompression isn't possible on file:// mounts.
	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		log.WithField("ep", ep.String()).
			Warn("file:// store doesn't support GZIP_OFFLOAD_DECOMPRESSION; using GZIP instead")

		spool.CompressionCodec = pb.CompressionCodec_GZIP
	}
	var path = filepath.Join(*FileSystemStoreRoot, filepath.FromSlash(ep.Path+spool.ContentPath()))

	// Create the journal's fragment directory, if not already present.
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}

	// Test if |path| exists already.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Pass.
	} else if err == nil {
		// Already persisted; we're done.
	} else {
		return err
	}

	// Open a temp file under the target path directory.
	var f, err = ioutil.TempFile(filepath.Dir(path), ".partial-"+filepath.Base(path))
	if err != nil {
		return err
	}

	defer func(name string) {
		if rmErr := os.Remove(f.Name()); rmErr != nil {
			log.WithFields(log.Fields{"err": rmErr, "path": path}).
				Warn("failed to cleanup temp file")
		}
	}(f.Name())

	switch spool.CompressionCodec {
	case pb.CompressionCodec_NONE:
		_, err = io.Copy(f, io.NewSectionReader(spool.File, 0, spool.ContentLength()))
	default:
		_, err = io.Copy(f, io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength))
	}

	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Link(f.Name(), path)
	}
	return err
}

func fsList(store pb.FragmentStore, ep *url.URL, prefix string, callback func(pb.Fragment)) error {
	var root = filepath.Join(*FileSystemStoreRoot, ep.Path)

	return filepath.Walk(filepath.Join(root, filepath.FromSlash(prefix)),
		func(path string, info os.FileInfo, err error) error {

			var name string

			if err != nil {
				return err
			} else if info.IsDir() {
				return nil // Descend into directory.
			} else if name, err = filepath.Rel(root, path); err != nil {
				return err
			} else if name == "." || name == ".." {
				// Never return "." or ".." as they are not real directories.
				return nil
			}

			name = filepath.FromSlash(name)

			if frag, err := pb.ParseContentPath(name); err != nil {
				log.WithFields(log.Fields{"path": path, "err": err}).Warning("parsing fragment")
			} else if info.Size() == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{"path": path}).Warning("zero-length fragment")
			} else {
				frag.ModTime = info.ModTime()
				frag.BackingStore = store
				callback(frag)
			}
			return nil
		})
}
