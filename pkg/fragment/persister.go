package fragment

import (
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
	"github.com/LiveRamp/gazette/pkg/protocol"
)

type Persister struct {
	qA, qB, qC []Spool
	mu         sync.Mutex
	doneCh     chan struct{}
}

// NewPersister returns an empty, initialized Persister.
func NewPersister() *Persister {
	return &Persister{doneCh: make(chan struct{})}
}

func (p *Persister) SpoolComplete(spool Spool) {
	if spool.ContentLength() == 0 || spool.BackingStore == "" {
		// Cannot persist this Spool.
	} else if spool.Primary {
		// Attempt to immediately persist the Spool.
		go func() {
			if err := copySpoolToStore(spool); err != nil {
				log.WithField("err", err).Warn("failed to persist Spool")
				p.queue(spool)
			}
		}()
	} else {
		p.queue(spool)
	}
	return
}

func (p *Persister) Finish() {
	p.doneCh <- struct{}{}
	<-p.doneCh
}

func (p *Persister) queue(spool Spool) {
	defer p.mu.Unlock()
	p.mu.Lock()

	p.qC = append(p.qC, spool)
}

func (p *Persister) Serve() {
	for done, exiting := false, false; !done; {

		if !exiting {
			select {
			case <-time.After(time.Minute):
			case <-p.doneCh:
				exiting = true
			}
		}

		for _, spool := range p.qA {
			if err := copySpoolToStore(spool); err != nil {
				log.WithField("err", err).Warn("failed to persist Spool")
				p.queue(spool)
			}
		}

		// Rotate queues.
		p.mu.Lock()
		p.qA, p.qB, p.qC = p.qB, p.qC, p.qA[:0]

		if exiting && len(p.qA) == 0 && len(p.qB) == 0 {
			done = true
		}
		p.mu.Unlock()
	}
	close(p.doneCh)
}

// copySpoolToStore performs an atomic copy of the Spool Fragment to its target BackingStore.
func copySpoolToStore(spool Spool) error {
	var cfs, err = cloudstore.NewFileSystem(nil, string(spool.BackingStore))
	if err != nil {
		return err
	}
	defer cfs.Close()

	// Create the journal's fragment directory, if not already present.
	if err = cfs.MkdirAll(spool.Journal.String(), 0750); err != nil {
		return err
	}

	w, err := cfs.OpenFile(spool.ContentPath(), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)

	if os.IsExist(err) {
		// Already present on target file system. All done!
		return nil
	} else if err != nil {
		return err
	}

	if spool.Fragment.CompressionCodec == protocol.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		if sce, ok := w.(interface{ SetContentEncoding(string) }); ok {
			sce.SetContentEncoding("gzip")
		} else {
			// Fallback to no compression.
			spool.CompressionCodec = protocol.CompressionCodec_NONE
		}
	}

	var rc = spool.CodecReader()
	if _, err = cfs.CopyAtomic(w, rc); err != nil {
		_ = rc.Close()
		return err
	}
	return rc.Close()
}
