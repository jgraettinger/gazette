package fragment

import (
	"os"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
	log "github.com/sirupsen/logrus"
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
	if spool.Primary {
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
	var fs, err = cloudstore.NewFileSystem(nil, string(spool.BackingStore))
	if err != nil {
		return err
	}
	defer fs.Close()

	// Create the journal's fragment directory, if not already present.
	if err = fs.MkdirAll(spool.Journal.String(), 0750); err != nil {
		return err
	}

	w, err := fs.OpenFile(spool.ContentPath(), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)

	if os.IsExist(err) {
		// Already present on target file system. All done!
		return nil
	} else if err != nil {
		return err
	}

	var rc = spool.CodecReader()
	if _, err = fs.CopyAtomic(w, rc); err != nil {
		_ = rc.Close()
		return err
	}
	return rc.Close()
}
