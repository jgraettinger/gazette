package v3_allocator

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
)

type Announcement struct {
	Key      string
	Revision int64

	etcd *clientv3.Client
}

func Announce(ctx context.Context, etcd *clientv3.Client, key, value string, lease clientv3.LeaseID) (*Announcement, error) {
	for {
		var resp, err = etcd.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(lease))).
			Commit()

		if err == nil && resp.Succeeded == false {
			err = fmt.Errorf("key exists")
		}

		if err == nil {
			return &Announcement{
				Key:      key,
				Revision: resp.Header.Revision,
				etcd:     etcd,
			}, nil
		}

		log.WithFields(log.Fields{"err": err, "key": key}).
			Warn("failed to announce key (will retry)")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(announceConflictRetryInterval):
			// Pass.
		}
	}
}

func (a *Announcement) Update(ctx context.Context, value string) error {
	var resp, err = a.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(a.Key), "=", a.Revision)).
		Then(clientv3.OpPut(a.Key, value, clientv3.WithIgnoreLease())).
		Commit()

	if err == nil && resp.Succeeded == false {
		err = fmt.Errorf("key modified or deleted externally (expected revision %d)", a.Revision)
	}
	if err == nil {
		a.Revision = resp.Header.Revision
	}
	return err
}

var announceConflictRetryInterval = time.Second * 10
