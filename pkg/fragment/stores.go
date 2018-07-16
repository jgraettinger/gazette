package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/gorilla/schema"
)

type Store interface {
	SignURL(protocol.Fragment, time.Duration) (*url.URL, error)

	Open(context.Context, protocol.Fragment) (io.ReadCloser, error)

	Persist(context.Context, Spool) error

	List(ctx context.Context, prefix string, callback func(protocol.Fragment) error) error
}

func parseStoreArgs(ep *url.URL, args interface{}) error {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	if q, err := url.ParseQuery(ep.RawQuery); err != nil {
		return err
	} else if err = decoder.Decode(&args, q); err != nil {
		return fmt.Errorf("parsing store URL arguments: %s", err)
	}
	return nil
}
