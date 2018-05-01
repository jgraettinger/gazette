package localrpc

import (
	"context"
	"io"
	"reflect"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// New begins the invocation of the given callback in a goroutine, and returns
// a ClientStream which is its ServerStream pair. The streams communicate
// through channels, and are otherwise complete implementations of their
// respective interfaces.
func New(ctx context.Context, cb func(grpc.ServerStream) error) grpc.ClientStream {
	var c1, c2 = make(chan interface{}), make(chan interface{})
	var hc, tc = make(chan metadata.MD), make(chan metadata.MD)

	var cancelCtx, cancel = context.WithCancel(ctx)

	var srv = chSrvStream{
		chStream:  chStream{ctx: cancelCtx, sendCh: c1, recvCh: c2},
		headerCh:  hc,
		trailerCh: tc,
	}

	go func() {
		if err := cb(srv); err != nil {
			select {
			case srv.sendCh <- err:
			case <-ctx.Done():
			}
		}
		cancel()
	}()

	return chClientStream{
		chStream:  chStream{ctx: cancelCtx, sendCh: c2, recvCh: c1},
		headerCh:  hc,
		trailerCh: tc,
	}
}

type chStream struct {
	ctx    context.Context
	sendCh chan interface{}
	recvCh chan interface{}
}

func (s chStream) Context() context.Context { return s.ctx }

func (s chStream) SendMsg(m interface{}) error {
	select {
	case into := <-s.sendCh:
		reflect.ValueOf(into).Set(reflect.ValueOf(m))
		s.sendCh <- nil
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s chStream) RecvMsg(m interface{}) error {
	select {
	case s.recvCh <- m:
		if r := <-s.recvCh; r != nil {
			panic("unexpected recvCh msg")
		}
		return nil
	case r, ok := <-s.recvCh:
		if !ok {
			return io.EOF
		} else if err, ok := r.(error); ok {
			return err
		} else {
			panic("unexpected recvCh msg")
		}
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

type chSrvStream struct {
	chStream
	headerCh, trailerCh chan<- metadata.MD
}

func (ss chSrvStream) SetHeader(md metadata.MD) error {
	ss.headerCh <- md
	return nil
}

func (ss chSrvStream) SendHeader(md metadata.MD) error {
	ss.headerCh <- md
	close(ss.headerCh)
	return nil
}

func (ss chSrvStream) SetTrailer(md metadata.MD) {
	ss.trailerCh <- md
}

type chClientStream struct {
	chStream
	headerCh, trailerCh <-chan metadata.MD
}

func (cs chClientStream) Header() (metadata.MD, error) {
	return cs.consumeMD(cs.headerCh), cs.Context().Err()
}

func (cs chClientStream) Trailer() metadata.MD {
	return cs.consumeMD(cs.trailerCh)
}

func (cs chClientStream) CloseSend() error {
	close(cs.sendCh)
	return nil
}

func (cs chClientStream) consumeMD(ch <-chan metadata.MD) metadata.MD {
	var all = make(metadata.MD)

	for {
		select {
		case md, ok := <-ch:
			if !ok {
				return all
			}
			for k, vs := range md {
				all[k] = vs
			}
		case <-cs.Context().Done():
			return nil
		}
	}
}
