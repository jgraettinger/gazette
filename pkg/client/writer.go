package client

import (
	"context"
	"io"

	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type Writer struct {
	Response pb.AppendResponse

	req    pb.AppendRequest
	stream pb.Broker_AppendClient
}

func NewAppend(ctx context.Context, cl pb.BrokerClient, journal pb.Journal, r io.Reader) (*pb.AppendResponse, error) {
	var stream, err = cl.Append(WithJournalHint(ctx, journal))
	if err != nil {
		return nil, err
	}
	var req = &pb.AppendRequest{Journal: journal}

	if err = stream.SendMsg(req); err != nil {
		stream.CloseSend()
		return nil, err
	}
	req.Journal = ""
	req.Content = make([]byte)
}

type appender struct {
	stream grpc.Stream
	req    pb.AppendRequest
}

func (a *appender) Write(p []byte) (n int, err error) {
	a.req.Content = p

	if err := a.stream.SendMsg(&a.req); err != nil {
		return 0, err
	} else {
		return len(p), nil
	}
}

func (a *appender) Close() error {

}
