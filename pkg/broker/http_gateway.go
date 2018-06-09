package broker

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type HTTPGateway struct {
	decoder  *schema.Decoder
	resolver resolver
	dialer   dialer
}

func NewHTTPGateway(resolver resolver, dialer dialer) *HTTPGateway {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	return &HTTPGateway{
		decoder:  decoder,
		resolver: resolver,
		dialer:   dialer,
	}
}

func (h *HTTPGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET", "HEAD":
		h.serveRead(w, r)
	case "PUT":
		h.serveWrite(w, r)
	default:
		http.Error(w, fmt.Sprintf("unknown method: %s", r.Method), http.StatusBadRequest)
	}
}

func (h *HTTPGateway) serveRead(w http.ResponseWriter, r *http.Request) {
	var req, err = h.parseReadRequest(r)

	if err != nil {
		if tr, ok := trace.FromContext(r.Context()); ok {
			tr.LazyPrintf("parsing request: %v", err)
			tr.SetError()
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var client pb.Broker_ReadClient
	var conn *grpc.ClientConn
	var resolution resolution
	var resp = new(pb.ReadResponse)

	resolution, err = h.resolver.resolve(resolveArgs{
		ctx:                   r.Context(),
		journal:               req.Journal,
		mayProxy:              true,
		requirePrimary:        false,
		requireFullAssignment: false,
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if resolution.status != pb.Status_OK {
		h.writeReadResponse(w, r, &pb.ReadResponse{
			Status: resolution.status,
			Header: &resolution.Header,
		})
		return
	}

	if conn, err = h.dialer.dial(r.Context(), resolution.BrokerId, resolution.Route); err == nil {
		if client, err = pb.NewBrokerClient(conn).Read(r.Context(), req); err == nil {
			err = client.RecvMsg(resp)
		}
	}

	if err != nil {
		if tr, ok := trace.FromContext(r.Context()); ok {
			tr.LazyPrintf("evaluating request: %v", err)
			tr.SetError()
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.writeReadResponse(w, r, resp)

	for {
		if err = client.RecvMsg(resp); err == io.EOF {
			break // Done.
		} else if err != nil {
			log.WithField("err", err).Warn("http_gateway: failed to proxy Read response")
			break // Done.
		}

		if _, err = w.Write(resp.Content); err != nil {
			log.WithField("err", err).Warn("http_gateway: failed to forward Read response")
			break
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}

func (h *HTTPGateway) serveWrite(w http.ResponseWriter, r *http.Request) {
	var req, err = h.parseAppendRequest(r)

	if err != nil {
		if tr, ok := trace.FromContext(r.Context()); ok {
			tr.LazyPrintf("parsing request: %v", err)
			tr.SetError()
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var client pb.Broker_AppendClient
	var conn *grpc.ClientConn
	var resolution resolution
	var resp = new(pb.AppendResponse)

	resolution, err = h.resolver.resolve(resolveArgs{
		ctx:                   r.Context(),
		journal:               req.Journal,
		mayProxy:              true,
		requirePrimary:        true,
		requireFullAssignment: true,
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if resolution.status != pb.Status_OK {
		h.writeAppendResponse(w, r, &pb.AppendResponse{
			Status: resolution.status,
			Header: &resolution.Header,
		})
		return
	}

	if conn, err = h.dialer.dial(r.Context(), resolution.BrokerId, resolution.Route); err == nil {
		if client, err = pb.NewBrokerClient(conn).Append(r.Context()); err == nil {
			err = client.SendMsg(req)
			*req = pb.AppendRequest{} // Clear metadata: hereafter, only Content is sent.
		}
	}

	var buffer = make([]byte, chunkSize)

	// Proxy content chunks from the http.Request through the Broker_AppendClient.
	var n int
	for done := false; !done && err == nil; {
		if n, err = r.Body.Read(buffer); err == io.EOF {
			done, err = true, nil
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			continue
		}

		req.Content = buffer[:n]
		if err = client.Send(req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if err == nil {
		if resp, err = client.CloseAndRecv(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if err != nil {
		if tr, ok := trace.FromContext(r.Context()); ok {
			tr.LazyPrintf("evaluating request: %v", err)
			tr.SetError()
		}
		return
	}
	h.writeAppendResponse(w, r, resp)
}

func (h *HTTPGateway) parseAppendRequest(r *http.Request) (*pb.AppendRequest, error) {
	var schema struct{}

	var err error
	if err = r.ParseForm(); err == nil {
		err = h.decoder.Decode(&schema, r.Form)
	}

	var req = &pb.AppendRequest{
		Journal: pb.Journal(r.URL.Path[1:]),
	}
	if err == nil {
		err = req.Validate()
	}
	return req, err
}

func (h *HTTPGateway) parseReadRequest(r *http.Request) (*pb.ReadRequest, error) {
	var schema struct {
		Offset int64
		Block  bool
	}

	var err error
	if err = r.ParseForm(); err == nil {
		err = h.decoder.Decode(&schema, r.Form)
	}

	var req = &pb.ReadRequest{
		Journal:      pb.Journal(r.URL.Path[1:]),
		Offset:       schema.Offset,
		Block:        schema.Block,
		MetadataOnly: r.Method == "HEAD",
	}
	if err == nil {
		err = req.Validate()
	}
	return req, err
}

func (h *HTTPGateway) writeAppendResponse(w http.ResponseWriter, r *http.Request, resp *pb.AppendResponse) {
	if resp.Header != nil {
		w.Header().Set(RouteTokenHeader, proto.CompactTextString(&resp.Header.Route))
	}
	if resp.Commit != nil {
		w.Header().Add(CommitBeginHeader, strconv.FormatInt(resp.Commit.Begin, 10))
		w.Header().Add(CommitEndHeader, strconv.FormatInt(resp.Commit.End, 10))
		w.Header().Add(WriteHeadHeader, strconv.FormatInt(resp.Commit.End, 10))

		var digest = resp.Commit.Sum.ToDigest()
		w.Header().Add(CommitSumHeader, hex.EncodeToString(digest[:]))
	}

	switch resp.Status {
	case pb.Status_OK:
		w.WriteHeader(http.StatusNoContent) // 204.
	case pb.Status_JOURNAL_NOT_FOUND:
		w.WriteHeader(http.StatusNotFound) // 404.
	default:
		http.Error(w, resp.Status.String(), http.StatusInternalServerError) // 500.
	}
}

func (h *HTTPGateway) writeReadResponse(w http.ResponseWriter, r *http.Request, resp *pb.ReadResponse) {
	if resp.Header != nil {
		w.Header().Set(RouteTokenHeader, proto.CompactTextString(&resp.Header.Route))
	}
	if resp.Fragment != nil {
		w.Header().Add(FragmentNameHeader, resp.Fragment.ContentName())

		if !resp.Fragment.ModTime.IsZero() {
			w.Header().Add(FragmentLastModifiedHeader, resp.Fragment.ModTime.Format(http.TimeFormat))
		}
		if resp.FragmentUrl != "" {
			w.Header().Add(FragmentLocationHeader, resp.FragmentUrl)
		}
	}
	if resp.WriteHead != 0 {
		w.Header().Add(WriteHeadHeader, strconv.FormatInt(resp.WriteHead, 10))
	}

	switch resp.Status {
	case pb.Status_OK:
		w.WriteHeader(http.StatusPartialContent) // 206.
	case pb.Status_JOURNAL_NOT_FOUND:
		http.Error(w, resp.Status.String(), http.StatusNotFound) // 404.
	case pb.Status_INSUFFICIENT_JOURNAL_BROKERS:
		http.Error(w, resp.Status.String(), http.StatusServiceUnavailable) // 503.
	case pb.Status_OFFSET_NOT_YET_AVAILABLE:
		http.Error(w, resp.Status.String(), http.StatusRequestedRangeNotSatisfiable) // 416.
	default:
		http.Error(w, resp.Status.String(), http.StatusInternalServerError) // 500.
	}
}

const (
	FragmentLastModifiedHeader = "X-Fragment-Last-Modified"
	FragmentLocationHeader     = "X-Fragment-Location"
	FragmentNameHeader         = "X-Fragment-Name"
	RouteTokenHeader           = "X-Route-Token"

	WriteHeadHeader   = "X-Write-Head"
	CommitBeginHeader = "X-Commit-Begin"
	CommitEndHeader   = "X-Commit-End"
	CommitSumHeader   = "X-Commit-SHA1-Sum"
)
