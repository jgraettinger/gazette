package http_gateway

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/LiveRamp/gazette/pkg/client"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/trace"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type Gateway struct {
	decoder *schema.Decoder
	client  pb.BrokerClient
}

func NewGateway(client pb.BrokerClient) *Gateway {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	return &Gateway{
		decoder: decoder,
		client:  client,
	}
}

func (h *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET", "HEAD":
		h.serveRead(w, r)
	case "PUT":
		h.serveWrite(w, r)
	default:
		http.Error(w, fmt.Sprintf("unknown method: %s", r.Method), http.StatusBadRequest)
	}
}

func (h *Gateway) serveRead(w http.ResponseWriter, r *http.Request) {
	var req, err = h.parseReadRequest(r)

	if err != nil {
		if tr, ok := trace.FromContext(r.Context()); ok {
			tr.LazyPrintf("parsing request: %v", err)
			tr.SetError()
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var stream pb.Broker_ReadClient
	var resp = new(pb.ReadResponse)

	if stream, err = h.client.Read(r.Context(), req); err == nil {
		err = stream.RecvMsg(resp)
	}
	if err != nil {
		log.WithField("err", err).Warn("http_gateway failed to proxy Read request")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		h.writeReadResponse(w, r, resp)
	}

	for {
		if err = stream.RecvMsg(resp); err == io.EOF {
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

func (h *Gateway) serveWrite(w http.ResponseWriter, r *http.Request) {
	var req, err = h.parseAppendRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var append = &client.Append{
		Context: r.Context(),
		Client:  h.client,
		Journal: req.Journal,
	}
	if _, err = io.Copy(append, r.Body); err != nil {
		append.Abort()
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	h.writeAppendResponse(w, r, &append.Response)
}

func (h *Gateway) parseReadRequest(r *http.Request) (*pb.ReadRequest, error) {
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

func (h *Gateway) writeReadResponse(w http.ResponseWriter, r *http.Request, resp *pb.ReadResponse) {
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

func (h *Gateway) parseAppendRequest(r *http.Request) (*pb.AppendRequest, error) {
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

func (h *Gateway) writeAppendResponse(w http.ResponseWriter, r *http.Request, resp *pb.AppendResponse) {
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
