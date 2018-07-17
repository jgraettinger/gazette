package http_gateway

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
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
	var reader = client.NewReader(r.Context(), h.client, req)

	if _, err = reader.Read(nil); err != nil && reader.Response.Status == pb.Status_OK {
		if r.Context().Err() != nil {
			http.Error(w, err.Error(), http.StatusRequestTimeout) // Request was aborted by client.
		} else {
			log.WithField("err", err).Warn("http_gateway: failed to proxy Read request")
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	writeReadResponse(w, r, reader.Response)

	if reader.Response.Status != pb.Status_OK {
		return
	}
	if _, err = io.Copy(flushWriter{w}, reader); err != nil &&
		r.Context().Err() == nil &&
		reader.Response.Status == pb.Status_OK {
		log.WithField("err", err).Warn("http_gateway: failed to proxy Read response")
	}
}

func (h *Gateway) serveWrite(w http.ResponseWriter, r *http.Request) {
	var req, err = h.parseAppendRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var appender = client.NewAppender(r.Context(), h.client, req)
	if _, err = io.Copy(appender, r.Body); err == nil {
		err = appender.Close()
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		appender.Abort()
		return
	}
	writeAppendResponse(w, r, appender.Response)
}

func (h *Gateway) parseReadRequest(r *http.Request) (pb.ReadRequest, error) {
	var schema struct {
		Offset int64
		Block  bool
	}
	var q url.Values
	var err error

	if q, err = url.ParseQuery(r.URL.RawQuery); err == nil {
		err = h.decoder.Decode(&schema, q)
	}
	var req = pb.ReadRequest{
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

func (h *Gateway) parseAppendRequest(r *http.Request) (pb.AppendRequest, error) {
	var schema struct{}
	var q url.Values
	var err error

	if q, err = url.ParseQuery(r.URL.RawQuery); err == nil {
		err = h.decoder.Decode(&schema, q)
	}
	var req = pb.AppendRequest{
		Journal: pb.Journal(r.URL.Path[1:]),
	}
	if err == nil {
		err = req.Validate()
	}
	return req, err
}

func writeReadResponse(w http.ResponseWriter, r *http.Request, resp pb.ReadResponse) {
	if resp.Header != nil {
		writeHeader(w, r, resp.Header)
	}
	if resp.Fragment != nil {
		w.Header().Add(FragmentNameHeader, resp.Fragment.ContentName())

		if !resp.Fragment.ModTime.IsZero() {
			w.Header().Add(FragmentLastModifiedHeader, resp.Fragment.ModTime.UTC().Format(http.TimeFormat))
		}
		if resp.FragmentUrl != "" {
			w.Header().Add(FragmentLocationHeader, resp.FragmentUrl)
		}
		w.Header().Add("Content-Range", fmt.Sprintf("bytes %v-%v/%v", resp.Offset,
			math.MaxInt64, math.MaxInt64))
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

func writeAppendResponse(w http.ResponseWriter, r *http.Request, resp pb.AppendResponse) {
	if resp.Header != nil {
		writeHeader(w, r, resp.Header)
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

func writeHeader(w http.ResponseWriter, r *http.Request, h *pb.Header) {
	if len(h.Route.Brokers) != 0 {
		w.Header().Set(RouteTokenHeader, proto.CompactTextString(&h.Route))
	}

	// If there is a primary broker, add a Location header which would direct
	// further requests of this journal to the primary.
	if len(h.Route.Brokers) != 0 && h.Route.Primary != -1 {
		var loc = h.Route.Endpoints[h.Route.Primary].URL()

		loc.Path = path.Join(loc.Path, r.URL.Path)
		w.Header().Add("Location", loc.String())
	}
}

type flushWriter struct{ io.Writer }

func (fw flushWriter) Write(p []byte) (n int, err error) {
	n, err = fw.Writer.Write(p)

	if flusher, ok := fw.Writer.(http.Flusher); ok {
		flusher.Flush()
	}
	return
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
