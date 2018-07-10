package codecs

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/gzip"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

// Decompressor is a ReadCloser where Close closes and releases Decompressor
// state, but does not Close or affect the underlying Reader.
type Decompressor io.ReadCloser

// Compressor is a WriteCloser where Close closes and releases Compressor
// state, potentially flushing final content to the underlying Writer,
// but does not Close or otherwise affect the underlying Writer.
type Compressor io.WriteCloser

func NewCodecReader(r io.Reader, codec pb.CompressionCodec) (Decompressor, error) {
	switch codec {
	case pb.CompressionCodec_NONE, pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION:
		return ioutil.NopCloser(r), nil
	case pb.CompressionCodec_GZIP:
		return gzip.NewReader(r)
	case pb.CompressionCodec_SNAPPY:
		return ioutil.NopCloser(snappy.NewReader(r)), nil
	case pb.CompressionCodec_ZSTANDARD:
		return zstdNewReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported codec %s", codec.String())
	}
}

func NewCodecWriter(w io.Writer, codec pb.CompressionCodec) (Compressor, error) {
	switch codec {
	case pb.CompressionCodec_NONE:
		return nopWriteCloser{w}, nil
	case pb.CompressionCodec_GZIP:
		return gzip.NewWriter(w), nil
	case pb.CompressionCodec_SNAPPY:
		return snappy.NewBufferedWriter(w), nil
	case pb.CompressionCodec_ZSTANDARD:
		return zstdNewWriter(w), nil
	default:
		return nil, fmt.Errorf("unsupported codec %s", codec.String())
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

var (
	zstdNewReader = func(io.Reader) io.ReadCloser { panic("ZSTANDARD was not enabled at compile time") }
	zstdNewWriter = func(io.Writer) io.WriteCloser { panic("ZSTANDARD was not enabled at compile time") }
)
