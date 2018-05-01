package protocol

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path"
	"strconv"
	"strings"
)

// ContentName returns the content-addressed base file name of this Fragment.
func (m *Fragment) ContentName() string {
	return fmt.Sprintf("%016x-%016x-%x%s", m.Begin, m.End, m.Sum.ToDigest(), m.CompressionCodec.ToExtension())
}

// ContentPath returns the content-addressed path of this Fragment.
func (m *Fragment) ContentPath() string { return m.Journal.String() + "/" + m.ContentName() }

// ContentLength returns the number of content bytes contained in this Fragment.
// If compression is used, this will differ from the file size of the Fragment.
func (m *Fragment) ContentLength() int64 { return m.End - m.Begin }

// Validate returns an error if the Fragment is not well-formed.
func (m *Fragment) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return ExtendContext(err, "Journal")
	} else if m.Begin > m.End {
		return NewValidationError("expected Begin <= End (have %d, %d)", m.Begin, m.End)
	} else if err = m.CompressionCodec.Validate(); err != nil {
		return ExtendContext(err, "CompressionCodec")
	}
	return nil
}

// ParseContentPath parses a ContentPath into a Fragment, or returns an error.
func ParseContentPath(p string) (Fragment, error) {
	return ParseContentName(Journal(path.Dir(p)), path.Base(p))
}

// ParseContentName parses a Journal and ContentName into a Fragment, or returns an error.
func ParseContentName(journal Journal, name string) (Fragment, error) {
	var f Fragment

	var ext = path.Ext(name)
	name = name[:len(name)-len(ext)]

	if fields := strings.Split(name, "-"); len(fields) != 3 {
		return Fragment{}, NewValidationError("wrong Fragment format: %v", name)
	} else if begin, err := strconv.ParseInt(fields[0], 16, 64); err != nil {
		return Fragment{}, ExtendContext(&ValidationError{Err: err}, "Begin")
	} else if end, err := strconv.ParseInt(fields[1], 16, 64); err != nil {
		return Fragment{}, ExtendContext(&ValidationError{Err: err}, "End")
	} else if sum, err := hex.DecodeString(fields[2]); err != nil {
		return Fragment{}, ExtendContext(&ValidationError{Err: err}, "Sum")
	} else if len(sum) != sha1.Size {
		return Fragment{}, NewValidationError("invalid SHA1Sum length: %x", sum)
	} else if cc, err := CompressionCodecFromExtension(ext); err != nil {
		return Fragment{}, err
	} else {
		f = Fragment{
			Journal:          journal,
			Begin:            begin,
			End:              end,
			Sum:              SHA1SumFromDigest(sum),
			CompressionCodec: cc,
		}
	}
	return f, f.Validate()
}

// SHA1SumFromDigest converts SHA1 sum in digest form into a SHA1Sum.
// |r| must have the length of a SHA1 digest (20 bytes), or it panics.
func SHA1SumFromDigest(r []byte) SHA1Sum {
	if len(r) != 20 {
		panic("invalid slice length")
	}
	var m SHA1Sum
	m.Part1 = binary.BigEndian.Uint64(r[0:8])
	m.Part2 = binary.BigEndian.Uint64(r[8:16])
	m.Part3 = binary.BigEndian.Uint32(r[16:20])
	return m
}

func (m *SHA1Sum) ToDigest() (r [20]byte) {
	binary.BigEndian.PutUint64(r[0:8], m.GetPart1())
	binary.BigEndian.PutUint64(r[8:16], m.GetPart2())
	binary.BigEndian.PutUint32(r[16:20], m.GetPart3())
	return
}

// CompressionCodecFromExtension matches a file extension to its corresponding CompressionCodec.
func CompressionCodecFromExtension(ext string) (CompressionCodec, error) {
	switch strings.ToLower(ext) {
	case "":
		return CompressionCodec_CONTENT_ENCODING, nil
	case ".raw":
		return CompressionCodec_NONE, nil
	case ".gz", ".gzip":
		return CompressionCodec_GZIP, nil
	case ".sz", ".snappy":
		return CompressionCodec_SNAPPY, nil
	case ".zst", ".zstandard":
		return CompressionCodec_ZSTANDARD, nil
	default:
		return CompressionCodec_NONE, NewValidationError("unrecognized compression extension: %s", ext)
	}
}

func (m CompressionCodec) Validate() error {
	if _, ok := CompressionCodec_name[int32(m)]; !ok {
		return NewValidationError("invalid value (%s)", m)
	}
	return nil
}

func (m CompressionCodec) ToExtension() string {
	switch m {
	case CompressionCodec_CONTENT_ENCODING:
		return ""
	case CompressionCodec_NONE:
		return ".raw"
	case CompressionCodec_GZIP:
		return ".gz"
	case CompressionCodec_SNAPPY:
		return ".sz"
	case CompressionCodec_ZSTANDARD:
		return ".zst"
	default:
		panic("invalid CompressionCodec")
	}
}
