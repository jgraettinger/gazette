package stores

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/schema"

	"github.com/LiveRamp/gazette/pkg/fragment"
	"github.com/LiveRamp/gazette/pkg/keepalive"
	"github.com/LiveRamp/gazette/pkg/protocol"
)

type s3StoreArgs struct {
	// ACL applied when persisting new fragments. By default, this is
	// s3.ObjectCannedACLBucketOwnerFullControl.
	ACL string
}

type s3Store struct {
	args s3StoreArgs
	bucket, prefix string
	client *s3.S3
}

func newS3Store(ep *url.URL) (*s3Store, error) {
	// Rely on the AWS SDK default credential chain.
	var config = aws.NewConfig()

	var args s3StoreArgs

	if err := parseStoreArgs(ep, &args); err != nil {
		return nil, err
	}

	// Support AWS_ENDPOINT environment variable override.
	if e := os.Getenv("AWS_ENDPOINT"); e != "" {
		config.Endpoint = aws.String(e)
		config.S3ForcePathStyle = aws.Bool(true)
	}

	var sess, err = session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("constructing S3 session: %s", err)
	}
	return &s3Store{
		args: args,
		bucket: ep.Host,
		prefix: ep.Path,
		client: s3.New(sess),
	}, nil
}

func (s *s3Store) SignURL(fragment protocol.Fragment, dur time.Duration) (*url.URL, error) {
	var params = s3.GetObjectInput{
		Bucket: aws.String(s.ep.Host),
		Key:    aws.String(path.Join(s.ep.Path, fragment.ContentPath())),
	}




	req, _ := fs.svc().GetObjectRequest(&params)
	if urlStr, err := req.Presign(validFor); err != nil {
		return nil, fmt.Errorf("s3 presign: %s", err)
	} else if urlObj, err := url.Parse(urlStr); err != nil {
		return nil, fmt.Errorf("s3 url parse: %s", err)
	} else {
		return urlObj, nil
	}

}

func (s *s3Store) Open(context.Context, protocol.Fragment) (io.ReadCloser, error) {}

func (s *s3Store) Persist(context.Context, fragment.Spool) error {}

func (s *s3Store) List(ctx context.Context, prefix string, callback func(protocol.Fragment) error) error {

}

func getS3ConfigProperties() (S3Properties, error) {
	return S3Properties{
		AWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		S3Region:           os.Getenv("AWS_DEFAULT_REGION"),
		S3GlobalCannedACL:  s3.ObjectCannedACLBucketOwnerFullControl,
		S3SSEAlgorithm:     "",
	}, nil
}

var (
// S3 files with Content-Encoding: gzip will get transparently decompressed
// with the default http transport, a behavior that we have to manually disable.
var s3HTTPClient = &http.Client{
	Transport: &http.Transport{
		DialContext:   keepalive.Dialer.DialContext,
		DisableCompression: true,
	},
}
)
