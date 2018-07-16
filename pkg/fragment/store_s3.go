package fragment

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/pkg/keepalive"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	//"github.com/LiveRamp/gazette/pkg/keepalive"
	"github.com/LiveRamp/gazette/pkg/protocol"
)

type s3Cfg struct {
	bucket string
	prefix string

	// AWS Profile to extract credentials from the shared credentials file.
	// For details, see:
	//   https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/
	// If empty, the default credentials are used.
	Profile string
	// Endpoint to connect to S3. If empty, the default S3 service is used.
	Endpoint string

	// ACL applied when persisting new fragments. By default, this is
	// s3.ObjectCannedACLBucketOwnerFullControl.
	ACL string
	// Storage class applied when persisting new fragments. By default,
	// this is s3.ObjectStorageClassStandard.
	StorageClass string
}

func s3SignGET(ep *url.URL, fragment protocol.Fragment, d time.Duration) (string, error) {
	var cfg, client, err = s3Client(ep)
	if err != nil {
		return "", err
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(path.Join(cfg.prefix, fragment.ContentPath())),
	}
	var req, _ = client.GetObjectRequest(&getObj)
	return req.Presign(d)
}

func s3Open(ctx context.Context, ep *url.URL, fragment protocol.Fragment) (io.ReadCloser, error) {
	var cfg, client, err = s3Client(ep)
	if err != nil {
		return nil, err
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(path.Join(cfg.prefix, fragment.ContentPath())),
	}
	if resp, err := client.GetObjectWithContext(ctx, &getObj); err != nil {
		return nil, err
	} else {
		return resp.Body, nil
	}
}

func s3Persist(ctx context.Context, ep *url.URL, spool Spool) error {
	var cfg, client, err = s3Client(ep)
	if err != nil {
		return err
	}

	// First test whether the Spool has been uploaded by another broker.
	var headObj = s3.HeadObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(path.Join(cfg.prefix, spool.ContentPath())),
	}
	if _, err = client.HeadObjectWithContext(ctx, &headObj); err == nil {
		return nil // Spool already persisted.
	} else if awsErr, ok := err.(awserr.RequestFailure); !ok || awsErr.StatusCode() != http.StatusNotFound {
		return err
	}

	var putObj = s3.PutObjectInput{
		Bucket:               headObj.Bucket,
		Key:                  headObj.Key,
		ServerSideEncryption: aws.String(s3.ServerSideEncryptionAes256),

		Body: spool.compressedFile,
	}
	if cfg.ACL != "" {
		putObj.ACL = aws.String(cfg.ACL)
	}
	if cfg.StorageClass != "" {
		putObj.StorageClass = aws.String(cfg.StorageClass)
	}
	if spool.CompressionCodec == protocol.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		putObj.ContentEncoding = aws.String("gzip")
	}

	client.PutObjectWithContext()

	return nil
}

func (s *s3Store) List(ctx context.Context, prefix string, callback func(protocol.Fragment) error) error {

}

func s3Client(ep *url.URL) (cfg s3Cfg, client *s3.S3, err error) {
	err = parseStoreArgs(ep, &cfg)
	cfg.bucket, cfg.prefix = ep.Host, ep.Path

	defer s3ClientsMu.Unlock()
	s3ClientsMu.Lock()

	var key = [2]string{cfg.Endpoint, cfg.Profile}
	if client = s3Clients[key]; client != nil {
		return
	}

	var awsConfig = aws.NewConfig()

	if cfg.Endpoint != "" {
		awsConfig.Endpoint = aws.String(cfg.Endpoint)
		// We must force path style because bucket-named virtual hosts
		// are not compatible with explicit endpoints.
		awsConfig.S3ForcePathStyle = aws.Bool(true)
	}

	// Override the default http.Transport's behavior of inserting
	// "Accept-Encoding: gzip" and transparently decompressing client-side.
	awsConfig.WithHTTPClient(&http.Client{
		Transport: &http.Transport{
			DialContext:        keepalive.Dialer.DialContext,
			DisableCompression: true,
		},
	})

	var awsSession *session.Session
	if awsSession, err = session.NewSession(awsConfig, &aws.Config{
		Credentials: credentials.NewSharedCredentials("", cfg.Profile),
	}); err != nil {
		err = fmt.Errorf("constructing S3 session: %s", err)
		return
	}

	client = s3.New(awsSession)
	s3Clients[key] = client

	return
}

var (
	s3Clients   map[[2]string]*s3.S3
	s3ClientsMu sync.Mutex
)
