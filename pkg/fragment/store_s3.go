package fragment

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/keepalive"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
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

func s3SignGET(ep *url.URL, fragment pb.Fragment, d time.Duration) (string, error) {
	var cfg, client, err = s3Client(ep)
	if err != nil {
		return "", err
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.prefix + fragment.ContentPath()),
	}
	var req, _ = client.GetObjectRequest(&getObj)
	return req.Presign(d)
}

func s3Open(ctx context.Context, ep *url.URL, fragment pb.Fragment) (io.ReadCloser, error) {
	var cfg, client, err = s3Client(ep)
	if err != nil {
		return nil, err
	}

	var getObj = s3.GetObjectInput{
		Bucket: aws.String(cfg.bucket),
		Key:    aws.String(cfg.prefix + fragment.ContentPath()),
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
		Key:    aws.String(cfg.prefix + spool.ContentPath()),
	}
	if _, err = client.HeadObjectWithContext(ctx, &headObj); err == nil {
		return nil // Spool already persisted. We're done.
	} else if awsErr, ok := err.(awserr.RequestFailure); !ok || awsErr.StatusCode() != http.StatusNotFound {
		return err
	}

	var putObj = s3.PutObjectInput{
		Bucket:               headObj.Bucket,
		Key:                  headObj.Key,
		ServerSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
	}

	if spool.CompressionCodec != pb.CompressionCodec_NONE {
		// Ensure |compressedFile| is ready. This is a no-op if compressed incrementally.
		spool.finishCompression()
		putObj.Body = io.NewSectionReader(spool.compressedFile, 0, spool.compressedLength)
	} else {
		putObj.Body = io.NewSectionReader(spool.File, 0, spool.ContentLength())
	}

	if cfg.ACL != "" {
		putObj.ACL = aws.String(cfg.ACL)
	}
	if cfg.StorageClass != "" {
		putObj.StorageClass = aws.String(cfg.StorageClass)
	}
	if spool.CompressionCodec == pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
		putObj.ContentEncoding = aws.String("gzip")
	}

	_, err = client.PutObjectWithContext(ctx, &putObj)
	return err
}

func s3List(ctx context.Context, store pb.FragmentStore, ep *url.URL, prefix string, callback func(pb.Fragment)) error {
	var cfg, client, err = s3Client(ep)
	if err != nil {
		return err
	}

	var list = s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.bucket),
		Prefix: aws.String(cfg.prefix + prefix),
	}
	var strip = len(cfg.prefix)

	return client.ListObjectsV2PagesWithContext(ctx, &list, func(objs *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range objs.Contents {

			if frag, err := pb.ParseContentPath((*obj.Key)[strip:]); err != nil {
				log.WithFields(log.Fields{"bucket": cfg.bucket, "key": *obj.Key, "err": err}).Warning("parsing fragment")
			} else if *obj.Size == 0 && frag.ContentLength() > 0 {
				log.WithFields(log.Fields{"obj": obj}).Warning("zero-length fragment")
			} else {
				frag.ModTime = *obj.LastModified
				frag.BackingStore = store
				callback(frag)
			}
		}
		return true
	})
}

func s3Client(ep *url.URL) (cfg s3Cfg, client *s3.S3, err error) {
	if err = parseStoreArgs(ep, &cfg); err != nil {
		return
	}
	// Omit leading slash from bucket prefix. Note that FragmentStore already
	// enforces that URL Paths end in '/'.
	cfg.bucket, cfg.prefix = ep.Host, ep.Path[1:]

	defer s3ClientsMu.Unlock()
	s3ClientsMu.Lock()

	var key = [2]string{cfg.Endpoint, cfg.Profile}
	if client = s3Clients[key]; client != nil {
		return
	}

	var awsConfig = aws.NewConfig()

	// Override the default http.Transport's behavior of inserting
	// "Accept-Encoding: gzip" and transparently decompressing client-side.
	awsConfig.WithHTTPClient(&http.Client{
		Transport: &http.Transport{
			DialContext:        keepalive.Dialer.DialContext,
			DisableCompression: true,
		},
	})

	if cfg.Endpoint != "" {
		awsConfig = aws.NewConfig()
		awsConfig.Endpoint = aws.String(cfg.Endpoint)
		// We must force path style because bucket-named virtual hosts
		// are not compatible with explicit endpoints.
		awsConfig.S3ForcePathStyle = aws.Bool(true)
	}

	if cfg.Profile != "" {
		awsConfig.Credentials = credentials.NewSharedCredentials("", cfg.Profile)
	}

	var awsSession *session.Session
	if awsSession, err = session.NewSession(awsConfig); err != nil {
		err = fmt.Errorf("constructing S3 session: %s", err)
		return
	}

	client = s3.New(awsSession)
	s3Clients[key] = client

	return
}

var (
	s3Clients   = make(map[[2]string]*s3.S3)
	s3ClientsMu sync.Mutex
)
