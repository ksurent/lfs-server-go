package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/logger"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

const (
	ContentType = "binary/octet-stream"
)

// AwsContentStore provides a simple file system based storage.
type AwsContentStore struct {
	client  *s3.S3
	bucket  *s3.Bucket
	authId  string
	authKey string
	acl     s3.ACL
}

// NewContentStore creates a ContentStore at the base directory.
func NewAwsContentStore() (*AwsContentStore, error) {
	os.Setenv("AWS_ACCESS_KEY_ID", config.Config.Aws.AccessKeyId)
	os.Setenv("AWS_SECRET_ACCESS_KEY", config.Config.Aws.SecretAccessKey)
	auth, err := aws.EnvAuth()
	if err != nil {
		logger.Log(logger.Kv{"fn": "AwsContentStore.NewAwsContentStore", "err": ": " + err.Error()})
		return &AwsContentStore{}, err
	}
	client := s3.New(auth, aws.Regions[config.Config.Aws.Region])
	bucket := client.Bucket(config.Config.Aws.BucketName)
	self := &AwsContentStore{bucket: bucket, client: client}
	self.makeBucket()
	self.setAcl()
	return self, nil
}

// Make the bucket if it does not exist
func (s *AwsContentStore) makeBucket() error {
	buckets, err := s.bucket.ListBuckets()
	if err != nil {
		logger.Log(logger.Kv{"fn": "AwsContentStore.makeBucket", "err": ": " + err.Error()})
		return err
	}
	var exists bool
	exists = false
	for _, b := range buckets.Buckets {
		if b.Name == s.bucket.Name {
			exists = true
		}
	}
	if !exists {
		err := s.bucket.PutBucket(s3.ACL(config.Config.Aws.BucketAcl))
		return err
	}
	return nil
}

func (s *AwsContentStore) Get(meta *MetaObject) (io.ReadCloser, error) {
	path := transformKey(meta.Oid)
	return s.bucket.GetReader(path)
}

func (s *AwsContentStore) getMetaData(meta *MetaObject) (*s3.Key, error) {
	path := transformKey(meta.Oid)
	return s.bucket.GetKey(path)
}

// TODO: maybe take write errors into account and buffer/resend to amazon?
func (s *AwsContentStore) Put(meta *MetaObject, r io.Reader) error {
	path := transformKey(meta.Oid)
	/*
		There is probably a better way to compute this but we need to write the file to memory to
		 compute the sha256 value and make sure what we're writing is correct.
		 If not, git wont be able to find it later
	*/
	hash := sha256.New()
	buf, _ := ioutil.ReadAll(r)
	hw := io.MultiWriter(hash)
	written, err := io.Copy(hw, bytes.NewReader(buf))
	if err != nil {
		logger.Log(logger.Kv{"fn": "AwsContentStore.Put", "err": ": " + err.Error()})
		return err
	}
	// Check that we've written out the entire file for computing the sha
	if written != meta.Size {
		return errSizeMismatch
	}
	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != meta.Oid {
		return errHashMismatch
	}
	retStat := s.bucket.PutReader(path, bytes.NewReader(buf), meta.Size, ContentType, s.acl)
	k, kerr := s.getMetaData(meta)
	if kerr != nil {
		logger.Log(logger.Kv{"fn": "AwsContentStore.Put", "err": ": " + kerr.Error()})
		return errWriteS3
	}
	if k.Size != meta.Size {
		return errSizeMismatch
	}
	return retStat
}

func (s *AwsContentStore) Exists(meta *MetaObject) bool {
	path := transformKey(meta.Oid)
	// returns a 404 error if its not there
	_, err := s.bucket.GetKey(path)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return false
		} else {
			logger.Log(logger.Kv{"fn": "AwsContentStore.Exists", "err": ": " + err.Error()})
			return false
		}
	}
	// if the object is not there, a 404 error is raised
	return true
}

func (s *AwsContentStore) Verify(meta *MetaObject) error {
	return errNotImplemented
}

func (s *AwsContentStore) setAcl() {
	switch config.Config.Aws.BucketAcl {
	case "private":
		s.acl = s3.Private
	case "public-read":
		s.acl = s3.PublicRead
	case "public-read-write":
		s.acl = s3.PublicReadWrite
	case "authenticated-read":
		s.acl = s3.AuthenticatedRead
	case "bucket-owner-read":
		s.acl = s3.BucketOwnerRead
	case "bucket-owner-full-control":
		s.acl = s3.BucketOwnerFull
	default:
		s.acl = s3.Private
	}
}
