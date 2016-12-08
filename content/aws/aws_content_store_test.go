package aws

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/meta"
)

var awsContentStore *AwsContentStore

func TestAwsContentStorePut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	awsContentStore, teardown, err := setupAwsTest()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBufferString("test content")
	if err := awsContentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	if err := awsContentStore.Exists(m); !err {
		t.Fatalf("expected content to exist after putting")
	}
}

func TestAwsContentStorePutHashMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	awsContentStore, teardown, err := setupAwsTest()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBufferString("bogus content")
	if err := awsContentStore.Put(m, b); err == nil {
		t.Fatal("expected put with bogus content to fail")
	}
}

func TestAwsContentStorePutSizeMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	awsContentStore, teardown, err := setupAwsTest()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 14,
	}

	b := bytes.NewBufferString("test content")
	if err := awsContentStore.Put(m, b); err == nil {
		t.Fatal("expected put with bogus size to fail")
	}

}

func TestAwsContentStoreGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	awsContentStore, teardown, err := setupAwsTest()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBufferString("test content")
	if err := awsContentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	r, err := awsContentStore.Get(m)
	if err != nil {
		t.Fatalf("expected get to succeed, got: %s", err)
	}

	by, _ := ioutil.ReadAll(r)
	if string(by) != "test content" {
		t.Fatalf("expected to read content, got: %s", string(by))
	}
}

func TestAwsContentStoreGetNonExisting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	awsContentStore, teardown, err := setupAwsTest()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	_, err = awsContentStore.Get(&meta.Object{Oid: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	if err == nil {
		t.Fatalf("expected to get an error, but content existed")
	}
}

func TestAwsContentStoreExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	awsContentStore, teardown, err := setupAwsTest()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	if awsContentStore.Exists(m) {
		t.Fatalf("expected content to not exist yet")
	}

	b := bytes.NewBufferString("test content")
	if err := awsContentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	if !awsContentStore.Exists(m) {
		t.Fatalf("expected content to exist")
	}
}

func setupAwsTest() (*AwsContentStore, func(), error) {
	id := os.Getenv("AWS_ACCESS_KEY_ID")
	key := os.Getenv("AWS_SECRET_ACCESS_KEY")
	region := os.Getenv("AWS_REGION")

	if id == "" || key == "" || region == "" {
		return nil, nil, errors.New("no AWS credentials")
	}

	store, err := NewAwsContentStore(&config.AwsConfig{
		Enabled:         true,
		AccessKeyId:     id,
		SecretAccessKey: key,
		Region:          region,
		BucketName:      "lfs-server-go-objects-test",
		BucketAcl:       "bucket-owner-full-control",
	})
	if err != nil {
		return nil, nil, err
	}

	teardown := func() {
		items, err := store.bucket.List("", "", "", 1000)
		if err != nil {
			return
		}

		var delItems []string
		for _, item := range items.Contents {
			delItems = append(delItems, item.Key)
		}

		if len(delItems) > 0 {
			err = store.bucket.MultiDel(delItems)
			if err != nil {
				return
			}
		}

		store.bucket.DelBucket()
	}

	return store, teardown, nil
}
