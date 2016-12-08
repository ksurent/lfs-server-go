package fs

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/ksurent/lfs-server-go/meta"
)

func TestContentStorePut(t *testing.T) {
	contentStore, teardown, err := setup()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBuffer([]byte("test content"))

	if err := contentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	if !contentStore.Exists(m) {
		t.Fatalf("expected content to exist after putting")
	}
}

func TestContentStorePutHashMismatch(t *testing.T) {
	contentStore, teardown, err := setup()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBufferString("bogus content")

	if err := contentStore.Put(m, b); err == nil {
		t.Error("expected put with bogus content to fail")
	}

	if contentStore.Exists(m) {
		t.Error("expected content to not exist after putting bogus content")
	}
}

func TestContentStorePutSizeMismatch(t *testing.T) {
	contentStore, teardown, err := setup()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 14,
	}

	b := bytes.NewBufferString("test content")

	if err := contentStore.Put(m, b); err == nil {
		t.Error("expected put with bogus size to fail")
	}

	if contentStore.Exists(m) {
		t.Error("expected content to not exist after putting bogus size")
	}
}

func TestContentStoreGet(t *testing.T) {
	contentStore, teardown, err := setup()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	m := &meta.Object{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBuffer([]byte("test content"))

	if err := contentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	r, err := contentStore.Get(m)
	if err != nil {
		t.Fatalf("expected get to succeed, got: %s", err)
	}

	by, _ := ioutil.ReadAll(r)
	if string(by) != "test content" {
		t.Fatalf("expected to read content, got: %s", string(by))
	}
}

func TestContentStoreGetNonExisting(t *testing.T) {
	contentStore, teardown, err := setup()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	_, err = contentStore.Get(&meta.Object{Oid: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	if err == nil {
		t.Fatalf("expected to get an error, but content existed")
	}
}

func setup() (*ContentStore, func(), error) {
	contentPath := "/tmp/content-store-test"
	store, err := NewContentStore(contentPath)
	if err != nil {
		return nil, nil, err
	}

	teardown := func() {
		os.RemoveAll(contentPath)
	}

	return store, teardown, nil
}
