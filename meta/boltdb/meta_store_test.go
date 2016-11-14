package boltdb

import (
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/meta"
)

var (
	metaStoreTest     meta.GenericMetaStore
	testUser          = "admin"
	testPass          = "admin"
	testAuth          = fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(testUser+":"+testPass)))
	badAuth           = fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte("azog:defiler")))
	content           = "this is my content"
	contentSize       = int64(len(content))
	contentOid        = "f97e1b2936a56511b3b6efc99011758e4700d60fb1674d31445d1ee40b663f24"
	nonexistingOid    = "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f"
	noAuthcontent     = "Some content goes here"
	noAuthContentSize = int64(len(noAuthcontent))
	noAuthOid         = "4609ed10888c145d228409aa5587bab9fe166093bb7c155491a96d079c9149be"
	extraRepo         = "mytestproject"
	testRepo          = "repo"
)

func TestGetWithAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	m, err := metaStoreTest.Get(&meta.RequestVars{Authorization: testAuth, Oid: contentOid})
	if err != nil {
		t.Fatalf("Error retreiving meta: %s", err)
	}

	if m.Oid != contentOid {
		t.Errorf("expected to get content oid, got: %s", m.Oid)
	}

	if m.Size != contentSize {
		t.Errorf("expected to get content size, got: %d", m.Size)
	}
}

func TestGetWithoutAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	_, err := metaStoreTest.Get(&meta.RequestVars{Authorization: badAuth, Oid: contentOid})
	if !meta.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}
}

func TestPutWithAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	getRv := &meta.RequestVars{Authorization: testAuth, Oid: nonexistingOid}

	putRv := &meta.RequestVars{Authorization: testAuth, Oid: nonexistingOid, Size: 42}

	m, err := metaStoreTest.Put(putRv)
	if err != nil {
		t.Errorf("expected put to succeed, got : %s", err)
	}

	if m.Existing {
		t.Errorf("expected meta to not have existed")
	}

	_, err = metaStoreTest.Get(getRv)
	if err == nil {
		t.Errorf("expected new put to not be committed yet")
	}

	_, err = metaStoreTest.GetPending(getRv)
	if err != nil {
		t.Errorf("expected to be able to retrieve pending put, got: %s", err)
	}

	if m.Oid != nonexistingOid {
		t.Errorf("expected oids to match, got: %s", m.Oid)
	}

	if m.Size != 42 {
		t.Errorf("expected sizes to match, got: %d", m.Size)
	}

	m, err = metaStoreTest.Commit(putRv)

	if !m.Existing {
		t.Errorf("expected existing to become true after commit")
	}

	_, err = metaStoreTest.Get(getRv)
	if err != nil {
		t.Errorf("expected new put to be committed now, got: %s", err)
	}

	if !m.Existing {
		t.Errorf("expected existing to be true for a committed object")
	}

	m, err = metaStoreTest.Put(putRv)
	if err != nil {
		t.Errorf("expected putting a duplicate object to succeed, got: %s", err)
	}

	if !m.Existing {
		t.Errorf("expecting existing to be true for a duplicate object")
	}
}

func TestPuthWithoutAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	_, err := metaStoreTest.Put(&meta.RequestVars{Authorization: badAuth, Oid: contentOid, Size: 42})
	if !meta.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}
}

func setupMeta() {
	config.Config.Ldap.Enabled = false
	store, err := NewMetaStore("test-meta-store.db")
	if err != nil {
		fmt.Printf("error initializing test meta store: %s\n", err)
		os.Exit(1)
	}

	metaStoreTest = store
	if err := metaStoreTest.AddUser(testUser, testPass); err != nil {
		teardownMeta()
		fmt.Printf("error adding test user to meta store: %s\n", err)
		os.Exit(1)
	}

	rv := &meta.RequestVars{Authorization: testAuth, Oid: contentOid, Size: contentSize}

	if _, err := metaStoreTest.Put(rv); err != nil {
		teardownMeta()
		fmt.Printf("error seeding test meta store: %s\n", err)
		os.Exit(1)
	}
	if _, err := metaStoreTest.Commit(rv); err != nil {
		teardownMeta()
		fmt.Printf("error seeding test meta store: %s\n", err)
		os.Exit(1)
	}
}

func teardownMeta() {
	os.RemoveAll("test-meta-store.db")
}
