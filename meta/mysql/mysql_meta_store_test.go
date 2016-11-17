package mysql

import (
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/ksurent/lfs-server-go/config"
	m "github.com/ksurent/lfs-server-go/meta"
)

var (
	metaStoreTestMySQL m.GenericMetaStore
	testUser           = "admin"
	testPass           = "admin"
	testAuth           = fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(testUser+":"+testPass)))
	badAuth            = fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte("azog:defiler")))
	content            = "this is my content"
	contentSize        = int64(len(content))
	contentOid         = "f97e1b2936a56511b3b6efc99011758e4700d60fb1674d31445d1ee40b663f24"
	nonexistingOid     = "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f"
	noAuthcontent      = "Some content goes here"
	noAuthContentSize  = int64(len(noAuthcontent))
	noAuthOid          = "4609ed10888c145d228409aa5587bab9fe166093bb7c155491a96d079c9149be"
	extraRepo          = "mytestproject"
	testRepo           = "repo"
)

func TestMySQLConfiguration(t *testing.T) {
	config.Config.MySQL = &config.MySQLConfig{
		Enabled:  true,
		Host:     "127.0.0.1:3306",
		Database: "lfs_server_go_test",
	}

	db, err := NewMySQLSession()
	if err == nil {
		db.Close()
		t.Errorf("expected validation error")
	}
}

func TestMySQLAddProjects(t *testing.T) {
	serr := setupMySQLMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}

	err := metaStoreTestMySQL.AddProject(extraRepo)

	if err != nil {
		t.Errorf("expected AddProject to succeed, got : %s", err)
	}
}

func TestMySQLPutWithAuth(t *testing.T) {
	serr := setupMySQLMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}

	rvPut := &m.RequestVars{
		Authorization: testAuth,
		Oid:           nonexistingOid,
		Size:          42,
		Repo:          testRepo,
	}
	rvGet := &m.RequestVars{
		Authorization: testAuth,
		Oid:           nonexistingOid,
	}

	meta, err := metaStoreTestMySQL.Put(rvPut)
	if err != nil {
		t.Errorf("expected put to succeed, got: %s", err)
	}

	if meta.Existing {
		t.Errorf("expected meta to not have existed")
	}

	_, err = metaStoreTestMySQL.Get(rvGet)
	if err == nil {
		t.Errorf("expected new put to not be committed yet")
	}

	meta, err = metaStoreTestMySQL.GetPending(rvGet)
	if err != nil {
		t.Errorf("expected new put to be pending")
	}

	if meta.Oid != nonexistingOid {
		t.Errorf("expected oids to match, got: %s", meta.Oid)
	}

	if meta.Size != 42 {
		t.Errorf("expected sizes to match, got: %d", meta.Size)
	}

	meta, err = metaStoreTestMySQL.Commit(rvPut)
	if err != nil {
		t.Errorf("expected commit to succeed, got: %s", err)
	}

	if !meta.Existing {
		t.Errorf("expected existing to become true after commit")
	}

	_, err = metaStoreTestMySQL.Get(rvGet)
	if err != nil {
		t.Errorf("expected new put to be committed now")
	}

	if !meta.Existing {
		t.Errorf("expected existing to be true for a committed object")
	}

	meta, err = metaStoreTestMySQL.Put(rvPut)
	if err != nil {
		t.Errorf("expected putting an duplicate object to succeed, got: %s", err)
	}

	if !meta.Existing {
		t.Errorf("expecting existing to be true for a duplicate object")
	}
}

func TestMySQLPutWithoutAuth(t *testing.T) {
	serr := setupMySQLMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}

	_, err := metaStoreTestMySQL.Put(&m.RequestVars{
		Authorization: badAuth,
		User:          testUser,
		Oid:           contentOid,
		Size:          42,
		Repo:          testRepo,
	})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}

	_, err = metaStoreTestMySQL.Put(&m.RequestVars{
		User: testUser,
		Oid:  contentOid,
		Size: 42,
		Repo: testRepo,
	})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}
}

func TestMySQLGetWithAuth(t *testing.T) {
	serr := setupMySQLMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}

	meta, err := metaStoreTestMySQL.Get(&m.RequestVars{Authorization: testAuth, Oid: noAuthOid})
	if err == nil {
		t.Fatalf("expected get to fail with unknown oid, got: %s", meta.Oid)
	}

	meta, err = metaStoreTestMySQL.Get(&m.RequestVars{Authorization: testAuth, Oid: contentOid})
	if err != nil {
		t.Fatalf("expected get to succeed, got: %s", err)
	}

	if meta.Oid != contentOid {
		t.Errorf("expected to get content oid, got: %s", meta.Oid)
	}

	if meta.Size != contentSize {
		t.Errorf("expected to get content size, got: %d", meta.Size)
	}
}

func TestMySQLGetWithoutAuth(t *testing.T) {
	serr := setupMySQLMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}

	_, err := metaStoreTestMySQL.Get(&m.RequestVars{Authorization: badAuth, Oid: noAuthOid})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}

	_, err = metaStoreTestMySQL.Get(&m.RequestVars{Oid: noAuthOid})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}
}

func setupMySQLMeta() error {
	// Setup config.Config
	config.Config.Ldap = &config.LdapConfig{Enabled: true, Server: "ldap://localhost:1389", Base: "o=company",
		UserObjectClass: "posixaccount", UserCn: "uid", BindPass: "admin"}
	config.Config.MySQL = &config.MySQLConfig{
		Enabled:  true,
		Host:     "127.0.0.1:3306",
		Username: "lfs_server",
		Password: "pass123",
		Database: "lfs_server_go",
	}

	mysqlStore, err := NewMySQLMetaStore()
	if err != nil {
		return errors.New(fmt.Sprintf("error initializing test meta store: %s\n", err))
	}

	metaStoreTestMySQL = mysqlStore

	// Clean up any test
	mysqlStore.client.Exec("TRUNCATE TABLE oid_maps")
	mysqlStore.client.Exec("TRUNCATE TABLE oids")
	mysqlStore.client.Exec("TRUNCATE TABLE projects")

	rv := &m.RequestVars{Authorization: testAuth, Oid: contentOid, Size: contentSize, Repo: testRepo}

	if _, err := metaStoreTestMySQL.Put(rv); err != nil {
		return errors.New(fmt.Sprintf("error seeding mysql test meta store: %s\n", err))
	}
	if _, err := metaStoreTestMySQL.Commit(rv); err != nil {
		return errors.New(fmt.Sprintf("error seeding mysql test meta store: %s\n", err))
	}

	return nil
}
