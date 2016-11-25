package mysql

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/meta"
)

var (
	metaStoreTestMySQL meta.GenericMetaStore
	testUser           = "admin"
	testPass           = "admin"
	contentSize        = int64(len("this is my content"))
	contentOid         = "f97e1b2936a56511b3b6efc99011758e4700d60fb1674d31445d1ee40b663f24"
	nonexistingOid     = "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f"
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

func TestMySQLPut(t *testing.T) {
	serr := setupMySQLMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}

	rvPut := &meta.RequestVars{
		Oid:  nonexistingOid,
		Size: 42,
		Repo: testRepo,
	}
	rvGet := &meta.RequestVars{
		Oid: nonexistingOid,
	}

	m, err := metaStoreTestMySQL.Put(rvPut)
	if err != nil {
		t.Errorf("expected put to succeed, got: %s", err)
	}

	if m.Existing {
		t.Errorf("expected meta to not have existed")
	}

	_, err = metaStoreTestMySQL.Get(rvGet)
	if err == nil {
		t.Errorf("expected new put to not be committed yet")
	}

	m, err = metaStoreTestMySQL.GetPending(rvGet)
	if err != nil {
		t.Errorf("expected new put to be pending")
	}

	if m.Oid != nonexistingOid {
		t.Errorf("expected oids to match, got: %s", m.Oid)
	}

	if m.Size != 42 {
		t.Errorf("expected sizes to match, got: %d", m.Size)
	}

	m, err = metaStoreTestMySQL.Commit(rvPut)
	if err != nil {
		t.Errorf("expected commit to succeed, got: %s", err)
	}

	if !m.Existing {
		t.Errorf("expected existing to become true after commit")
	}

	_, err = metaStoreTestMySQL.Get(rvGet)
	if err != nil {
		t.Errorf("expected new put to be committed now")
	}

	if !m.Existing {
		t.Errorf("expected existing to be true for a committed object")
	}

	m, err = metaStoreTestMySQL.Put(rvPut)
	if err != nil {
		t.Errorf("expected putting an duplicate object to succeed, got: %s", err)
	}

	if !m.Existing {
		t.Errorf("expecting existing to be true for a duplicate object")
	}
}

func TestMySQLGet(t *testing.T) {
	serr := setupMySQLMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}

	m, err := metaStoreTestMySQL.Get(&meta.RequestVars{Oid: nonexistingOid})
	if err == nil {
		t.Fatalf("expected get to fail with unknown oid, got: %s", m.Oid)
	}

	m, err = metaStoreTestMySQL.Get(&meta.RequestVars{Oid: contentOid})
	if err != nil {
		t.Fatalf("expected get to succeed, got: %s", err)
	}

	if m.Oid != contentOid {
		t.Errorf("expected to get content oid, got: %s", m.Oid)
	}

	if m.Size != contentSize {
		t.Errorf("expected to get content size, got: %d", m.Size)
	}
}

func TestMySQLAuthenticacte(t *testing.T) {
	// MySQL authentication is currently not implemented
}

func setupMySQLMeta() error {
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

	rv := &meta.RequestVars{Oid: contentOid, Size: contentSize, Repo: testRepo}

	if _, err := metaStoreTestMySQL.Put(rv); err != nil {
		return errors.New(fmt.Sprintf("error seeding mysql test meta store: %s\n", err))
	}
	if _, err := metaStoreTestMySQL.Commit(rv); err != nil {
		return errors.New(fmt.Sprintf("error seeding mysql test meta store: %s\n", err))
	}

	return nil
}
