package mysql

import (
	"fmt"
	"testing"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/meta"
)

var (
	testUser    = "admin"
	testPass    = "admin"
	contentSize = int64(len("this is my content"))
	contentOid  = "f97e1b2936a56511b3b6efc99011758e4700d60fb1674d31445d1ee40b663f24"
	contentRepo = "repo"
)

func TestPutGet(t *testing.T) {
	testMetaStore, err := setupMeta()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownMeta(testMetaStore)

	rv := &meta.RequestVars{
		Oid:  contentOid,
		Size: contentSize,
		Repo: contentRepo,
	}

	if _, err := testMetaStore.Put(rv); err != nil {
		t.Errorf("expected Put() to succeed, got: %s", err)
	}

	if _, err := testMetaStore.Get(rv); !meta.IsObjectNotFound(err) {
		t.Errorf("expected Get() to return 'not found', got: %s", err)
	}

	m, err := testMetaStore.GetPending(rv)
	if err != nil {
		t.Errorf("expected GetPending() to succeed, got: %s", err)
	} else {
		if m.Oid != contentOid {
			t.Errorf("expected pending object id to be %d, got: %d", contentOid, m.Oid)
		}
		if m.Size != contentSize {
			t.Errorf("expected pending object size to be %d, got: %d", contentSize, m.Size)
		}
		if m.Existing {
			t.Error("expected meta object to be in the pending state")
		}
		if len(m.ProjectNames) != 1 || m.ProjectNames[0] != contentRepo {
			t.Errorf("expected pending object to belong to project %q, got: %v", contentRepo, m.ProjectNames)
		}
	}

	if _, err := testMetaStore.Commit(rv); err != nil {
		t.Errorf("expected Commit() to succeed, got: %s", err)
	}

	if _, err = testMetaStore.GetPending(rv); !meta.IsObjectNotFound(err) {
		t.Errorf("expected GetPending() to return 'not found', got: %s", err)
	}

	m, err = testMetaStore.Get(rv)
	if err != nil {
		t.Errorf("expected Get() to succeed, got: %s", err)
	} else {
		if m.Oid != contentOid {
			t.Errorf("expected committed object id to be %d, got: %d", contentOid, m.Oid)
		}
		if m.Size != contentSize {
			t.Errorf("expected committed object size to be %d, got: %d", contentSize, m.Size)
		}
		if !m.Existing {
			t.Error("expected meta object to be in the committed state")
		}
		if len(m.ProjectNames) != 1 || m.ProjectNames[0] != contentRepo {
			t.Errorf("expected committed object to belong to project %q, got: %v", contentRepo, m.ProjectNames)
		}
	}
}

func TestPutDuplicate(t *testing.T) {
	testMetaStore, err := setupMeta()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownMeta(testMetaStore)

	rv := &meta.RequestVars{
		Oid:  contentOid,
		Size: contentSize,
		Repo: contentRepo,
	}

	_, err = testMetaStore.Put(rv)
	if err != nil {
		t.Errorf("expected Put() to succeed, got: %s", err)
	}

	_, err = testMetaStore.Put(rv)
	if err != nil {
		t.Errorf("expected duplicate pending Put() to succeed, got: %s", err)
	}

	if _, err = testMetaStore.Commit(rv); err != nil {
		t.Errorf("expected Commit() to succeed, got: %s", err)
	}

	_, err = testMetaStore.Put(rv)
	if err != nil {
		t.Errorf("expected duplicate committed Put() to succeed, got: %s", err)
	}
}

func TestProjects(t *testing.T) {
	testMetaStore, err := setupMeta()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownMeta(testMetaStore)

	if err := testMetaStore.AddProject(contentRepo); err != nil {
		t.Errorf("expected AddProject() to succeed, got: %s", err)
	}

	projects, err := testMetaStore.Projects()
	if err != nil {
		t.Errorf("expected Projects() to succeed, got: %s", err)
	} else if len(projects) != 1 || projects[0].Name != contentRepo {
		t.Errorf("expected Projects() to return %s, got: %v", contentRepo, projects)
	}
}

func TestAuthentication(t *testing.T) {
	t.Skip("MySQL backend does not yet support user management and authentication")
}

func setupMeta() (*MySQLMetaStore, error) {
	config.Config.MySQL = &config.MySQLConfig{
		Enabled:  true,
		Host:     "127.0.0.1:3306",
		Username: "lfs_server",
		Password: "pass123",
		Database: "lfs_server_go",
	}

	metaStore, err := NewMySQLMetaStore()
	if err != nil {
		return nil, fmt.Errorf("error initializing test meta store: %s", err)
	}

	metaStore.client.Exec("TRUNCATE TABLE oid_maps")
	metaStore.client.Exec("TRUNCATE TABLE oids")
	metaStore.client.Exec("TRUNCATE TABLE projects")

	return metaStore, nil
}

func teardownMeta(metaStore *MySQLMetaStore) {
	metaStore.Close()
}
