package cassandra

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ksurent/lfs-server-go/meta"
)

var (
	metaStoreTestCassandra meta.GenericMetaStore

	testUser       = "admin"
	testPass       = "admin"
	contentSize    = int64(len("this is my content"))
	contentOid     = "f97e1b2936a56511b3b6efc99011758e4700d60fb1674d31445d1ee40b663f24"
	nonexistingOid = "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f"
	extraRepo      = "mytestproject"
	testRepo       = "repo"
)

func TestCassandraGet(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	m, err := metaStoreTestCassandra.Get(&meta.RequestVars{Oid: nonexistingOid})
	if err == nil {
		t.Fatalf("expected get to fail with unknown oid, got: %s", m.Oid)
	}

	m, err = metaStoreTestCassandra.Get(&meta.RequestVars{Oid: contentOid})
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

func TestCassandraUsers(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	err := metaStoreTestCassandra.AddUser(testUser, testPass)
	if err != nil {
		t.Errorf("Unable to add user, error %s", err.Error())
	}

	users, err := metaStoreTestCassandra.Users()
	if err != nil {
		t.Errorf("Unable to retrieve users, error %s", err.Error())
	}
	if len(users) == 0 {
		t.Errorf("Adding a user failed")
	}

	uErr := metaStoreTestCassandra.DeleteUser(testUser)
	if uErr != nil {
		t.Errorf("Unable to delete user, error %s", err.Error())
	}
}

func TestCassandraPut(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	m, err := metaStoreTestCassandra.Put(&meta.RequestVars{Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected put to succeed, got: %s", err)
	}

	if m.Existing {
		t.Errorf("expected meta to not have existed")
	}

	_, err = metaStoreTestCassandra.Get(&meta.RequestVars{Oid: nonexistingOid})
	if err == nil {
		t.Errorf("expected new put to not be committed yet")
	}

	m, err = metaStoreTestCassandra.GetPending(&meta.RequestVars{Oid: nonexistingOid})
	if err != nil {
		t.Errorf("expected new put to be pending, got: %s", err)
	}

	if m.Oid != nonexistingOid {
		t.Errorf("expected oids to match, got: %s", m.Oid)
	}

	if m.Size != 42 {
		t.Errorf("expected sizes to match, got: %d", m.Size)
	}

	m, err = metaStoreTestCassandra.Commit(&meta.RequestVars{Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected commit to succeed, got: %s", err)
	}

	if !m.Existing {
		t.Errorf("expected existing to become true after commit")
	}

	_, err = metaStoreTestCassandra.Get(&meta.RequestVars{Oid: nonexistingOid})
	if err != nil {
		t.Errorf("expected new put to be committed now, got: %s", err)
	}

	if !m.Existing {
		t.Errorf("expected existing to be true for a committed object")
	}

	m, err = metaStoreTestCassandra.Put(&meta.RequestVars{Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected putting an duplicate object to succeed, got: %s", err)
	}

	if !m.Existing {
		t.Errorf("expecting existing to be true for a duplicate object")
	}
}

func TestCassandraProjects(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	err := metaStoreTestCassandra.AddProject(extraRepo)
	if err != nil {
		t.Errorf("Failed to create project: %s", err)
	}

	listProjects, err := metaStoreTestCassandra.Projects()
	if err != nil {
		t.Errorf("Failed getting cassandra projects: %s", err)
	}
	found := false
	for i := range listProjects {
		if listProjects[i].Name == extraRepo {
			found = true
		}
	}
	if !found {
		t.Errorf("Failed finding project %s", extraRepo)
	}
}

func TestCassandraAuthenticate(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	ok, _ := metaStoreTestCassandra.Authenticate(testUser, testPass)
	if !ok {
		t.Errorf("expected authentication to succeed")
	}

	ok, _ = metaStoreTestCassandra.Authenticate("azog", "defiler")
	if ok {
		t.Errorf("expected authentication to fail")
	}
}

func setupCassandraMeta() error {
	store, err := NewCassandraMetaStore()
	if err != nil {
		fmt.Printf("error initializing test meta store: %s\n", err)
		return errors.New(fmt.Sprintf("error initializing test meta store: %s\n", err))
	}

	metaStoreTestCassandra = store
	if err := metaStoreTestCassandra.AddUser(testUser, testPass); err != nil {
		teardownCassandraMeta()
		fmt.Printf("error adding test user to meta store: %s\n", err)
		return errors.New(fmt.Sprintf("error adding test user to meta store: %s\n", err))
	}

	rv := &meta.RequestVars{Oid: contentOid, Size: contentSize, Repo: testRepo}

	if _, err := metaStoreTestCassandra.Put(rv); err != nil {
		teardownCassandraMeta()
		fmt.Printf("error seeding cassandra test meta store: %s\n", err)
		return errors.New(fmt.Sprintf("error seeding cassandra test meta store: %s\n", err))
	}
	if _, err := metaStoreTestCassandra.Commit(rv); err != nil {
		teardownCassandraMeta()
		fmt.Printf("error seeding cassandra test meta store: %s\n", err)
		return errors.New(fmt.Sprintf("error seeding cassandra test meta store: %s\n", err))
	}

	return nil
}

func teardownCassandraMeta() {
	sess, err := NewCassandraSession()
	if err != nil {
		fmt.Printf("error tearing down cassandra test meta store: %s\n", err)
		return
	}
	DropCassandra(sess.Client)
}
