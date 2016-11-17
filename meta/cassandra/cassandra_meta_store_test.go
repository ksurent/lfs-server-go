package cassandra

import (
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/ksurent/lfs-server-go/config"
	m "github.com/ksurent/lfs-server-go/meta"
)

var (
	metaStoreTestCassandra *CassandraMetaStore
)

var (
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

func TestCassandraGetWithAuth(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	meta, err := metaStoreTestCassandra.Get(&m.RequestVars{Authorization: testAuth, Oid: noAuthOid})
	if err == nil {
		t.Fatalf("expected get to fail with unknown oid, got: %s", meta.Oid)
	}

	meta, err = metaStoreTestCassandra.Get(&m.RequestVars{Authorization: testAuth, Oid: contentOid})
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

func TestCassandraGetWithoutAuth(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	_, err := metaStoreTestCassandra.Get(&m.RequestVars{Authorization: badAuth, Oid: contentOid})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}

	_, err = metaStoreTestCassandra.Get(&m.RequestVars{Oid: contentOid})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
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

	config.Config.Ldap.Enabled = true

	_, luErr := metaStoreTestCassandra.Users()
	if luErr == nil {
		t.Errorf("Expected to raise error when trying to check users with ldap enabled")
	}
	config.Config.Ldap.Enabled = false

	uErr := metaStoreTestCassandra.DeleteUser(testUser)
	if uErr != nil {
		t.Errorf("Unable to delete user, error %s", err.Error())
	}

}

func TestCassandraPutWithAuth(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	meta, err := metaStoreTestCassandra.Put(&m.RequestVars{Authorization: testAuth, Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected put to succeed, got: %s", err)
	}

	if meta.Existing {
		t.Errorf("expected meta to not have existed")
	}

	_, err = metaStoreTestCassandra.Get(&m.RequestVars{Authorization: testAuth, Oid: nonexistingOid})
	if err == nil {
		t.Errorf("expected new put to not be committed yet")
	}

	meta, err = metaStoreTestCassandra.GetPending(&m.RequestVars{Authorization: testAuth, Oid: nonexistingOid})
	if err != nil {
		t.Errorf("expected new put to be pending, got: %s", err)
	}

	if meta.Oid != nonexistingOid {
		t.Errorf("expected oids to match, got: %s", meta.Oid)
	}

	if meta.Size != 42 {
		t.Errorf("expected sizes to match, got: %d", meta.Size)
	}

	meta, err = metaStoreTestCassandra.Commit(&m.RequestVars{Authorization: testAuth, Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected commit to succeed, got: %s", err)
	}

	if !meta.Existing {
		t.Errorf("expected existing to become true after commit")
	}

	_, err = metaStoreTestCassandra.Get(&m.RequestVars{Authorization: testAuth, Oid: nonexistingOid})
	if err != nil {
		t.Errorf("expected new put to be committed now, got: %s", err)
	}

	if !meta.Existing {
		t.Errorf("expected existing to be true for a committed object")
	}

	meta, err = metaStoreTestCassandra.Put(&m.RequestVars{Authorization: testAuth, Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected putting an duplicate object to succeed, got: %s", err)
	}

	if !meta.Existing {
		t.Errorf("expecting existing to be true for a duplicate object")
	}
}

func TestCassandraPutWithoutAuth(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	_, err := metaStoreTestCassandra.Put(&m.RequestVars{Authorization: badAuth, User: testUser, Oid: contentOid, Size: 42})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}

	_, err = metaStoreTestCassandra.Put(&m.RequestVars{User: testUser, Oid: contentOid, Size: 42, Repo: testRepo})
	if !m.IsAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}
}

func TestCassandraOids(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	allOids, _ := metaStoreTestCassandra.findAllOids()
	cb := len(allOids)

	createOidErr := metaStoreTestCassandra.createOid(nonexistingOid, 1)
	if createOidErr != nil {
		t.Errorf("Failed to create OID")
	}

	allOids, _ = metaStoreTestCassandra.findAllOids()
	if cb == len(allOids) {
		t.Errorf("Failed add OID")
	}

	mo, findOidErr := metaStoreTestCassandra.findOid(nonexistingOid)
	if findOidErr != nil {
		t.Errorf("Failed find OID")
	}
	if mo == nil || mo.Oid != nonexistingOid {
		t.Errorf("Failed find OID, it does not match")
	}

	mos, mosErr := metaStoreTestCassandra.Objects()
	if mosErr != nil {
		t.Errorf("error was raised when trying to fetch objects %s", mosErr.Error())
	}
	if len(mos) == 0 {
		t.Errorf("No objects where found, at least 1 was expected")
	}

	delOidErr := metaStoreTestCassandra.removeOid(nonexistingOid)
	if delOidErr != nil {
		t.Errorf("Failed remove OID")
	}

	err := metaStoreTestCassandra.createPendingOid(nonexistingOid, 1)
	if err != nil {
		t.Errorf("Failed to create pending OID")
	}

	_, err = metaStoreTestCassandra.findPendingOid(nonexistingOid)
	if err != nil {
		t.Errorf("Failed to find pending OID")
	}

	err = metaStoreTestCassandra.removePendingOid(nonexistingOid)
	if err != nil {
		t.Errorf("Failed to remove pending OID")
	}

	_, err = metaStoreTestCassandra.findPendingOid(nonexistingOid)
	if err == nil {
		t.Errorf("Did not expect to find removed pending OID")
	}
}

func TestCassandraProjects(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	err := metaStoreTestCassandra.createProject(extraRepo)
	if err != nil {
		t.Errorf("Failed to create project")
	}

	listProjects, err := metaStoreTestCassandra.findAllProjects()
	if err != nil {
		t.Errorf("Failed getting cassandra projects")
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

	proj, err := metaStoreTestCassandra.findProject(extraRepo)
	if err != nil {
		t.Errorf("Failed to find project")
	}

	if proj.Name != extraRepo {
		t.Errorf("Failed to find project, got wrong name in response %s", proj.Name)
	}

	_, err = metaStoreTestCassandra.findProject("")
	if err == nil {
		t.Errorf("Expected error but got none")
	}

	_, err = metaStoreTestCassandra.Projects()
	if err != nil {
		t.Errorf("Failed getting cassandra projects")
	}

	delErr := metaStoreTestCassandra.removeProject(extraRepo)
	if delErr != nil {
		t.Errorf("Failed to delete project")
	}

	_, findPErrEmpty := metaStoreTestCassandra.findProject(extraRepo)
	if findPErrEmpty == nil {
		t.Errorf("findProject should have raised an error")
	}

}

func TestProjectOidRelationship(t *testing.T) {
	serr := setupCassandraMeta()
	if serr != nil {
		t.Errorf(serr.Error())
	}
	defer teardownCassandraMeta()

	err := metaStoreTestCassandra.createProject(testRepo)
	if err != nil {
		t.Errorf("Failed creating project")
	}
	err = metaStoreTestCassandra.addOidToProject(contentOid, testRepo)
	if err != nil {
		t.Errorf("Failed adding OID to project")
	}
	err = metaStoreTestCassandra.removeOidFromProject(contentOid, testRepo)
	if err != nil {
		t.Errorf("Failed removing OID from project %s", err.Error())
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

	rv := &m.RequestVars{Authorization: testAuth, Oid: contentOid, Size: contentSize, Repo: testRepo}

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
