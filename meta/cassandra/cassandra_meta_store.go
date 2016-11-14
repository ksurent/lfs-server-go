package cassandra

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/extauth/ldap"
	"github.com/ksurent/lfs-server-go/logger"
	"github.com/ksurent/lfs-server-go/meta"

	"github.com/gocql/gocql"
	"github.com/relops/cqlr"
)

type CassandraMetaStore struct {
	cassandraService *CassandraService
	client           *gocql.Session
}

const (
	CassandraPendingTable   string = "pending_oids"
	CassandraCommittedTable        = "oids"
)

var errUnsupported = errors.New("This feature is not supported by this backend")

func NewCassandraMetaStore() (*CassandraMetaStore, error) {
	sess, err := NewCassandraSession()
	if err != nil {
		return nil, err
	}

	return &CassandraMetaStore{
		cassandraService: sess,
		client:           sess.Client,
	}, nil
}

func (self *CassandraMetaStore) Close() {
	defer self.client.Close()
	return
}

func (self *CassandraMetaStore) createProject(project string) error {
	counter := make(map[string]interface{}, 1)
	self.client.Query("select count(*) as count from projects where name = ?", project).MapScan(counter)
	if val, ok := counter["count"].(int64); ok && val > 0 {
		// already there
		return nil
	}
	err := self.client.Query("insert into projects (name) values(?)", project).Exec()
	return err
}

func (self *CassandraMetaStore) addOidToProject(oid string, project string) error {
	// Cannot bind on collections
	q := fmt.Sprintf("update projects set oids = oids + {'%s'} where name = '%s'", oid, project)
	err := self.client.Query(q).Exec()
	return err
}

func (self *CassandraMetaStore) createPendingOid(oid string, size int64) error {
	return self.client.Query("insert into pending_oids (oid, size) values (?, ?)", oid, size).Exec()
}

func (self *CassandraMetaStore) createOid(oid string, size int64) error {
	return self.client.Query("insert into oids (oid, size) values (?, ?)", oid, size).Exec()
}

func (self *CassandraMetaStore) removePendingOid(oid string) error {
	return self.client.Query("delete from pending_oids where oid = ?", oid).Exec()
}

func (self *CassandraMetaStore) removeOid(oid string) error {
	/*
		Oids are shared amongst projects, so this will need to find out the following:
		1. What projects (if any) have the requested OID.
		2. If other projects are still using the OID, then do not delete it from the main OID listing
	*/
	//	return self.client.Query("update projects set oids = oids - {?} where oids contains ?", oid).Exec()
	return self.client.Query("delete from oids where oid = ?", oid).Exec()
}

func (self *CassandraMetaStore) removeOidFromProject(oid, project string) error {
	/*
		Oids are shared amongst projects, so this will need to find out the following:
		1. What projects (if any) have the requested OID.
		2. If other projects are still using the OID, then do not delete it from the main OID listing
	*/
	q := fmt.Sprintf("update projects set oids = oids - {'%s'} where name = '%s'", oid, project)
	return self.client.Query(q).Exec()
}

func (self *CassandraMetaStore) removeProject(projectName string) error {
	return self.client.Query("delete from projects where name = ?", projectName).Exec()
}

func (self *CassandraMetaStore) findProject(projectName string) (*meta.Project, error) {
	if projectName == "" {
		return nil, meta.ErrProjectNotFound
	}
	q := self.client.Query("select * from projects where name = ?", projectName)
	b := cqlr.BindQuery(q)
	var ct meta.Project
	b.Scan(&ct)
	defer b.Close()
	if ct.Name == "" {
		return nil, meta.ErrProjectNotFound
	}
	return &ct, nil
}

func (self *CassandraMetaStore) findPendingOid(oid string) (*meta.Object, error) {
	m, err := self.doFindOid(oid, CassandraPendingTable)
	if err != nil {
		return nil, err
	}

	m.Existing = false

	return m, nil
}

func (self *CassandraMetaStore) findOid(oid string) (*meta.Object, error) {
	m, err := self.doFindOid(oid, CassandraCommittedTable)
	if err != nil {
		return nil, err
	}

	m.Existing = true

	return m, nil
}

func (self *CassandraMetaStore) doFindOid(oid, table string) (*meta.Object, error) {
	q := self.client.Query("select oid, size from "+table+" where oid = ? limit 1", oid)
	b := cqlr.BindQuery(q)
	defer b.Close()

	var m meta.Object
	b.Scan(&m)

	if m.Oid == "" {
		return nil, meta.ErrObjectNotFound
	}

	itr := self.cassandraService.Client.Query("select name from projects where oids contains ?", oid).Iter()
	defer itr.Close()

	var project string
	for itr.Scan(&project) {
		m.ProjectNames = append(m.ProjectNames, project)
	}

	return &m, nil
}

/*
Oid finder - returns a []*meta.Object
*/
func (self *CassandraMetaStore) findAllOids() ([]*meta.Object, error) {
	itr := self.cassandraService.Client.Query("select oid, size from oids;").Iter()
	var oid string
	var size int64
	oid_list := make([]*meta.Object, 0)
	for itr.Scan(&oid, &size) {
		oid_list = append(oid_list, &meta.Object{Oid: oid, Size: size})
	}
	itr.Close()
	return oid_list, nil
}

/*
Project finder - returns a []*meta.Project
*/
func (self *CassandraMetaStore) findAllProjects() ([]*meta.Project, error) {
	itr := self.cassandraService.Client.Query("select name, oids from projects;").Iter()
	var oids []string
	var name string
	project_list := []*meta.Project{}
	for itr.Scan(&name, &oids) {
		project_list = append(project_list, &meta.Project{Name: name, Oids: oids})
	}
	itr.Close()
	if len(project_list) == 0 {
		return nil, meta.ErrProjectNotFound
	}
	return project_list, nil
}

// Put() creates uncommitted objects from meta.RequestVars and stores them in the
// meta store
func (self *CassandraMetaStore) Put(v *meta.RequestVars) (*meta.Object, error) {
	if !self.authenticate(v.Authorization) {
		return nil, meta.ErrNotAuthenticated
	}

	// Don't care here if it's pending or committed
	if m, err := self.doGet(v); err == nil {
		return m, nil
	}

	m := &meta.Object{
		Oid:          v.Oid,
		Size:         v.Size,
		ProjectNames: []string{v.Repo},
		Existing:     false,
	}

	err := self.doPut(m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Commit() finds uncommitted objects in the meta store using data in
// meta.RequestVars and commits them
func (self *CassandraMetaStore) Commit(v *meta.RequestVars) (*meta.Object, error) {
	if !self.authenticate(v.Authorization) {
		return nil, meta.ErrNotAuthenticated
	}

	m, err := self.GetPending(v)
	if err != nil {
		return nil, err
	}

	m.Existing = true

	err = self.doPut(m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (self *CassandraMetaStore) doPut(m *meta.Object) error {

	if !m.Existing {
		// Creating pending object

		if err := self.createPendingOid(m.Oid, m.Size); err != nil {
			return err
		}

		return nil
	}

	// Committing pending object

	if err := self.removePendingOid(m.Oid); err != nil {
		return err
	}

	// TODO transform this into a logged batch

	if err := self.createOid(m.Oid, m.Size); err != nil {
		return err
	}

	for _, project := range m.ProjectNames {
		// XXX pending projects?

		if err := self.createProject(project); err != nil {
			return err
		}

		if err := self.addOidToProject(m.Oid, project); err != nil {
			return err
		}
	}

	return nil
}

// Get() retrieves meta information for a committed object given information in
// meta.RequestVars
func (self *CassandraMetaStore) Get(v *meta.RequestVars) (*meta.Object, error) {
	if !self.authenticate(v.Authorization) {
		return nil, meta.ErrNotAuthenticated
	}

	m, err := self.doGet(v)
	if err != nil {
		return nil, err
	} else if !m.Existing {
		return nil, meta.ErrObjectNotFound
	}

	return m, nil
}

// Same as Get() but for uncommitted objects
func (self *CassandraMetaStore) GetPending(v *meta.RequestVars) (*meta.Object, error) {
	if !self.authenticate(v.Authorization) {
		return nil, meta.ErrNotAuthenticated
	}

	m, err := self.doGet(v)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (self *CassandraMetaStore) doGet(v *meta.RequestVars) (*meta.Object, error) {

	if m, err := self.findOid(v.Oid); err == nil {
		m.Existing = true
		return m, nil
	}

	if m, err := self.findPendingOid(v.Oid); err == nil {
		m.Existing = false
		return m, nil
	}

	return nil, meta.ErrObjectNotFound
}

/*
finds a user
Usage: FindUser("testuser")
*/
func (self *CassandraMetaStore) findUser(user string) (*meta.User, error) {
	var mu meta.User
	q := self.client.Query("select * from users where username = ?", user)
	b := cqlr.BindQuery(q)
	b.Scan(&mu)
	if mu.Name == "" {
		return nil, meta.ErrUserNotFound
	}
	return &mu, nil
}

/*
Adds a user to the system, only for use when not using ldap
*/
func (self *CassandraMetaStore) AddUser(user, pass string) error {
	if config.Config.Ldap.Enabled {
		return ldap.ErrUseLdap
	}
	_, uErr := self.findUser(user)
	// return nil if the user is already there
	if uErr == nil {
		return nil
	}
	encryptedPass, err := meta.EncryptPass([]byte(pass))
	if err != nil {
		return err
	}

	return self.client.Query("insert into users (username, password) values(?, ?)", user, encryptedPass).Exec()
}

/*
Removes a user from the system, only for use when not using ldap
Usage: DeleteUser("testuser")
*/
func (self *CassandraMetaStore) DeleteUser(user string) error {
	if config.Config.Ldap.Enabled {
		return ldap.ErrUseLdap
	}
	return self.client.Query("delete from users where username = ?", user).Exec()
}

/*
returns all users, only for use when not using ldap
*/
func (self *CassandraMetaStore) Users() ([]*meta.User, error) {
	if config.Config.Ldap.Enabled {
		return []*meta.User{}, ldap.ErrUseLdap
	}
	var mu meta.User
	users := make([]*meta.User, 0)
	q := self.client.Query("select username from users")
	b := cqlr.BindQuery(q)
	for b.Scan(&mu) {
		users = append(users, &mu)
	}
	return users, nil
}

/*
returns all Oids
*/
func (self *CassandraMetaStore) Objects() ([]*meta.Object, error) {
	return self.findAllOids()
}

/*
Returns a []*meta.Project
*/
func (self *CassandraMetaStore) Projects() ([]*meta.Project, error) {
	return self.findAllProjects()
}

/*
AddProject (create a new project using POST)
Only implemented on MySQL meta store
*/
func (self *CassandraMetaStore) AddProject(name string) error {
	return errUnsupported
}

/*
Auth routine.  Requires an auth string like
"Basic YWRtaW46YWRtaW4="
*/
func (self *CassandraMetaStore) authenticate(authorization string) bool {
	if config.Config.IsPublic() {
		return true
	}

	if authorization == "" {
		logger.Log("No authentication info")
		return false
	}

	if !strings.HasPrefix(authorization, "Basic ") {
		logger.Log("Authentication info does not look like Basic HTTP")
		return false
	}

	c, err := base64.URLEncoding.DecodeString(strings.TrimPrefix(authorization, "Basic "))
	if err != nil {
		logger.Log(err)
		return false
	}
	cs := string(c)
	i := strings.IndexByte(cs, ':')
	if i < 0 {
		return false
	}
	user, password := cs[:i], cs[i+1:]

	if config.Config.Ldap.Enabled {
		return ldap.AuthenticateLdap(user, password)
	}
	mu, err := self.findUser(user)
	if err != nil {
		logger.Log(err)
		return false
	}

	match, err := meta.CheckPass([]byte(mu.Password), []byte(password))
	if err != nil {
		logger.Log(err)
	}
	return match
}
