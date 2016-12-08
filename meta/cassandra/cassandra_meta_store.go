package cassandra

import (
	"errors"
	"fmt"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/meta"

	"github.com/gocql/gocql"
	"github.com/relops/cqlr"
)

type CassandraMetaStore struct {
	cassandraService *CassandraService
	client           *gocql.Session
}

var errUnsupported = errors.New("This feature is not supported by this backend")

func NewCassandraMetaStore(cfg *config.CassandraConfig) (*CassandraMetaStore, error) {
	sess, err := NewCassandraSession(cfg)
	if err != nil {
		return nil, err
	}

	return &CassandraMetaStore{
		cassandraService: sess,
		client:           sess.Client,
	}, nil
}

func (self *CassandraMetaStore) Close() {
	self.client.Close()
}

func (self *CassandraMetaStore) createProject(project string, pending bool) error {
	counter := make(map[string]interface{}, 1)
	self.client.Query("select count(*) as count from projects where name = ?", project).MapScan(counter)
	if val, ok := counter["count"].(int64); ok && val > 0 {
		// already there
		return nil
	}
	return self.client.Query("insert into projects (name, pending) values(?, ?)", project, pending).Exec()
}

func (self *CassandraMetaStore) addOidToProject(oid string, project string) error {
	// Cannot bind on collections
	q := fmt.Sprintf("update projects set oids = oids + {'%s'} where name = '%s'", oid, project)
	return self.client.Query(q).Exec()
}

func (self *CassandraMetaStore) createPendingOid(m *meta.Object) error {
	err := self.client.Query(`
		insert into
			oids (oid, size, pending)
		values
			(?, ?, ?)
	`, m.Oid, m.Size, true).Exec()
	if err != nil {
		return err
	}

	for _, name := range m.ProjectNames {
		err := self.createProject(name, true)
		if err != nil {
			return err
		}

		err = self.addOidToProject(m.Oid, name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *CassandraMetaStore) commitPendingOid(m *meta.Object) error {
	err := self.client.Query(`
		update
			oids
		set
			pending = ?
		where
			oid = ?
	`, false, m.Oid).Exec()
	if err != nil {
		return err
	}

	itr := self.cassandraService.Client.Query(`
		select
			name
		from
			projects
		where
			oids contains ?
	`, m.Oid).Iter()

	var project string
	for itr.Scan(&project) {
		err = self.client.Query(`
			update
				projects
			set
				pending = ?
			where
				name = ?
		`, false, project).Exec()
		if err != nil {
			return err
		}
	}

	if err = itr.Close(); err != nil {
		return err
	}

	return nil
}

func (self *CassandraMetaStore) findProject(projectName string) (*meta.Project, error) {
	q := self.client.Query("select * from projects where name = ?", projectName)
	b := cqlr.BindQuery(q)

	var ct meta.Project
	b.Scan(&ct)

	if err := b.Close(); err != nil {
		return nil, err
	}

	if ct.Name == "" {
		return nil, meta.ErrProjectNotFound
	}

	return &ct, nil
}

func (self *CassandraMetaStore) findOid(oid string, pending bool) (*meta.Object, error) {
	q := self.client.Query(`
		select
			oid, size
		from
			oids
		where
			oid = ?
			and pending = ?
		allow filtering
	`, oid, pending)
	b := cqlr.BindQuery(q)

	var m meta.Object
	b.Scan(&m)

	if err := b.Close(); err != nil {
		return nil, err
	}

	if m.Oid == "" {
		return nil, meta.ErrObjectNotFound
	}

	m.Existing = !pending

	itr := self.cassandraService.Client.Query(`
		select
			name
		from
			projects
		where
			oids contains ?
	`, oid).Iter()

	var project string
	for itr.Scan(&project) {
		m.ProjectNames = append(m.ProjectNames, project)
	}

	if err := itr.Close(); err != nil {
		return nil, err
	}

	return &m, nil
}

/*
Oid finder - returns a []*meta.Object
*/
func (self *CassandraMetaStore) findAllOids() ([]*meta.Object, error) {
	itr := self.cassandraService.Client.Query("select oid, size, pending from oids where pending = false;").Iter()
	var oid string
	var size int64
	oid_list := make([]*meta.Object, 0)
	for itr.Scan(&oid, &size) {
		oid_list = append(oid_list, &meta.Object{Oid: oid, Size: size})
	}

	if err := itr.Close(); err != nil {
		return nil, err
	}

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

	if err := itr.Close(); err != nil {
		return nil, err
	}

	if len(project_list) == 0 {
		return nil, meta.ErrProjectNotFound
	}
	return project_list, nil
}

// Put() creates uncommitted objects from meta.RequestVars and stores them in the
// meta store
func (self *CassandraMetaStore) Put(v *meta.RequestVars) (*meta.Object, error) {
	// Don't care here if it's pending or committed
	// TODO one query
	if m, err := self.findOid(v.Oid, false); err == nil {
		return m, nil
	}
	if m, err := self.findOid(v.Oid, true); err == nil {
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
		if err := self.createPendingOid(m); err != nil {
			return err
		}

		return nil
	}

	return self.commitPendingOid(m)
}

// Get() retrieves meta information for a committed object given information in
// meta.RequestVars
func (self *CassandraMetaStore) Get(v *meta.RequestVars) (*meta.Object, error) {
	return self.findOid(v.Oid, false)
}

// Same as Get() but for uncommitted objects
func (self *CassandraMetaStore) GetPending(v *meta.RequestVars) (*meta.Object, error) {
	return self.findOid(v.Oid, true)
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

	if err := b.Close(); err != nil {
		return nil, err
	}

	if mu.Name == "" {
		return nil, meta.ErrUserNotFound
	}

	return &mu, nil
}

/*
Adds a user to the system
*/
func (self *CassandraMetaStore) AddUser(user, pass string) error {
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
Removes a user from the system
Usage: DeleteUser("testuser")
*/
func (self *CassandraMetaStore) DeleteUser(user string) error {
	return self.client.Query("delete from users where username = ?", user).Exec()
}

/*
returns all users
*/
func (self *CassandraMetaStore) Users() ([]*meta.User, error) {
	var mu meta.User
	users := make([]*meta.User, 0)
	q := self.client.Query("select username from users")
	b := cqlr.BindQuery(q)
	for b.Scan(&mu) {
		users = append(users, &mu)
	}

	if err := b.Close(); err != nil {
		return nil, err
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
*/
func (self *CassandraMetaStore) AddProject(name string) error {
	return self.createProject(name, false)
}

/*
Auth routine.  Requires an auth string like
"Basic YWRtaW46YWRtaW4="
*/
func (self *CassandraMetaStore) Authenticate(user, pass string) (bool, error) {
	mu, err := self.findUser(user)
	if err != nil {
		return false, err
	}

	return meta.CheckPass([]byte(mu.Password), []byte(pass))
}
