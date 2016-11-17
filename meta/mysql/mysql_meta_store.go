package mysql

import (
	"database/sql"
	"encoding/base64"
	"strings"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/extauth/ldap"
	"github.com/ksurent/lfs-server-go/logger"
	m "github.com/ksurent/lfs-server-go/meta"
)

type MySQLMetaStore struct {
	client *sql.DB
}

func NewMySQLMetaStore() (*MySQLMetaStore, error) {
	db, err := NewMySQLSession()
	if err != nil {
		return nil, err
	}
	return &MySQLMetaStore{client: db}, nil
}

/*
Close (method close mysql connection)
*/
func (s *MySQLMetaStore) Close() {
	s.client.Close()
}

// Find all committed meta objects (called from the management interface)
func (s *MySQLMetaStore) findAllOids() ([]*m.Object, error) {
	rows, err := s.client.Query("select oid, size from oids where pending = 0")
	if err != nil {
		logger.Log(err)
		return nil, err
	}
	defer rows.Close()

	var (
		oid     string
		size    int64
		oidList []*m.Object
	)

	for rows.Next() {
		err := rows.Scan(&oid, &size)
		if err != nil {
			return nil, err
		}
		oidList = append(oidList, &m.Object{Oid: oid, Size: size})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return oidList, nil
}

// Find committed oids for a project id
func (s *MySQLMetaStore) mapOid(id int) ([]string, error) {
	rows, err := s.client.Query("select oid from oid_maps where projectID = ? and pending = 0", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		oid     string
		oidList []string
	)

	for rows.Next() {
		err := rows.Scan(&oid)
		if err != nil {
			return nil, err
		}
		oidList = append(oidList, oid)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return oidList, nil
}

// Find all committed projects
func (s *MySQLMetaStore) findAllProjects() ([]*m.Project, error) {
	rows, err := s.client.Query("select id, name from projects where pending = 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		name        string
		id          int
		projectList []*m.Project
	)

	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			return nil, err
		}

		oids, err := s.mapOid(id)
		if err != nil {
			return nil, err
		}

		projectList = append(projectList, &m.Project{Name: name, Oids: oids})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return projectList, nil
}

// Create committed project (called from the management interface)
func (s *MySQLMetaStore) createProject(name string) error {
	_, err := s.client.Exec("insert into projects (name, pending) values (?, 0)", name)
	return err
}

// Transactionally change status from pending to committed
func (s *MySQLMetaStore) commitPendingObject(meta *m.Object) error {
	tx, err := s.client.Begin()
	if err != nil {
		return err
	}

	tx.Exec("update oids set pending = 0 where oid = ?", meta.Oid)

	for _, name := range meta.ProjectNames {
		tx.Exec("update projects set pending = 0 where name = ?", name)
	}

	return tx.Commit()
}

// Transactionally create pending oid and related data
func (s *MySQLMetaStore) createPendingObject(meta *m.Object) error {
	tx, err := s.client.Begin()
	if err != nil {
		return err
	}

	tx.Exec("insert into oids (oid, size, pending) values (?, ?, 1)", meta.Oid, meta.Size)

	for _, name := range meta.ProjectNames {
		res, err := tx.Exec(`
			insert into
				projects (name, pending)
			values
				(?, 1)
			on duplicate key update
				id = last_insert_id(id)
		`, name)
		if err == nil {
			id, _ := res.LastInsertId()
			tx.Exec("insert into oid_maps (oid, projectID) values (?, ?)", meta.Oid, id)
		}
	}

	return tx.Commit()
}

func (s *MySQLMetaStore) findOid(oid string, pending bool) (*m.Object, error) {

	meta := m.Object{Existing: !pending}

	n := 0
	if pending {
		n = 1
	}

	err := s.client.QueryRow(`
		select
			oid, size
		from
			oids
		where
			oid = ?
			and pending = ?
	`, oid, n).Scan(&meta.Oid, &meta.Size)
	if err != nil {
		return nil, err
	}

	if meta.Oid == "" {
		return nil, m.ErrObjectNotFound
	}

	rows, err := s.client.Query(`
		select
			name
		from
			projects p
		join
			oid_maps m
		on
			p.id = m.projectID
		where
			m.oid = ?
			and p.pending = ?
	`, oid, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var name string
	for rows.Next() {
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}

		meta.ProjectNames = append(meta.ProjectNames, name)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

// Put() creates uncommitted objects from m.RequestVars and stores them in the
// meta store
func (s *MySQLMetaStore) Put(v *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(v.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	// Don't care here if it's pending or committed
	// TODO one query
	if meta, err := s.findOid(v.Oid, false); err == nil {
		return meta, nil
	}
	if meta, err := s.findOid(v.Oid, true); err == nil {
		return meta, nil
	}

	meta := &m.Object{
		Oid:          v.Oid,
		Size:         v.Size,
		ProjectNames: []string{v.Repo},
		Existing:     false,
	}

	err := s.doPut(meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

// Commit() finds uncommitted objects in the meta store using data in
// m.RequestVars and commits them
func (s *MySQLMetaStore) Commit(v *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(v.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	meta, err := s.GetPending(v)
	if err != nil {
		return nil, err
	}

	meta.Existing = true

	err = s.doPut(meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (s *MySQLMetaStore) doPut(meta *m.Object) error {
	if !meta.Existing {
		if err := s.createPendingObject(meta); err != nil {
			return err
		}

		return nil
	}

	return s.commitPendingObject(meta)
}

func (s *MySQLMetaStore) Get(v *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(v.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	meta, err := s.findOid(v.Oid, false)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

// Get() retrieves meta information for a committed object given information in
// m.RequestVars
func (s *MySQLMetaStore) GetPending(v *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(v.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	meta, err := s.findOid(v.Oid, true)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

/*
AddUser (Add a new user)
Not implemented in mysql_meta_store
*/
func (s *MySQLMetaStore) AddUser(user, pass string) error {
	return ldap.ErrUseLdap
}

/*
AddProject (Add a new project)
*/
func (s *MySQLMetaStore) AddProject(name string) error {
	return s.createProject(name)
}

/*
DeleteUser (Delete a user)
Not implemented
*/
func (s *MySQLMetaStore) DeleteUser(user string) error {
	return ldap.ErrUseLdap
}

/*
Users (get list of users)
Not implemented
*/
func (s *MySQLMetaStore) Users() ([]*m.User, error) {
	return []*m.User{}, ldap.ErrUseLdap
}

/*
Objects (get all oids)
return meta object
*/
func (s *MySQLMetaStore) Objects() ([]*m.Object, error) {
	return s.findAllOids()
}

/*
Projects (get all projects)
return meta project object
*/
func (s *MySQLMetaStore) Projects() ([]*m.Project, error) {
	return s.findAllProjects()
}

/*
Auth routine.  Requires an auth string like
"Basic YWRtaW46YWRtaW4="
*/
func (s *MySQLMetaStore) authenticate(authorization string) bool {
	if config.Config.IsPublic() {
		return true
	}

	if !config.Config.Ldap.Enabled {
		logger.Log("MySQL based authentication is not implemented, please use LDAP")
		return false
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

	return ldap.AuthenticateLdap(user, password)
}
