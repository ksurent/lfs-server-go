package main

import (
	"database/sql"
	"encoding/base64"
	"strings"
)

type MySQLMetaStore struct {
	client *sql.DB
}

func NewMySQLMetaStore(db *sql.DB) (*MySQLMetaStore, error) {
	return &MySQLMetaStore{client: db}, nil
}

/*
Close (method close mysql connection)
*/
func (m *MySQLMetaStore) Close() {
	m.client.Close()
}

// Find all committed meta objects (called from the management interface)
func (m *MySQLMetaStore) findAllOids() ([]*MetaObject, error) {
	rows, err := m.client.Query("select oid, size from oids where pending = 0")
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.findAllOids", "msg": err})
		return nil, err
	}
	defer rows.Close()

	var (
		oid     string
		size    int64
		oidList []*MetaObject
	)

	for rows.Next() {
		err := rows.Scan(&oid, &size)
		if err != nil {
			logger.Log(kv{"fn": "MySQLMetaStore.findAllOids", "msg": err})
		}
		oidList = append(oidList, &MetaObject{Oid: oid, Size: size})
	}

	return oidList, nil
}

// Find committed oids for a project id
func (m *MySQLMetaStore) mapOid(id int) ([]string, error) {
	rows, err := m.client.Query("select oid from oid_maps where projectID = ? and pending = 0", id)
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.mapOid", "msg": err})
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
			logger.Log(kv{"fn": "MySQLMetaStore.mapOid", "msg": err})
			return nil, err
		}
		oidList = append(oidList, oid)
	}

	return oidList, nil
}

// Find all committed projects
func (m *MySQLMetaStore) findAllProjects() ([]*MetaProject, error) {
	rows, err := m.client.Query("select id, name from projects where pending = 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		name        string
		id          int
		projectList []*MetaProject
	)

	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			logger.Log(kv{"fn": "MySQLMetaStore.findAllProjects", "msg": err})
		}

		oids, err := m.mapOid(id)
		if err != nil {
			logger.Log(kv{"fn": "MySQLMetaStore.findAllProjects", "msg": err})
			return nil, err
		}

		projectList = append(projectList, &MetaProject{Name: name, Oids: oids})
	}

	return projectList, nil
}

// Create committed project (called from the management interface)
func (m *MySQLMetaStore) createProject(name string) error {
	_, err := m.client.Exec("insert into projects (name, pending) values (?, 0)", name)
	return err
}

// Transactionally change status from pending to committed
func (m *MySQLMetaStore) commitPendingObject(meta *MetaObject) error {
	tx, err := m.client.Begin()
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.commitPendingObject", "msg": "Could not start a transaction: " + err.Error()})
		return err
	}

	tx.Exec("update oids set pending = 0 where oid = ?", meta.Oid)

	for _, name := range meta.ProjectNames {
		tx.Exec("update projects set pending = 0 where name = ?", name)
	}

	return tx.Commit()
}

// Transactionally create pending oid and related data
func (m *MySQLMetaStore) createPendingObject(meta *MetaObject) error {
	tx, err := m.client.Begin()
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.createPendingObject", "msg": "Could not start a transaction: " + err.Error()})
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

func (m *MySQLMetaStore) findOid(oid string, pending bool) (*MetaObject, error) {

	meta := MetaObject{Existing: !pending}

	n := 0
	if pending {
		n = 1
	}

	err := m.client.QueryRow(`
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
		return nil, errObjectNotFound
	}

	rows, err := m.client.Query(`
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

	return &meta, nil
}

// Put() creates uncommitted objects from RequestVars and stores them in the
// meta store
func (m *MySQLMetaStore) Put(v *RequestVars) (*MetaObject, error) {
	if !m.authenticate(v.Authorization) {
		logger.Log(kv{"fn": "MySQLMetaStore.Put", "msg": "Unauthorized"})
		return nil, newAuthError()
	}

	// Don't care here if it's pending or committed
	// TODO one query
	if meta, err := m.findOid(v.Oid, false); err == nil {
		return meta, nil
	}
	if meta, err := m.findOid(v.Oid, true); err == nil {
		return meta, nil
	}

	meta := &MetaObject{
		Oid:          v.Oid,
		Size:         v.Size,
		ProjectNames: []string{v.Repo},
		Existing:     false,
	}

	err := m.doPut(meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

// Commit() finds uncommitted objects in the meta store using data in
// RequestVars and commits them
func (m *MySQLMetaStore) Commit(v *RequestVars) (*MetaObject, error) {
	if !m.authenticate(v.Authorization) {
		logger.Log(kv{"fn": "MySQLMetaStore.Commit", "msg": "Unauthorized"})
		return nil, newAuthError()
	}

	meta, err := m.GetPending(v)
	if err != nil {
		return nil, err
	}

	meta.Existing = true

	err = m.doPut(meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (m *MySQLMetaStore) doPut(meta *MetaObject) error {
	if !meta.Existing {
		if err := m.createPendingObject(meta); err != nil {
			return err
		}

		return nil
	}

	return m.commitPendingObject(meta)
}

func (m *MySQLMetaStore) Get(v *RequestVars) (*MetaObject, error) {
	if !m.authenticate(v.Authorization) {
		return nil, newAuthError()
	}

	meta, err := m.findOid(v.Oid, false)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

// Get() retrieves meta information for a committed object given information in
// RequestVars
func (m *MySQLMetaStore) GetPending(v *RequestVars) (*MetaObject, error) {
	if !m.authenticate(v.Authorization) {
		return nil, newAuthError()
	}

	meta, err := m.findOid(v.Oid, true)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

/*
AddUser (Add a new user)
Not implemented in mysql_meta_store
*/
func (m *MySQLMetaStore) AddUser(user, pass string) error {
	return errNotImplemented
}

/*
AddProject (Add a new project)
*/
func (m *MySQLMetaStore) AddProject(name string) error {
	return m.createProject(name)
}

/*
DeleteUser (Delete a user)
Not implemented
*/
func (m *MySQLMetaStore) DeleteUser(user string) error {
	return errNotImplemented
}

/*
Users (get list of users)
Not implemented
*/
func (m *MySQLMetaStore) Users() ([]*MetaUser, error) {
	return []*MetaUser{}, errNotImplemented
}

/*
Objects (get all oids)
return meta object
*/
func (m *MySQLMetaStore) Objects() ([]*MetaObject, error) {
	return m.findAllOids()
}

/*
Projects (get all projects)
return meta project object
*/
func (m *MySQLMetaStore) Projects() ([]*MetaProject, error) {
	return m.findAllProjects()
}

/*
Auth routine.  Requires an auth string like
"Basic YWRtaW46YWRtaW4="
*/
func (m *MySQLMetaStore) authenticate(authorization string) bool {
	if Config.IsPublic() {
		return true
	}

	if authorization == "" {
		return false
	}

	if !strings.HasPrefix(authorization, "Basic ") {
		return false
	}

	c, err := base64.URLEncoding.DecodeString(strings.TrimPrefix(authorization, "Basic "))
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.authenticate", "msg": err.Error()})
		return false
	}
	cs := string(c)
	i := strings.IndexByte(cs, ':')
	if i < 0 {
		return false
	}
	user, password := cs[:i], cs[i+1:]

	if Config.Ldap.Enabled {
		return authenticateLdap(user, password)
	}

	logger.Log(kv{"fn": "MySQLMetaStore.authenticate", "msg": "Authentication failed, please make sure LDAP is set to true"})
	return false

}
