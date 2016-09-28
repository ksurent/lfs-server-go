package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
)

const (
	MySQLCommittedTable string = "oids"
	MySQLPendingTable          = "pending_oids"
)

/*
MySQLMetaStore struct.
*/
type MySQLMetaStore struct {
	mysqlService *MySQLService
	client       *sql.DB
}

/*
NewMySQLMetaStore (method update the MySQLMetaStore struct)
*/
func NewMySQLMetaStore(mysqlService ...*MySQLService) (*MySQLMetaStore, error) {
	if len(mysqlService) == 0 {
		mysqlService = append(mysqlService, NewMySQLSession())
	}

	mysql := mysqlService[0]

	if mysql.Fail {
		return nil, errMissingParams
	}
	return &MySQLMetaStore{mysqlService: mysql, client: mysql.Client}, nil
}

/*
Close (method close mysql connection)
*/
func (m *MySQLMetaStore) Close() {
	defer m.client.Close()
	return
}

/*
Oid finder - returns a []*MetaObject
*/
func (m *MySQLMetaStore) findAllOids() ([]*MetaObject, error) {
	rows, err := m.client.Query("select oid, size from oids;")
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

/*
OID Maps
*/
func (m *MySQLMetaStore) mapOid(id int64) ([]string, error) {
	rows, err := m.client.Query("select oid from oid_maps where projectID = ?", id)
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

/*
Project finder - returns a []*MetaProject
*/
func (m *MySQLMetaStore) findAllProjects() ([]*MetaProject, error) {
	rows, err := m.client.Query("select id, name from projects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		name        string
		id          int64
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

// Create project
func (m *MySQLMetaStore) createProject(name string) error {
	_, err := m.client.Exec("insert into projects (name) values (?)", name)
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.createProject", "msg": fmt.Sprintf("MySQL insert query failed with error %s", err)})
		return err
	}
	return nil
}

// Transactionally commit pending object and create/update all dependent data
func (m *MySQLMetaStore) commitPendingObject(meta *MetaObject) error {
	tx, err := m.client.Begin()
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore", "msg": "Could not start a transaction: " + err.Error()})
		return err
	}

	// move from pending to committed
	tx.Exec("insert into "+MySQLCommittedTable+" (oid, size) values (?, ?)", meta.Oid, meta.Size)
	tx.Exec("delete from "+MySQLPendingTable+" where oid = ?", meta.Oid)

	// create projects if needed and update oid -> project relations
	for _, name := range meta.ProjectNames {
		tx.Exec("insert ignore into projects (name) values (?)", name)
		tx.Exec("insert into oid_maps (oid, projectID) values (?, ?)", meta.Oid, name)
	}

	return tx.Commit()
}

// Create pending oid
func (m *MySQLMetaStore) createPendingOid(oid string, size int64) error {
	_, err := m.client.Exec("insert into pending_oids (oid, size) values (?, ?)", oid, size)

	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.createPendingOid", "msg": err})
		return nil
	}
	return nil
}

func (m *MySQLMetaStore) findPendingOid(oid string) (*MetaObject, error) {
	meta, err := m.doFindOid(oid, MySQLPendingTable)
	if err != nil {
		return nil, err
	}

	meta.Existing = false

	return meta, nil
}

func (m *MySQLMetaStore) findOid(oid string) (*MetaObject, error) {
	meta, err := m.doFindOid(oid, MySQLCommittedTable)
	if err != nil {
		return nil, err
	}

	meta.Existing = true

	return meta, nil
}

func (m *MySQLMetaStore) doFindOid(oid, table string) (*MetaObject, error) {
	var meta MetaObject

	err := m.client.QueryRow("select oid, size from "+table+" where oid = ?", oid).Scan(&meta.Oid, &meta.Size)
	if err != nil {
		return nil, err
	}

	if meta.Oid == "" {
		return nil, errObjectNotFound
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
	if meta, err := m.doGet(v); err == nil {
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
		if err := m.createPendingOid(meta.Oid, meta.Size); err != nil {
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

	meta, err := m.doGet(v)
	if err != nil {
		return nil, err
	} else if !meta.Existing {
		return nil, errObjectNotFound
	}

	return meta, nil
}

// Get() retrieves meta information for a committed object given information in
// RequestVars
func (m *MySQLMetaStore) GetPending(v *RequestVars) (*MetaObject, error) {
	if !m.authenticate(v.Authorization) {
		return nil, newAuthError()
	}

	meta, err := m.doGet(v)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (m *MySQLMetaStore) doGet(v *RequestVars) (*MetaObject, error) {

	if meta, err := m.findOid(v.Oid); err == nil {
		meta.Existing = true
		return meta, nil
	}

	if meta, err := m.findPendingOid(v.Oid); err == nil {
		meta.Existing = false
		return meta, nil
	}

	return nil, errObjectNotFound
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
	err := m.createProject(name)
	return err
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
	ao, err := m.findAllOids()
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.Objects", "msg": err.Error()})
	}
	return ao, err
}

/*
Projects (get all projects)
return meta project object
*/
func (m *MySQLMetaStore) Projects() ([]*MetaProject, error) {
	ao, err := m.findAllProjects()
	if err != nil {
		logger.Log(kv{"fn": "MySQLMetaStore.Projects", "msg": err.Error()})
	}
	return ao, err
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
