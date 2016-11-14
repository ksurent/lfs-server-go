package meta

import (
	"errors"
)

var (
	ErrObjectNotFound  = errors.New("Object not found")
	ErrProjectNotFound = errors.New("Project not found")
	ErrUserNotFound    = errors.New("Unable to find user")
)

// MetaObject is object metadata as seen by the object and metadata stores.
type Object struct {
	Oid          string   `json:"oid" cql:"oid"`
	Size         int64    `json:"size "cql:"size"`
	ProjectNames []string `json:"project_names"`
	Existing     bool
}

// MetaProject is project metadata
type Project struct {
	Name string   `json:"name" cql:"name"`
	Oids []string `json:"oids" cql:"oids"`
}

// MetaUser encapsulates information about a meta store user
type User struct {
	Name     string `cql:"username"`
	Password string ` cql:"password"`
}

// Wrapper for MetaStore so we can use different types
type GenericMetaStore interface {
	Put(v *RequestVars) (*Object, error)
	Get(v *RequestVars) (*Object, error)
	GetPending(v *RequestVars) (*Object, error)
	Commit(v *RequestVars) (*Object, error)
	Close()
	DeleteUser(user string) error
	AddUser(user, pass string) error
	AddProject(projectName string) error
	Users() ([]*User, error)
	Objects() ([]*Object, error)
	Projects() ([]*Project, error)
}
