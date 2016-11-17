package meta

import (
	"errors"
	"fmt"

	"github.com/ksurent/lfs-server-go/config"

	"golang.org/x/crypto/bcrypt"
)

type authError struct {
	error
}

func IsAuthError(err error) bool {
	_, ok := err.(authError)
	return ok
}

var (
	ErrObjectNotFound   = errors.New("Object not found")
	ErrProjectNotFound  = errors.New("Project not found")
	ErrUserNotFound     = errors.New("Unable to find user")
	ErrNotAuthenticated = authError{errors.New("Forbidden")}
)

// RequestVars contain variables from the HTTP request. Variables from routing, json body decoding, and
// some headers are stored.
type RequestVars struct {
	Oid           string
	Size          int64
	User          string
	Password      string
	Namespace     string
	Repo          string
	Authorization string
}

func (v *RequestVars) ObjectLink() string {
	path := fmt.Sprintf("/%s/%s/objects/%s", v.Namespace, v.Repo, v.Oid)

	if config.Config.IsHTTPS() {
		return fmt.Sprintf("%s://%s%s", config.Config.Scheme, config.Config.Host, path)
	}

	return fmt.Sprintf("http://%s%s", config.Config.Host, path)
}

func (v *RequestVars) VerifyLink() string {
	path := fmt.Sprintf("/%s/%s/verify", v.Namespace, v.Repo)

	if config.Config.IsHTTPS() {
		return fmt.Sprintf("%s://%s%s", config.Config.Scheme, config.Config.Host, path)
	}

	return fmt.Sprintf("http://%s%s", config.Config.Host, path)
}

type BatchVars struct {
	Objects []*RequestVars `json:"objects"`
}

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

func EncryptPass(password []byte) (string, error) {
	// Hashing the password with the cost of 10
	hashedPassword, err := bcrypt.GenerateFromPassword(password, 10)
	return string(hashedPassword), err
}

func CheckPass(hashedPassword, password []byte) (bool, error) {
	// Comparing the password with the hash
	err := bcrypt.CompareHashAndPassword(hashedPassword, password)
	// no error means success
	return (err == nil), nil
}
