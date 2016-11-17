package boltdb

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"strings"
	"time"

	"github.com/ksurent/lfs-server-go/auth/ldap"
	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/logger"
	m "github.com/ksurent/lfs-server-go/meta"

	"github.com/boltdb/bolt"
)

// MetaStore implements a metadata storage. It stores user credentials and Meta information
// for objects. The storage is handled by boltdb.
type MetaStore struct {
	db *bolt.DB
}

var (
	errNoBucket    = errors.New("Bucket not found")
	errUnsupported = errors.New("This feature is not supported by this backend")
)

var (
	usersBucket    = []byte("users")
	objectsBucket  = []byte("objects")
	projectsBucket = []byte("projects")
)

// NewMetaStore creates a new MetaStore using the boltdb database at dbFile.
func NewMetaStore(dbFile string) (*MetaStore, error) {
	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(usersBucket); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(objectsBucket); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(projectsBucket); err != nil {
			return err
		}

		return nil
	})

	return &MetaStore{db: db}, nil
}

// Get() retrieves meta information for a committed object given information in
// m.m.RequestVars
func (s *MetaStore) Get(rv *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(rv.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	meta, err := s.doGet(rv)
	if err != nil {
		return nil, err
	} else if !meta.Existing {
		return nil, m.ErrObjectNotFound
	}

	return meta, nil
}

// Same as Get() but for uncommitted objects
func (s *MetaStore) GetPending(rv *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(rv.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	meta, err := s.doGet(rv)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (s *MetaStore) doGet(rv *m.RequestVars) (*m.Object, error) {
	var meta m.Object
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(objectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		value := bucket.Get([]byte(rv.Oid))
		if len(value) == 0 {
			return m.ErrObjectNotFound
		}

		dec := gob.NewDecoder(bytes.NewBuffer(value))
		return dec.Decode(&meta)
	})

	if err != nil {
		return nil, err
	}

	return &meta, nil
}

func (s *MetaStore) findProject(projectName string) (*m.Project, error) {
	// var projects []*m.Project
	var project *m.Project
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(projectsBucket)
		if bucket == nil {
			return errNoBucket
		}
		val := bucket.Get([]byte(projectName))
		if len(val) < 1 {
			return m.ErrProjectNotFound
		}
		dec := gob.NewDecoder(bytes.NewBuffer(val))
		return dec.Decode(&project)
	})
	if err != nil {
		return nil, err
	}
	if project.Name != "" {
		return project, nil
	}
	return nil, m.ErrProjectNotFound
}

// Currently the OIDS are nil
func (s *MetaStore) createProject(rv *m.RequestVars) error {
	if _, err := s.findProject(rv.Repo); err == nil {
		// already there
		return nil
	}

	if rv.Repo == "" {
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(projectsBucket)
		if bucket == nil {
			// should never get here unless the db is jacked
			return errNoBucket
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		meta := m.Project{Name: rv.Repo, Oids: []string{rv.Oid}}
		err := enc.Encode(meta)
		// Just a bunch o keys
		err = bucket.Put([]byte(rv.Repo), buf.Bytes())
		if err != nil {
			return err
		}

		return nil
	})
	return err
}

// Put() creates uncommitted objects from m.m.RequestVars and stores them in the
// meta store
func (s *MetaStore) Put(rv *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(rv.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	// Don't care here if it's pending or committed
	if meta, err := s.doGet(rv); err == nil {
		return meta, nil
	}

	if rv.Repo != "" {
		err := s.createProject(rv)
		if err != nil {
			return nil, err
		}
	}

	meta := &m.Object{
		Oid:          rv.Oid,
		Size:         rv.Size,
		ProjectNames: []string{rv.Repo},
		Existing:     false,
	}

	err := s.doPut(meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

// Commit() finds uncommitted objects in the meta store using data in
// m.m.RequestVars and commits them
func (s *MetaStore) Commit(rv *m.RequestVars) (*m.Object, error) {
	if !s.authenticate(rv.Authorization) {
		return nil, m.ErrNotAuthenticated
	}

	meta, err := s.GetPending(rv)
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

func (s *MetaStore) doPut(meta *m.Object) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(meta)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(objectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		err = bucket.Put([]byte(meta.Oid), buf.Bytes())
		if err != nil {
			return err
		}

		return nil
	})
}

// Close closes the underlying boltdb.
func (s *MetaStore) Close() {
	s.db.Close()
}

// AddUser adds user credentials to the meta store.
func (s *MetaStore) AddUser(user, pass string) error {
	if config.Config.Ldap.Enabled {
		return ldap.ErrUseLdap
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(usersBucket)
		if bucket == nil {
			return errNoBucket
		}
		encryptedPass, err := m.EncryptPass([]byte(pass))
		if err != nil {
			return err
		}
		if val := bucket.Get([]byte(user)); len(val) > 0 {
			return nil // Already there
		}
		return bucket.Put([]byte(user), []byte(encryptedPass))
	})
	return err
}

// DeleteUser removes user credentials from the meta store.
func (s *MetaStore) DeleteUser(user string) error {
	if config.Config.Ldap.Enabled {
		return ldap.ErrUseLdap
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(usersBucket)
		if bucket == nil {
			return errNoBucket
		}

		err := bucket.Delete([]byte(user))
		return err
	})

	return err
}

// Users returns all m.Users in the meta store
func (s *MetaStore) Users() ([]*m.User, error) {
	if config.Config.Ldap.Enabled {
		return []*m.User{}, ldap.ErrUseLdap
	}
	var users []*m.User

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(usersBucket)
		if bucket == nil {
			return errNoBucket
		}

		bucket.ForEach(func(k, v []byte) error {
			users = append(users, &m.User{Name: string(k)})
			return nil
		})
		return nil
	})

	return users, err
}

// Objects returns all m.Objects in the meta store
func (s *MetaStore) Objects() ([]*m.Object, error) {
	var objects []*m.Object

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(objectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		bucket.ForEach(func(k, v []byte) error {
			var meta m.Object
			dec := gob.NewDecoder(bytes.NewBuffer(v))
			err := dec.Decode(&meta)
			if err != nil {
				return err
			}
			objects = append(objects, &meta)
			return nil
		})
		return nil
	})
	return objects, err
}

// authenticate uses the authorization string to determine whether
// or not to proceed. This server assumes an HTTP Basic auth format.
func (s *MetaStore) authenticate(authorization string) bool {
	if config.Config.IsPublic() {
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
	value := ""

	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(usersBucket)
		if bucket == nil {
			return errNoBucket
		}

		value = string(bucket.Get([]byte(user)))
		return nil
	})
	match, err := m.CheckPass([]byte(value), []byte(password))
	if err != nil {
		logger.Log(err)
	}
	return match
}

func (s *MetaStore) Projects() ([]*m.Project, error) {
	var projects []*m.Project
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(projectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		bucket.ForEach(func(k, v []byte) error {
			var meta m.Project
			dec := gob.NewDecoder(bytes.NewBuffer(v))
			err := dec.Decode(&meta)
			if err != nil {
				return err
			}
			projects = append(projects, &meta)
			return nil
		})
		return nil
	})
	return projects, err
}

// TODO
func (s *MetaStore) AddProject(name string) error {
	return errUnsupported
}
