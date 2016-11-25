package boltdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/ksurent/lfs-server-go/meta"

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
// meta.RequestVars
func (s *MetaStore) Get(rv *meta.RequestVars) (*meta.Object, error) {
	m, err := s.doGet(rv)
	if err != nil {
		return nil, err
	} else if !m.Existing {
		return nil, meta.ErrObjectNotFound
	}

	return m, nil
}

// Same as Get() but for uncommitted objects
func (s *MetaStore) GetPending(rv *meta.RequestVars) (*meta.Object, error) {
	return s.doGet(rv)
}

func (s *MetaStore) doGet(rv *meta.RequestVars) (*meta.Object, error) {
	var m meta.Object
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(objectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		value := bucket.Get([]byte(rv.Oid))
		if len(value) == 0 {
			return meta.ErrObjectNotFound
		}

		dec := gob.NewDecoder(bytes.NewBuffer(value))
		return dec.Decode(&m)
	})

	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *MetaStore) findProject(projectName string) (*meta.Project, error) {
	var project *meta.Project
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(projectsBucket)
		if bucket == nil {
			return errNoBucket
		}
		val := bucket.Get([]byte(projectName))
		if len(val) < 1 {
			return meta.ErrProjectNotFound
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
	return nil, meta.ErrProjectNotFound
}

// Currently the OIDS are nil
func (s *MetaStore) createProject(rv *meta.RequestVars) error {
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
		m := meta.Project{Name: rv.Repo, Oids: []string{rv.Oid}}
		err := enc.Encode(m)
		// Just a bunch o keys
		err = bucket.Put([]byte(rv.Repo), buf.Bytes())
		if err != nil {
			return err
		}

		return nil
	})
	return err
}

// Put() creates uncommitted objects from meta.RequestVars and stores them in the
// meta store
func (s *MetaStore) Put(rv *meta.RequestVars) (*meta.Object, error) {
	// Don't care here if it's pending or committed
	if m, err := s.doGet(rv); err == nil {
		return m, nil
	}

	if rv.Repo != "" {
		err := s.createProject(rv)
		if err != nil {
			return nil, err
		}
	}

	m := &meta.Object{
		Oid:          rv.Oid,
		Size:         rv.Size,
		ProjectNames: []string{rv.Repo},
		Existing:     false,
	}

	err := s.doPut(m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Commit() finds uncommitted objects in the meta store using data in
// meta.RequestVars and commits them
func (s *MetaStore) Commit(rv *meta.RequestVars) (*meta.Object, error) {
	m, err := s.GetPending(rv)
	if err != nil {
		return nil, err
	}

	m.Existing = true

	err = s.doPut(m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (s *MetaStore) doPut(m *meta.Object) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(m)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(objectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		err = bucket.Put([]byte(m.Oid), buf.Bytes())
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
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(usersBucket)
		if bucket == nil {
			return errNoBucket
		}
		encryptedPass, err := meta.EncryptPass([]byte(pass))
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

// Users returns all meta.Users in the meta store
func (s *MetaStore) Users() ([]*meta.User, error) {
	var users []*meta.User

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(usersBucket)
		if bucket == nil {
			return errNoBucket
		}

		bucket.ForEach(func(k, v []byte) error {
			users = append(users, &meta.User{Name: string(k)})
			return nil
		})
		return nil
	})

	return users, err
}

// Objects returns all meta.Objects in the meta store
func (s *MetaStore) Objects() ([]*meta.Object, error) {
	var objects []*meta.Object

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(objectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		bucket.ForEach(func(k, v []byte) error {
			var m meta.Object
			dec := gob.NewDecoder(bytes.NewBuffer(v))
			err := dec.Decode(&m)
			if err != nil {
				return err
			}
			objects = append(objects, &m)
			return nil
		})
		return nil
	})
	return objects, err
}

// Authenticate uses the authorization string to determine whether
// or not to proceed. This server assumes an HTTP Basic auth format.
func (s *MetaStore) Authenticate(user, pass string) (bool, error) {
	var encryptedPass []byte
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(usersBucket)
		if bucket == nil {
			return errNoBucket
		}

		encryptedPass = bucket.Get([]byte(user))
		return nil
	})

	return meta.CheckPass(encryptedPass, []byte(pass))
}

func (s *MetaStore) Projects() ([]*meta.Project, error) {
	var projects []*meta.Project
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(projectsBucket)
		if bucket == nil {
			return errNoBucket
		}

		bucket.ForEach(func(k, v []byte) error {
			var m meta.Project
			dec := gob.NewDecoder(bytes.NewBuffer(v))
			err := dec.Decode(&m)
			if err != nil {
				return err
			}
			projects = append(projects, &m)
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
