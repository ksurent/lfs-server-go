package fs

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/ksurent/lfs-server-go/content"
	"github.com/ksurent/lfs-server-go/meta"
)

// ContentStore provides a simple file system based storage.
type ContentStore struct {
	basePath string
}

// NewContentStore creates a ContentStore at the base directory.
func NewContentStore(base string) (*ContentStore, error) {
	if err := os.MkdirAll(base, 0750); err != nil {
		return nil, err
	}

	return &ContentStore{base}, nil
}

// Get takes a Meta object and retreives the content from the store, returning
// it as an io.Reader.
func (s *ContentStore) Get(m *meta.Object) (io.ReadCloser, error) {
	path := filepath.Join(s.basePath, content.TransformKey(m.Oid))

	return os.Open(path)
}

// Put takes a Meta object and an io.Reader and writes the content to the store.
func (s *ContentStore) Put(m *meta.Object, r io.Reader) error {
	path := filepath.Join(s.basePath, content.TransformKey(m.Oid))
	tmpPath := path + ".tmp"

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0640)
	if err != nil {
		return err
	}
	defer os.Remove(tmpPath)

	hash := sha256.New()
	hw := io.MultiWriter(hash, file)

	written, err := io.Copy(hw, r)
	if err != nil {
		file.Close()
		return err
	}
	file.Close()

	if written != m.Size {
		return content.ErrSizeMismatch
	}

	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != m.Oid {
		return content.ErrHashMismatch
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

// Exists returns true if the object exists in the content store.
func (s *ContentStore) Exists(m *meta.Object) bool {
	path := filepath.Join(s.basePath, content.TransformKey(m.Oid))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func (s *ContentStore) Verify(m *meta.Object) error {
	path := filepath.Join(s.basePath, content.TransformKey(m.Oid))

	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if stat.Size() != m.Size {
		return content.ErrSizeMismatch
	}

	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()

	buf := bufio.NewReader(fh)
	hash := sha256.New()

	if _, err := buf.WriteTo(hash); err != nil {
		return err
	}

	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != m.Oid {
		return content.ErrHashMismatch
	}

	return nil
}
