package content

import (
	"errors"
	"io"
	"path/filepath"

	m "github.com/ksurent/lfs-server-go/meta"
)

var (
	ErrSizeMismatch = errors.New("Content size does not match")
	ErrHashMismatch = errors.New("Content has does not match OID")
)

type GenericContentStore interface {
	Get(*m.Object) (io.ReadCloser, error)
	Put(*m.Object, io.Reader) error
	Exists(*m.Object) bool
	Verify(*m.Object) error
}

func TransformKey(key string) string {
	if len(key) < 5 {
		return key
	}

	return filepath.Join(key[0:2], key[2:4], key[4:len(key)])
}
