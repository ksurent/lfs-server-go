package meta

import (
	"errors"

	"golang.org/x/crypto/bcrypt"
)

type authError struct {
	error
}

func IsAuthError(err error) bool {
	_, ok := err.(authError)
	return ok
}

var ErrNotAuthenticated = authError{errors.New("Forbidden")}

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
