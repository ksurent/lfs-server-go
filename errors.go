package main

import "errors"

var (
	errMySQLNotImplemented = errors.New("Not Implemented when using 'bolt' or 'cassandra' meta store backend")
	errMissingParams       = errors.New("Missing params")
)
