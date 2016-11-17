package main

import "errors"

var (
	errWriteS3             = errors.New("Erred writing to S3")
	errMySQLNotImplemented = errors.New("Not Implemented when using 'bolt' or 'cassandra' meta store backend")
	errMissingParams       = errors.New("Missing params")
)
