package logger

import (
	"os"
)

var defaultLogger = NewKVLogger(os.Stdout)

func Log(data Kv) {
	defaultLogger.Log(data)
}

func Fatal(data Kv) {
	defaultLogger.Log(data)
	os.Exit(1)
}
