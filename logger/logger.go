package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
)

func Log(data interface{}) {
	pc, file, line, ok := runtime.Caller(2)
	if ok {
		file = path.Base(file)
	} else {
		file = "???"
		line = 0
	}

	fname := "???"
	if ok {
		if f := runtime.FuncForPC(pc); f != nil {
			fname = f.Name()
		}
	}

	log.Println(fmt.Sprintf("[%s:%s:%d]: %s", fname, file, line, data))
}

func Fatal(data interface{}) {
	Log(data)
	os.Exit(1)
}

func SetOutput(w io.Writer) {
	log.SetOutput(w)
}
