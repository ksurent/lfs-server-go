package main

import (
	"crypto/tls"
	"expvar"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/peterbourgon/g2g"
)

const (
	contentMediaType = "application/vnd.git-lfs"
	metaMediaType    = contentMediaType + "+json"
)

var (
	logger       = NewKVLogger(os.Stdout)
	BuildVersion = "0.1.0"
)

var (
	metaResponse     = expvar.NewMap("meta")
	downloadResponse = expvar.NewMap("download")
	uploadResponse   = expvar.NewMap("upload")

	metaPending   = expvar.NewInt("pending_objects")
	totalRequests = expvar.NewInt("total_requests")

	expvarVersion = expvar.NewString("BuildVersion")
)

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

var graphite *g2g.Graphite

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}

func wrapHttps(l net.Listener, cert, key string) (net.Listener, error) {
	var err error

	config := &tls.Config{}

	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1"}
	}

	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	netListener := l.(*TrackingListener).Listener

	tlsListener := tls.NewListener(tcpKeepAliveListener{netListener.(*net.TCPListener)}, config)
	return tlsListener, nil
}

func FindMetaStore() (GenericMetaStore, error) {
	switch Config.BackingStore {
	case "bolt":
		m, err := NewMetaStore(Config.MetaDB)
		return m, err
	case "cassandra":
		m, err := NewCassandraMetaStore(NewCassandraSession())
		return m, err
	case "mysql":
		db, err := NewMySQLSession()
		if err != nil {
			return nil, err
		}
		return NewMySQLMetaStore(db)
	default:
		m, err := NewMetaStore(Config.MetaDB)
		return m, err
	}
}

func findContentStore() (GenericContentStore, error) {
	logger.Log(kv{"fn": "findContentStore", "msg": fmt.Sprintf("Using ContentStore %s", Config.ContentStore)})
	switch Config.ContentStore {
	case "filestore":
		return NewContentStore(Config.ContentPath)
	case "aws":
		return NewAwsContentStore()
	default:
		return NewContentStore(Config.ContentPath)
	}
}
func main() {
	if len(os.Args) == 2 && os.Args[1] == "-v" {
		fmt.Println(BuildVersion)
		os.Exit(0)
	}

	var listener net.Listener
	runtime.GOMAXPROCS(Config.NumProcs)

	tl, err := NewTrackingListener(Config.Listen)
	if err != nil {
		logger.Fatal(kv{"fn": "main", "err": "Could not create listener: " + err.Error()})
	}

	listener = tl

	if Config.IsHTTPS() {
		if Config.UseTLS() {
			logger.Log(kv{"fn": "main", "msg": "Using tls"})
			listener, err = wrapHttps(tl, Config.Cert, Config.Key)
			if err != nil {
				logger.Fatal(kv{"fn": "main", "err": "Could not create https listener: " + err.Error()})
			}
		} else {
			logger.Log(kv{"fn": "main", "msg": "Will generate https hrefs"})
		}
	}

	metaStore, err := FindMetaStore()
	if err != nil {
		logger.Fatal(kv{"fn": "main", "err": "Could not open the meta store: " + err.Error()})
	}

	contentStore, err := findContentStore()
	if err != nil {
		logger.Fatal(kv{"fn": "main", "err": "Could not open the content store: " + err.Error()})
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	go func(c chan os.Signal, listener net.Listener) {
		for {
			sig := <-c
			switch sig {
			case syscall.SIGHUP: // Graceful shutdown
				tl.Close()
			}
		}
	}(c, tl)

	if Config.Graphite.Enabled {
		graphite = g2g.NewGraphite(
			Config.Graphite.Endpoint,
			time.Duration(Config.Graphite.IntervalS)*time.Second,
			time.Duration(Config.Graphite.TimeoutMs)*time.Millisecond,
		)

		prefix := strings.Trim(Config.Graphite.Prefix, ".")

		if Config.Graphite.AppendHostname {
			host, err := os.Hostname()
			if err != nil {
				logger.Log(kv{"fn": "main", "msg": "Could not detect hostname: " + err.Error()})
				host = "localhost"
			}
			host = strings.Replace(host, ".", "_", -1)

			if prefix == "" {
				prefix = host
			} else {
				prefix += "." + host
			}
		}

		setupGraphiteMetrics(prefix, graphite)

		logger.Log(kv{"fn": "main", "msg": "Sending metrics", "prefix": prefix, "endpoint": Config.Graphite.Endpoint})
	}

	logger.Log(kv{"fn": "main", "msg": "listening", "pid": os.Getpid(), "addr": Config.Listen, "version": BuildVersion})

	expvarVersion.Set(BuildVersion)

	app := NewApp(contentStore, metaStore)
	app.Serve(listener)
	tl.WaitForChildren()

	if Config.Graphite.Enabled {
		graphite.Shutdown()
	}
}

func setupGraphiteMetrics(prefix string, graphite *g2g.Graphite) {
	for _, v := range []struct {
		m    *expvar.Map
		name string
	}{
		{downloadResponse, "download"},
		{uploadResponse, "upload"},
		{metaResponse, "meta"},
	} {
		var mapPrefix string
		if prefix == "" {
			mapPrefix = v.name
		} else {
			mapPrefix = prefix + "." + v.name
		}

		for _, code := range []string{"200", "202", "401", "403", "404", "500"} {
			v.m.Set(code, new(expvar.Int))
			graphite.Register(mapPrefix+".http_"+code, v.m.Get(code))
		}
	}

	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == "memstats" || kv.Key == "cmdline" {
			// skip built-in vars
			return
		}

		if kv.Key == "download" || kv.Key == "upload" || kv.Key == "meta" {
			return
		}

		var path string
		if prefix == "" {
			path = kv.Key
		} else {
			path = prefix + "." + kv.Key
		}

		graphite.Register(path, kv.Value)
	})
}
