package main

import (
	"expvar"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/content"
	"github.com/ksurent/lfs-server-go/content/aws"
	"github.com/ksurent/lfs-server-go/content/fs"
	"github.com/ksurent/lfs-server-go/logger"
	"github.com/ksurent/lfs-server-go/meta"
	"github.com/ksurent/lfs-server-go/meta/boltdb"
	"github.com/ksurent/lfs-server-go/meta/cassandra"
	"github.com/ksurent/lfs-server-go/meta/mysql"

	"github.com/peterbourgon/g2g"
)

const (
	contentMediaType = "application/vnd.git-lfs"
	metaMediaType    = contentMediaType + "+json"
)

var (
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

var graphite *g2g.Graphite

func findMetaStore() (meta.GenericMetaStore, error) {
	switch config.Config.BackingStore {
	case "bolt":
		return boltdb.NewMetaStore(config.Config.MetaDB)
	case "cassandra":
		return cassandra.NewCassandraMetaStore()
	case "mysql":
		return mysql.NewMySQLMetaStore()
	default:
		return boltdb.NewMetaStore(config.Config.MetaDB)
	}
}

func findContentStore() (content.GenericContentStore, error) {
	logger.Log("Using content store " + config.Config.ContentStore)

	switch config.Config.ContentStore {
	case "filestore":
		return fs.NewContentStore(config.Config.ContentPath)
	case "aws":
		return aws.NewAwsContentStore()
	default:
		return fs.NewContentStore(config.Config.ContentPath)
	}
}
func main() {
	if len(os.Args) == 2 && os.Args[1] == "-v" {
		fmt.Println(BuildVersion)
		os.Exit(0)
	}

	runtime.GOMAXPROCS(config.Config.NumProcs)

	if config.Config.IsHTTPS() {
		logger.Log("Will generate https hrefs")
	}

	metaStore, err := findMetaStore()
	if err != nil {
		logger.Fatal("Could not open the meta store: " + err.Error())
	}

	contentStore, err := findContentStore()
	if err != nil {
		logger.Fatal("Could not open the content store: " + err.Error())
	}

	if config.Config.Graphite.Enabled {
		graphite = g2g.NewGraphite(
			config.Config.Graphite.Endpoint,
			time.Duration(config.Config.Graphite.IntervalS)*time.Second,
			time.Duration(config.Config.Graphite.TimeoutMs)*time.Millisecond,
		)

		prefix := strings.Trim(config.Config.Graphite.Prefix, ".")

		if config.Config.Graphite.AppendHostname {
			host, err := os.Hostname()
			if err != nil {
				logger.Log("Could not detect hostname: " + err.Error())
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

		logger.Log("Graphite metrics prefix is " + prefix)
		logger.Log("Sending metrics to " + config.Config.Graphite.Endpoint)

		defer graphite.Shutdown()
	}

	logger.Log("Version: " + BuildVersion)

	expvarVersion.Set(BuildVersion)


	app := NewApp(contentStore, metaStore)
	err = app.Serve()
	if err != nil {
		logger.Fatal(err)
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
