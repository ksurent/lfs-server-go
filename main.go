package main

import (
	"expvar"
	"flag"
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

	"github.com/facebookgo/pidfile"
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

func findMetaStore(cfg *config.Configuration) (meta.GenericMetaStore, error) {
	logger.Log("Meta store: " + cfg.BackingStore)

	switch cfg.BackingStore {
	case "bolt":
		return boltdb.NewMetaStore(cfg.MetaDB)
	case "cassandra":
		return cassandra.NewCassandraMetaStore(cfg.Cassandra)
	case "mysql":
		return mysql.NewMySQLMetaStore(cfg.MySQL)
	default:
		return boltdb.NewMetaStore(cfg.MetaDB)
	}
}

func findContentStore(cfg *config.Configuration) (content.GenericContentStore, error) {
	logger.Log("Content store: " + cfg.ContentStore)

	switch cfg.ContentStore {
	case "filestore":
		return fs.NewContentStore(cfg.ContentPath)
	case "aws":
		return aws.NewAwsContentStore(cfg.Aws)
	default:
		return fs.NewContentStore(cfg.ContentPath)
	}
}

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit.")
	configFile := flag.String("config", "", "Path to configuration.")

	flag.Parse()

	if *showVersion {
		fmt.Println(BuildVersion)
		os.Exit(0)
	}

	if *configFile == "" {
		logger.Fatal("Need -config")
	}

	cfg, err := config.NewFromFile(*configFile)
	if err != nil {
		logger.Fatal("Failed to parse " + *configFile + ": " + err.Error())
	}

	runtime.GOMAXPROCS(cfg.NumProcs)

	if cfg.IsHTTPS() {
		logger.Log("Will generate https hrefs")
	}

	metaStore, err := findMetaStore(cfg)
	if err != nil {
		logger.Fatal("Could not open the meta store: " + err.Error())
	}

	contentStore, err := findContentStore(cfg)
	if err != nil {
		logger.Fatal("Could not open the content store: " + err.Error())
	}

	if cfg.Graphite.Enabled {
		interval, err := time.ParseDuration(cfg.Graphite.Interval)
		if err != nil {
			logger.Log("Failed to parse Graphite interval (" + err.Error() + "), defaulting to 60 seconds")
			interval = 60 * time.Second
		}

		timeout, err := time.ParseDuration(cfg.Graphite.Timeout)
		if err != nil {
			logger.Log("Failed to parse Graphite timeout (" + err.Error() + "), defaulting to 2 seconds")
			timeout = 2 * time.Second
		}

		graphite = g2g.NewGraphite(cfg.Graphite.Endpoint, interval, timeout)
		defer graphite.Shutdown()

		prefix := strings.Trim(cfg.Graphite.Prefix, ".")

		if cfg.Graphite.AppendHostname {
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

		logger.Log("Graphite metrics prefix: " + prefix)
		logger.Log("Graphite endpoint: " + cfg.Graphite.Endpoint)
	}

	logger.Log("Version: " + BuildVersion)

	expvarVersion.Set(BuildVersion)

	if err := pidfile.Write(); err != nil && !pidfile.IsNotConfigured(err) {
		logger.Fatal(err)
	}

	err = NewApp(cfg, contentStore, metaStore).Serve()
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
