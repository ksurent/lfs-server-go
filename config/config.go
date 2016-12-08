package config

import (
	"os"
	"runtime"
	"strings"

	"github.com/fatih/structs"
	"gopkg.in/ini.v1"
)

type CassandraConfig struct {
	Hosts        string `json:"hosts"`
	Keyspace     string `json:"keyspace"`
	ProtoVersion int    `json:"ProtoVersion"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Enabled      bool   `json:"enabled"`
}

type AwsConfig struct {
	AccessKeyId     string `json:"accesskeyid"`
	SecretAccessKey string `json:"secretaccesskey"`
	Region          string `json:"region"`
	BucketName      string `json:"bucketname"`
	BucketAcl       string `json:"bucketacl"`
	Enabled         bool   `json:"enabled"`
}

type LdapConfig struct {
	Enabled         bool   `json:"enabled"`
	Server          string `json:"server"`
	Base            string `json:"base"`
	UserObjectClass string `json:"userobjectclass"`
	UserCn          string `json:"usercn"`
	BindDn          string `json:"binddn"`
	BindPass        string `json:"bindpass"`
}

type MySQLConfig struct {
	Host     string `json:"host"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	Enabled  bool   `json:"enabled"`
}

type GraphiteConfig struct {
	Endpoint       string `json:"endpoint"`
	Prefix         string `json:"prefix"`
	AppendHostname bool   `json:"append_hostname"`
	Enabled        bool   `json:"enabled"`
	Interval       string `json:"interval"`
	Timeout        string `json:"timeout"`
}

// Configuration holds application configuration. Values will be pulled from
// environment variables, prefixed by keyPrefix. Default values can be added
// via tags.
type Configuration struct {
	Listen       string           `json:"listen"`
	Host         string           `json:"host"`
	UrlContext   string           `json:"url_context"`
	ContentPath  string           `json:"content_path"`
	Cert         string           `json:"cert"`
	Key          string           `json:"key"`
	Scheme       string           `json:"scheme"`
	Public       bool             `json:"public"`
	MetaDB       string           `json:"metadb"`
	BackingStore string           `json:"backing_store"`
	ContentStore string           `json:"content_store"`
	LogFile      string           `json:"logfile"`
	NumProcs     int              `json:"numprocs"`
	Aws          *AwsConfig       `json:"aws"`
	Cassandra    *CassandraConfig `json:"cassandra"`
	Ldap         *LdapConfig      `json:"ldap"`
	MySQL        *MySQLConfig     `json:"mysql"`
	Graphite     *GraphiteConfig  `json:"graphite"`
}

func (c *Configuration) IsHTTPS() bool {
	return strings.Contains(c.Scheme, "https")
}

func (c *Configuration) UseTLS() bool {
	return c.Cert != "" && c.Key != ""
}

func (c *Configuration) IsPublic() bool {
	return c.Public
}

func (c *Configuration) DumpConfig() map[string]interface{} {
	return structs.Map(c)
}

var GoEnv = os.Getenv("GO_ENV")

func NewFromFile(configFile string) (*Configuration, error) {
	iniCfg, err := ini.Load(configFile)
	if err != nil {
		return nil, err
	}

	if GoEnv == "" {
		GoEnv = "production"
	}

	//Force scheme to be a valid value
	if iniCfg.Section("Main").Key("Scheme").String() != "" {
		val := iniCfg.Section("Main").Key("Scheme").String()
		switch val {
		case
			"http", "https":
			val = val
		default:
			val = "http"
		}
	}

	cfg := &Configuration{
		Listen:       "tcp://:8080",
		Host:         "localhost:8080",
		UrlContext:   "",
		ContentPath:  "lfs-content",
		Cert:         "",
		Key:          "",
		Scheme:       "http",
		Public:       true,
		MetaDB:       "lfs-test.db",
		BackingStore: "bolt",
		ContentStore: "filesystem",
		NumProcs:     runtime.NumCPU(),
		Ldap:         &LdapConfig{},
		Aws:          &AwsConfig{},
		Cassandra:    &CassandraConfig{},
		MySQL:        &MySQLConfig{},
		Graphite:     &GraphiteConfig{},
	}

	for _, v := range []struct {
		section string
		dest    interface{}
	}{
		{"Main", cfg},
		{"Aws", cfg.Aws},
		{"Ldap", cfg.Ldap},
		{"Cassandra", cfg.Cassandra},
		{"MySQL", cfg.MySQL},
		{"Graphite", cfg.Graphite},
	} {
		if err := iniCfg.Section(v.section).MapTo(v.dest); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}
