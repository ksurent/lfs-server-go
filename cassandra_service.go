package main

import (
	"fmt"

	"github.com/ksurent/lfs-server-go/config"
	"github.com/ksurent/lfs-server-go/logger"

	"github.com/gocql/gocql"
)

type CassandraService struct {
	Client *gocql.Session
}

// TODO: Add auth for cassandra
func NewCassandraSession() *CassandraService {
	cluster := gocql.NewCluster(config.Config.Cassandra.Hosts)
	cluster.ProtoVersion = config.Config.Cassandra.ProtoVersion
	q := fmt.Sprintf("create keyspace if not exists %s_%s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", config.Config.Cassandra.Keyspace, config.GoEnv)
	session, err := cluster.CreateSession()
	err = session.Query(q).Exec()
	session.Close()
	cluster.Keyspace = fmt.Sprintf("%s_%s", config.Config.Cassandra.Keyspace, config.GoEnv)
	cluster.Consistency = gocql.Quorum
	session, err = cluster.CreateSession()
	perror(initializeCassandra(session))
	perror(err)
	logger.Log(logger.Kv{"fn": "cassandra_service", "msg": fmt.Sprintf("Connecting to host '%s'\n", config.Config.Cassandra.Hosts)})
	logger.Log(logger.Kv{"fn": "cassandra_service", "msg": fmt.Sprintf("Cassandra.namespace '%s_%s'\n", config.Config.Cassandra.Keyspace, config.GoEnv)})
	return &CassandraService{Client: session}
}

func initializeCassandra(session *gocql.Session) error {
	// projects table
	q := fmt.Sprintf("create table if not exists projects (name text PRIMARY KEY, oids SET<text>);")
	err := session.Query(q).Exec()
	if err != nil {
		return err
	}

	// create an index so we can search on oids
	q = fmt.Sprintf("create index if not exists on projects(oids);")
	err = session.Query(q).Exec()
	if err != nil {
		return err
	}

	// Oids table
	q = fmt.Sprintf("create table if not exists oids(oid text primary key, size bigint);")
	session.Query(q).Exec()
	if err != nil {
		return err
	}

	// Pending table
	q = fmt.Sprintf("create table if not exists pending_oids(oid text primary key, size bigint);")
	session.Query(q).Exec()
	if err != nil {
		return err
	}

	// user management
	q = fmt.Sprintf("create table if not exists users(username text primary key, password text);")
	return session.Query(q).Exec()
}

func DropCassandra(session *gocql.Session) error {
	m := fmt.Sprintf("%s_%s", config.Config.Cassandra.Keyspace, config.GoEnv)
	q := fmt.Sprintf("drop keyspace %s;", m)
	c := NewCassandraSession().Client
	return c.Query(q).Exec()
}
