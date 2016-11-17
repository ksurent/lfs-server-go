package cassandra

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
func NewCassandraSession() (*CassandraService, error) {
	cluster := gocql.NewCluster(config.Config.Cassandra.Hosts)
	cluster.ProtoVersion = config.Config.Cassandra.ProtoVersion

	logger.Log("Connecting to " + config.Config.Cassandra.Hosts)

	keyspace := fmt.Sprintf("%s_%s", config.Config.Cassandra.Keyspace, config.GoEnv)
	logger.Log("Using keyspace " + keyspace)

	q := fmt.Sprintf(`
		create keyspace if not exists
			%s
		with replication = {
			'class': 'SimpleStrategy',
			'replication_factor': 1
		};
	`, keyspace)

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	err = session.Query(q).Exec()
	if err != nil {
		return nil, err
	}
	session.Close()

	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum

	session, err = cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	err = initializeCassandra(session)
	if err != nil {
		return nil, err
	}

	return &CassandraService{Client: session}, nil
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
	sess, err := NewCassandraSession()
	if err != nil {
		return err
	}
	return sess.Client.Query(q).Exec()
}
