package cassandra

import (
	"fmt"

	"github.com/ksurent/lfs-server-go/config"

	"github.com/gocql/gocql"
)

type CassandraService struct {
	Client *gocql.Session
}

// TODO: Add auth for cassandra
func NewCassandraSession(cfg *config.CassandraConfig) (*CassandraService, error) {
	cluster := gocql.NewCluster(cfg.Hosts)
	cluster.ProtoVersion = cfg.ProtoVersion

	q := fmt.Sprintf(`
		create keyspace if not exists
			%s
		with replication = {
			'class': 'SimpleStrategy',
			'replication_factor': 1
		};
	`, cfg.Keyspace)

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	err = session.Query(q).Exec()
	if err != nil {
		return nil, err
	}
	session.Close()

	cluster.Keyspace = cfg.Keyspace
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
	q := fmt.Sprintf("create table if not exists projects (name text PRIMARY KEY, oids SET<text>, pending boolean);")
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
	q = fmt.Sprintf(`create table if not exists oids(oid text primary key, size bigint, pending boolean);`)
	session.Query(q).Exec()
	if err != nil {
		return err
	}

	// user management
	q = fmt.Sprintf("create table if not exists users(username text primary key, password text);")
	return session.Query(q).Exec()
}
