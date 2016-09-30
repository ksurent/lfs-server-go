package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

/*
MySQLService struct
*/
type MySQLService struct {
	Client *sql.DB
	Fail   bool
}

/*
NewMySQLSession (method used in mysql_meta_store.go)
create requeired table and return sql client object
*/
func NewMySQLSession() *MySQLService {

	validate := validateConfig()

	if validate {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s",
			Config.MySQL.Username,
			Config.MySQL.Password,
			Config.MySQL.Host,
			Config.MySQL.Database)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return &MySQLService{Fail: true}
		}

		if err := createTables(db); err != nil {
			return &MySQLService{Fail: true}
		}

		return &MySQLService{Client: db}
	}

	logger.Log(kv{"fn": "NewMySQLSession", "msg": "MySQL configuration validation failed"})
	return &MySQLService{Fail: true}
}

func createTables(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	tx.Exec(`
		create table if not exists
			projects(
				id int not null auto_increment primary key,
				name varchar(255) not null unique,
				pending tinyint(1) unsigned not null default 1
			)
		engine=innodb
	`)

	tx.Exec(`
		create table if not exists
			oids(
				oid char(64) not null primary key,
				size bigint not null,
				pending tinyint unsigned not null default 1
			)
		engine=innodb
	`)

	tx.Exec(`
		create table if not exists
			oid_maps(
				id int not null auto_increment primary key,
				oid char(64) not null,
				projectID int not null,

				index (projectID)
			)
		engine=innodb
	`)

	return tx.Commit()
}

func validateConfig() bool {
	if len(strings.TrimSpace(Config.MySQL.Database)) == 0 && len(strings.TrimSpace(Config.MySQL.Host)) == 0 {
		logger.Log(kv{"fn": "NewMySQLSession", "msg": "Require Host and Database to connect MySQL "})
		return false
	}

	if len(strings.TrimSpace(Config.MySQL.Username)) == 0 && len(strings.TrimSpace(Config.MySQL.Password)) == 0 {
		logger.Log(kv{"fn": "NewMySQLSession", "msg": "Require Username and Password to connect MySQL "})
		return false
	}

	return true
}
