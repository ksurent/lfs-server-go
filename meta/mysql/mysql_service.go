package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ksurent/lfs-server-go/config"

	_ "github.com/go-sql-driver/mysql"
)

func NewMySQLSession() (*sql.DB, error) {
	err := validateConfig()
	if err != nil {
		return nil, fmt.Errorf("config: %s", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		config.Config.MySQL.Username,
		config.Config.MySQL.Password,
		config.Config.MySQL.Host,
		config.Config.MySQL.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping: %s", err)
	}

	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("creating tables: %s", err)
	}

	return db, nil
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

func validateConfig() error {
	if len(strings.TrimSpace(config.Config.MySQL.Host)) == 0 {
		return errors.New("MySQL host is not specified")
	}

	if len(strings.TrimSpace(config.Config.MySQL.Database)) == 0 {
		return errors.New("MySQL database is not specified")
	}

	if len(strings.TrimSpace(config.Config.MySQL.Username)) == 0 {
		return errors.New("MySQL username is not specified")
	}

	if len(strings.TrimSpace(config.Config.MySQL.Password)) == 0 {
		return errors.New("MySQL password is not specified")
	}

	return nil
}
