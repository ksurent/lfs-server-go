package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ksurent/lfs-server-go/config"

	_ "github.com/go-sql-driver/mysql"
)

func NewMySQLSession(cfg *config.MySQLConfig) (*sql.DB, error) {
	err := validateConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("config: %s", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Database)

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

func validateConfig(cfg *config.MySQLConfig) error {
	if len(strings.TrimSpace(cfg.Host)) == 0 {
		return errors.New("MySQL host is not specified")
	}

	if len(strings.TrimSpace(cfg.Database)) == 0 {
		return errors.New("MySQL database is not specified")
	}

	if len(strings.TrimSpace(cfg.Username)) == 0 {
		return errors.New("MySQL username is not specified")
	}

	if len(strings.TrimSpace(cfg.Password)) == 0 {
		return errors.New("MySQL password is not specified")
	}

	return nil
}
