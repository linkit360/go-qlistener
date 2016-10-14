package service

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
)

type DataBaseConfig struct {
	ConnMaxLifetime  int    `default:"-1" yaml:"conn_ttl"`
	MaxOpenConns     int    `default:"15" yaml:"max_conns"`
	ReconnectTimeout int    `default:"10" yaml:"timeout"`
	User             string `default:""`
	Pass             string `default:""`
	Port             string `default:""`
	Name             string `default:""`
	Host             string `default:""`
	SSLMode          string `default:"disabled"`
	TablePrefix      string `default:""`
}

func (dbConfig DataBaseConfig) GetConnStr() string {
	dsn := "postgres://" +
		dbConfig.User + ":" +
		dbConfig.Pass + "@" +
		dbConfig.Host + ":" +
		dbConfig.Port + "/" +
		dbConfig.Name + "?sslmode=" +
		dbConfig.SSLMode
	return dsn
}

func initDatabase(dbConf DataBaseConfig) {
	svc.dbConfig = dbConf
	svc.connect()
}

func (svc *Service) connect() {

	if svc.db != nil {
		return
	}

	var err error
	svc.db, err = sql.Open("postgres", svc.dbConfig.GetConnStr())
	if err != nil {
		fmt.Printf("open error %s, dsn: %s", err.Error(), svc.dbConfig.GetConnStr())
		log.WithField("error", err.Error()).Fatal("db connect")
	}
	if err = svc.db.Ping(); err != nil {
		fmt.Printf("ping error %s, dsn: %s", err.Error(), svc.dbConfig.GetConnStr())
		log.WithField("error", err.Error()).Fatal("db ping")
	}

	svc.db.SetMaxOpenConns(svc.dbConfig.MaxOpenConns)
	svc.db.SetConnMaxLifetime(time.Second * time.Duration(svc.dbConfig.ConnMaxLifetime))

	log.WithFields(log.Fields{
		"host": svc.dbConfig.Host, "dbname": svc.dbConfig.Name, "user": svc.dbConfig.User}).Debug("database connected")
	return
}
