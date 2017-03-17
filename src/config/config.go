package config

import (
	"flag"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"fmt"
	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/qlistener/src/service"
	reporter_client "github.com/vostrok/reporter/rpcclient"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
)

type ServerConfig struct {
	Port string `default:"50304"`
}
type AppConfig struct {
	AppName           string                       `yaml:"app_name"`
	Server            ServerConfig                 `yaml:"server"`
	InMemClientConfig inmem_client.ClientConfig    `yaml:"inmem_client"`
	ReporterConfig    reporter_client.ClientConfig `yaml:"reporter_client"`
	Service           service.ServiceConfig        `yaml:"service"`
	Consumer          amqp.ConsumerConfig          `yaml:"consumer"`
	DbConf            db.DataBaseConfig            `yaml:"db"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/appconfig.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}
	if appConfig.AppName == "" {
		log.Fatal("app name must be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.AppName, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Consumer.Conn.Host = envString("RBMQ_HOST", appConfig.Consumer.Conn.Host)

	appConfig.Service.GeoIpPath = envString("GEOIP_PATH", appConfig.Service.GeoIpPath)

	log.WithField("config", fmt.Sprintf("%#v", appConfig)).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
