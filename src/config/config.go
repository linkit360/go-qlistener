package config

import (
	"flag"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/db"
	"github.com/vostrok/qlistener/src/service"
	"github.com/vostrok/rabbit"
)

type ServerConfig struct {
	Port string `default:"50304"`
}
type AppConfig struct {
	Server   ServerConfig          `yaml:"server"`
	Service  service.ServiceConfig `yaml:"service"`
	Consumer rabbit.ConsumerConfig `yaml:"consumer"`
	DbConf   db.DataBaseConfig     `yaml:"db"`
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

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Consumer.Connection.Host = envString("RBMQ_HOST", appConfig.Consumer.Connection.Host)

	appConfig.Service.GeoIpPath = envString("GEOIP_PATH", appConfig.Service.GeoIpPath)

	log.WithField("config", appConfig).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
