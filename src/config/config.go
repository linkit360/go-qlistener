package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-qlistener/src/service"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/db"
)

type ServerConfig struct {
	Host string `default:"127.0.0.1" yaml:"host"`
	Port string `default:"50304" yaml:"port"`
}
type AppConfig struct {
	AppName  string                  `yaml:"app_name"`
	Server   ServerConfig            `yaml:"server"`
	MidConf  mid_client.ClientConfig `yaml:"mid_client"`
	Service  service.ServiceConfig   `yaml:"service"`
	Notifier amqp.NotifierConfig     `yaml:"notifier"`
	Consumer amqp.ConsumerConfig     `yaml:"consumer"`
	DbConf   db.DataBaseConfig       `yaml:"db"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/qlistener.yml", "configuration yml file")
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
