package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net"

	log "github.com/Sirupsen/logrus"
	"github.com/oschwald/geoip2-golang"
	amqp_driver "github.com/streadway/amqp"
	"github.com/ua-parser/uap-go/uaparser"

	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
)

var svc Service

const ACTIVE_STATUS = 1

func InitService(sConf ServiceConfig, dbConf db.DataBaseConfig, notifConf amqp.ConsumerConfig) {
	log.SetLevel(log.DebugLevel)

	var err error

	svc.db = db.Init(dbConf)
	svc.sConfig = sConf
	svc.dbConf = dbConf
	svc.ipDb, err = geoip2.Open(sConf.GeoIpPath)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("geoip init")
	}
	svc.uaparser, err = uaparser.New(sConf.UAParserRegexesPath)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("User Agent Parser init")
	}

	svc.m = newMetrics()

	svc.consumer = amqp.NewConsumer(notifConf)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}

	// access campaign queue
	amqp.InitQueue(
		svc.consumer,
		svc.accessCampaignChan,
		processAccessCampaign,
		sConf.ThreadsCount,
		sConf.Queue.AccessCampaign,
		sConf.Queue.AccessCampaign,
	)

	// content sent queue
	amqp.InitQueue(
		svc.consumer,
		svc.contentSentChan,
		processContentSent,
		sConf.ThreadsCount,
		sConf.Queue.ContentSent,
		sConf.Queue.ContentSent,
	)

	// user actions queue
	amqp.InitQueue(
		svc.consumer,
		svc.userActionsChan,
		processUserActions,
		sConf.ThreadsCount,
		sConf.Queue.UserActions,
		sConf.Queue.UserActions,
	)

	// operator transactions queue
	amqp.InitQueue(
		svc.consumer,
		svc.operatorTransactionsChan,
		operatorTransactions,
		sConf.ThreadsCount,
		sConf.Queue.OperatorTransactions,
		sConf.Queue.OperatorTransactions,
	)

	// CQR-s
	if err := initInMem(); err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("init mem")
	}
}

type Service struct {
	db                       *sql.DB
	consumer                 *amqp.Consumer
	contentSentChan          <-chan amqp_driver.Delivery
	accessCampaignChan       <-chan amqp_driver.Delivery
	userActionsChan          <-chan amqp_driver.Delivery
	operatorTransactionsChan <-chan amqp_driver.Delivery
	ipDb                     *geoip2.Reader
	uaparser                 *uaparser.Parser
	sConfig                  ServiceConfig
	dbConf                   db.DataBaseConfig
	tables                   map[string]struct{}
	m                        Metrics
}
type QueuesConfig struct {
	AccessCampaign       string `default:"access_campaign" yaml:"access_campaign"`
	ContentSent          string `default:"content_sent" yaml:"content_sent"`
	UserActions          string `default:"user_actions" yaml:"user_actions"`
	OperatorTransactions string `default:"operator_transactions" yaml:"operator_transactions"`
}
type ServiceConfig struct {
	GeoIpPath             string       `yaml:"geoip_path" default:"dev/GeoLite2-City.mmdb"`
	SubscriptionsLoadDays int          `default:"10" yaml:"subscriptions_load_days"`
	ThreadsCount          int          `default:"1" yaml:"threads_count"`
	UAParserRegexesPath   string       `default:"/home/centos/linkit/regexes.yaml" yaml:"ua_parser_regexes_path"`
	Queue                 QueuesConfig `yaml:"queue"`
	Tables                []string     `yaml:"tables"`
}

type IpInfo struct {
	Ip                  string
	Country             string
	Iso                 string
	City                string
	Timezone            string
	Latitude            float64
	Longitude           float64
	MetroCode           uint
	PostalCode          string
	Subdivisions        string
	IsAnonymousProxy    bool
	IsSatelliteProvider bool
	AccuracyRadius      uint16
}

func geoIp(ip string) (IpInfo, error) {
	if ip == "" {
		return IpInfo{}, errors.New("GeoIP Parse: Empty IP")
	}
	record, err := svc.ipDb.City(net.ParseIP(ip))
	if err != nil {
		return IpInfo{}, fmt.Errorf("GeoIP Parse City: IP: %s: error: %s", ip, err.Error())
	}
	ipInfo := IpInfo{
		Ip:                  ip,                         // => 81.2.69.142
		Country:             record.Country.Names["en"], // => United Kingdom
		Iso:                 record.Country.IsoCode,     // => GB
		City:                record.City.Names["en"],    //  => Arnold
		Timezone:            record.Location.TimeZone,   // => Europe/London
		Latitude:            record.Location.Latitude,   // => 53
		Longitude:           record.Location.Longitude,  // => -1.1333
		MetroCode:           record.Location.MetroCode,
		AccuracyRadius:      record.Location.AccuracyRadius,
		PostalCode:          record.Postal.Code, // => NG5
		IsAnonymousProxy:    record.Traits.IsAnonymousProxy,
		IsSatelliteProvider: record.Traits.IsSatelliteProvider,
	}
	if len(record.Subdivisions) > 0 {
		ipInfo.Subdivisions = record.Subdivisions[0].Names["en"] // => England
	}
	return ipInfo, nil
}
