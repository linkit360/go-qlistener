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

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
)

var svc Service

const ACTIVE_STATUS = 1

func InitService(
	sConf ServiceConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	consumerConf amqp.ConsumerConfig,
) {
	log.SetLevel(log.DebugLevel)

	inmem_client.Init(inMemConfig)
	svc.db = db.Init(dbConf)
	svc.sConfig = sConf
	svc.dbConf = dbConf

	var err error
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

	svc.consumer = Consumers{
		Access:      amqp.NewConsumer(consumerConf, sConf.Queue.AccessCampaign.Name, sConf.Queue.AccessCampaign.PrefetchCount),
		UserActions: amqp.NewConsumer(consumerConf, sConf.Queue.UserActions.Name, sConf.Queue.UserActions.PrefetchCount),
		ContentSent: amqp.NewConsumer(consumerConf, sConf.Queue.ContentSent.Name, sConf.Queue.ContentSent.PrefetchCount),
		Operator:    amqp.NewConsumer(consumerConf, sConf.Queue.TransactionLog.Name, sConf.Queue.TransactionLog.PrefetchCount),
		MTManager:   amqp.NewConsumer(consumerConf, sConf.Queue.MTManager.Name, sConf.Queue.MTManager.PrefetchCount),
		Pixels:      amqp.NewConsumer(consumerConf, sConf.Queue.PixelSent.Name, sConf.Queue.PixelSent.PrefetchCount),
	}
	if err := svc.consumer.Access.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}
	if err := svc.consumer.UserActions.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}
	if err := svc.consumer.ContentSent.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}
	if err := svc.consumer.Operator.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}
	if err := svc.consumer.MTManager.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}
	if err := svc.consumer.Pixels.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}

	// access campaign queue
	amqp.InitQueue(
		svc.consumer.Access,
		svc.accessCampaignChan,
		processAccessCampaign,
		sConf.Queue.AccessCampaign.ThreadsCount,
		sConf.Queue.AccessCampaign.Name,
		sConf.Queue.AccessCampaign.Name,
	)

	// content sent queue
	amqp.InitQueue(
		svc.consumer.ContentSent,
		svc.contentSentChan,
		processContentSent,
		sConf.Queue.ContentSent.ThreadsCount,
		sConf.Queue.ContentSent.Name,
		sConf.Queue.ContentSent.Name,
	)

	// user actions queue
	amqp.InitQueue(
		svc.consumer.UserActions,
		svc.userActionsChan,
		processUserActions,
		sConf.Queue.UserActions.ThreadsCount,
		sConf.Queue.UserActions.Name,
		sConf.Queue.UserActions.Name,
	)

	// operator transactions queue
	amqp.InitQueue(
		svc.consumer.Operator,
		svc.operatorTransactionLogChan,
		operatorTransactions,
		sConf.Queue.TransactionLog.ThreadsCount,
		sConf.Queue.TransactionLog.Name,
		sConf.Queue.TransactionLog.Name,
	)
	// combined mt manager queue
	amqp.InitQueue(
		svc.consumer.MTManager,
		svc.mtManagerChan,
		processMTManagerTasks,
		sConf.Queue.MTManager.ThreadsCount,
		sConf.Queue.MTManager.Name,
		sConf.Queue.MTManager.Name,
	)

	// combined mt manager queue
	amqp.InitQueue(
		svc.consumer.Pixels,
		svc.pixelsChan,
		processPixels,
		sConf.Queue.PixelSent.ThreadsCount,
		sConf.Queue.PixelSent.Name,
		sConf.Queue.PixelSent.Name,
	)
}

type Service struct {
	db                         *sql.DB
	consumer                   Consumers
	contentSentChan            <-chan amqp_driver.Delivery
	accessCampaignChan         <-chan amqp_driver.Delivery
	userActionsChan            <-chan amqp_driver.Delivery
	operatorTransactionLogChan <-chan amqp_driver.Delivery
	mtManagerChan              <-chan amqp_driver.Delivery
	pixelsChan                 <-chan amqp_driver.Delivery
	ipDb                       *geoip2.Reader
	uaparser                   *uaparser.Parser
	sConfig                    ServiceConfig
	dbConf                     db.DataBaseConfig
	tables                     map[string]struct{}
	m                          Metrics
}
type Consumers struct {
	Access      *amqp.Consumer
	UserActions *amqp.Consumer
	ContentSent *amqp.Consumer
	Operator    *amqp.Consumer
	MTManager   *amqp.Consumer
	Pixels      *amqp.Consumer
}
type QueuesConfig struct {
	AccessCampaign config.ConsumeQueueConfig `yaml:"access_campaign"`
	ContentSent    config.ConsumeQueueConfig `yaml:"content_sent"`
	UserActions    config.ConsumeQueueConfig `yaml:"user_actions"`
	TransactionLog config.ConsumeQueueConfig `yaml:"transaction_log"`
	MTManager      config.ConsumeQueueConfig `yaml:"mt_manager"`
	PixelSent      config.ConsumeQueueConfig `yaml:"pixel_sent"`
}

type ServiceConfig struct {
	GeoIpPath           string       `yaml:"geoip_path" default:"dev/GeoLite2-City.mmdb"`
	UAParserRegexesPath string       `default:"/home/centos/linkit/regexes.yaml" yaml:"ua_parser_regexes_path"`
	Queue               QueuesConfig `yaml:"queues"`
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
