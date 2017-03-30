package service

import (
	"database/sql"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/oschwald/geoip2-golang"
	amqp_driver "github.com/streadway/amqp"
	"github.com/ua-parser/uap-go/uaparser"

	inmem_client "github.com/linkit360/go-inmem/rpcclient"
	reporter_client "github.com/linkit360/go-reporter/rpcclient"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/config"
	"github.com/linkit360/go-utils/db"
)

var svc Service

type Service struct {
	db                         *sql.DB
	consumer                   Consumers
	contentSentChan            <-chan amqp_driver.Delivery
	uniqueUrlsChan             <-chan amqp_driver.Delivery
	accessCampaignChan         <-chan amqp_driver.Delivery
	userActionsChan            <-chan amqp_driver.Delivery
	operatorTransactionLogChan <-chan amqp_driver.Delivery
	mtManagerChan              <-chan amqp_driver.Delivery
	pixelsChan                 <-chan amqp_driver.Delivery
	redirectsChan              <-chan amqp_driver.Delivery
	ipDb                       *geoip2.Reader
	uaparser                   *uaparser.Parser
	sConfig                    ServiceConfig
	dbConf                     db.DataBaseConfig
	tables                     map[string]struct{}
	m                          Metrics
}

type ServiceConfig struct {
	GeoIpPath           string       `yaml:"geoip_path" default:"dev/GeoLite2-City.mmdb"`
	UAParserRegexesPath string       `default:"/home/centos/linkit/regexes.yaml" yaml:"ua_parser_regexes_path"`
	Queue               QueuesConfig `yaml:"queues"`
}

type Consumers struct {
	Access      *amqp.Consumer
	UserActions *amqp.Consumer
	ContentSent *amqp.Consumer
	UniqueUrl   *amqp.Consumer
	Operator    *amqp.Consumer
	MTManager   *amqp.Consumer
	Pixels      *amqp.Consumer
	Redirects   *amqp.Consumer
}

type QueuesConfig struct {
	AccessCampaign config.ConsumeQueueConfig `yaml:"access_campaign"`
	ContentSent    config.ConsumeQueueConfig `yaml:"content_sent"`
	UniqueUrls     config.ConsumeQueueConfig `yaml:"unique_urls"`
	UserActions    config.ConsumeQueueConfig `yaml:"user_actions"`
	TransactionLog config.ConsumeQueueConfig `yaml:"transaction_log"`
	MTManager      config.ConsumeQueueConfig `yaml:"mt_manager"`
	PixelSent      config.ConsumeQueueConfig `yaml:"pixel_sent"`
	Redirects      config.ConsumeQueueConfig `yaml:"redirect"`
}

func InitService(
	name string,
	sConf ServiceConfig,
	inMemConfig inmem_client.ClientConfig,
	reporterConfig reporter_client.ClientConfig,
	dbConf db.DataBaseConfig,
	consumerConf amqp.ConsumerConfig,
) {
	log.SetLevel(log.DebugLevel)
	appName = name

	inmem_client.Init(inMemConfig)
	svc.db = db.Init(dbConf)
	svc.sConfig = sConf
	svc.dbConf = dbConf

	if err := reporter_client.Init(reporterConfig); err != nil && reporterConfig.Enabled {
		log.Error(fmt.Errorf("reporter_client.Init: %s", err.Error()))
	}

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

	svc.m = newMetrics(appName)

	svc.consumer = Consumers{
		Access:      initConsumer(consumerConf, sConf.Queue.AccessCampaign, svc.accessCampaignChan, processAccessCampaign),
		UserActions: initConsumer(consumerConf, sConf.Queue.UserActions, svc.userActionsChan, processUserActions),
		ContentSent: initConsumer(consumerConf, sConf.Queue.ContentSent, svc.contentSentChan, processContentSent),
		UniqueUrl:   initConsumer(consumerConf, sConf.Queue.UniqueUrls, svc.uniqueUrlsChan, processUniqueUrls),
		Operator:    initConsumer(consumerConf, sConf.Queue.TransactionLog, svc.operatorTransactionLogChan, operatorTransactions),
		MTManager:   initConsumer(consumerConf, sConf.Queue.MTManager, svc.mtManagerChan, processMTManagerTasks),
		Pixels:      initConsumer(consumerConf, sConf.Queue.PixelSent, svc.pixelsChan, processPixels),
		Redirects:   initConsumer(consumerConf, sConf.Queue.Redirects, svc.redirectsChan, processRedirects),
	}
}

func initConsumer(
	consumerConf amqp.ConsumerConfig,
	queueConf config.ConsumeQueueConfig,
	readChan <-chan amqp_driver.Delivery,
	fn func(<-chan amqp_driver.Delivery),
) *amqp.Consumer {
	if !queueConf.Enabled {
		log.Infof("rbmq consumer disabled: %s ", queueConf.Name)
		return nil
	}

	consumer := amqp.NewConsumer(consumerConf, queueConf.Name, queueConf.PrefetchCount)
	if err := consumer.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}

	amqp.InitQueue(
		consumer,
		readChan,
		fn,
		queueConf.ThreadsCount,
		queueConf.Name,
		queueConf.Name,
	)
	return consumer
}
