package service

import (
	"database/sql"
	"encoding/json"

	"github.com/oschwald/geoip2-golang"
	log "github.com/sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"
	"github.com/ua-parser/uap-go/uaparser"

	mid_client "github.com/linkit360/go-mid/rpcclient"
	mid "github.com/linkit360/go-mid/service"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/config"
	"github.com/linkit360/go-utils/db"
)

var svc Service

type Service struct {
	db                         *sql.DB
	n                          *amqp.Notifier
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
	m                          Metrics
}

type ServiceConfig struct {
	GeoIpPath              string       `yaml:"geoip_path" default:"dev/GeoLite2-City.mmdb"`
	UAParserRegexesPath    string       `default:"/home/centos/linkit/regexes.yaml" yaml:"ua_parser_regexes_path"`
	PixelBufferTimoutHours int          `yaml:"pixel_buffer_timeout_hours" default:"24"`
	Queue                  QueuesConfig `yaml:"queues"`
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
	Hit            string                    `yaml:"reporter_hit"`
	Pixel          string                    `yaml:"reporter_pixel"`
	Transaction    string                    `yaml:"reporter_transaction"`
	Outflow        string                    `yaml:"reporter_outflow"`
}

func InitService(
	name string,
	sConf ServiceConfig,
	midConfig mid_client.ClientConfig,
	notifierConfig amqp.NotifierConfig,
	dbConf db.DataBaseConfig,
	consumerConf amqp.ConsumerConfig,
) {
	log.SetLevel(log.DebugLevel)
	appName = name

	mid_client.Init(midConfig)
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

	svc.n = amqp.NewNotifier(notifierConfig)

	svc.m = newMetrics(appName)

	svc.consumer = Consumers{
		Access:      amqp.InitConsumer(consumerConf, sConf.Queue.AccessCampaign, svc.accessCampaignChan, processAccessCampaign),
		UserActions: amqp.InitConsumer(consumerConf, sConf.Queue.UserActions, svc.userActionsChan, processUserActions),
		ContentSent: amqp.InitConsumer(consumerConf, sConf.Queue.ContentSent, svc.contentSentChan, processContentSent),
		UniqueUrl:   amqp.InitConsumer(consumerConf, sConf.Queue.UniqueUrls, svc.uniqueUrlsChan, processUniqueUrls),
		Operator:    amqp.InitConsumer(consumerConf, sConf.Queue.TransactionLog, svc.operatorTransactionLogChan, operatorTransactions),
		MTManager:   amqp.InitConsumer(consumerConf, sConf.Queue.MTManager, svc.mtManagerChan, processMTManagerTasks),
		Pixels:      amqp.InitConsumer(consumerConf, sConf.Queue.PixelSent, svc.pixelsChan, processPixels),
		Redirects:   amqp.InitConsumer(consumerConf, sConf.Queue.Redirects, svc.redirectsChan, processRedirects),
	}
}

func publishReporter(queue string, c mid.Collect) (err error) {
	event := amqp.EventNotify{
		EventName: "ee",
		EventData: c,
	}
	var body []byte
	body, err = json.Marshal(event)

	if err != nil {
		return
	}
	svc.n.Publish(amqp.AMQPMessage{
		QueueName: queue,
		Body:      body,
	})
	return nil
}
