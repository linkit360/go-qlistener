package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/oschwald/geoip2-golang"
	amqp_driver "github.com/streadway/amqp"

	"github.com/vostrok/db"
	"github.com/vostrok/rabbit"
)

var svc Service

const ACTIVE_STATUS = 1

func InitService(sConf ServiceConfig) {
	log.SetLevel(log.DebugLevel)

	var err error

	svc.db = db.Init(sConf.DbConf)
	svc.sConfig = sConf
	svc.ipDb, err = geoip2.Open(sConf.GeoIpPath)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("geoip init")
	}

	svc.m = Metrics{
		AccessCampaign: initAccessCampaignMetrics(),
		ContentSent:    initContentSentMetrics(),
		UserActions:    initUserActionsMetrics(),
	}

	svc.consumer = rabbit.NewConsumer(sConf.Consumer)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq connect: ", err.Error())
	}

	// access campaign consumer
	svc.accessCampaign, err = svc.consumer.AnnounceQueue(
		sConf.Queue.AccessCampaign, sConf.Queue.AccessCampaign)
	if err != nil {
		log.WithFields(log.Fields{
			"queue": sConf.Queue.AccessCampaign,
			"error": err.Error(),
		}).Fatal("rbmq consumer: AnnounceQueue")
	}
	go svc.consumer.Handle(svc.accessCampaign, accessCampaign, sConf.ThreadsCount,
		sConf.Queue.AccessCampaign, sConf.Queue.AccessCampaign)

	// content sent consumer
	svc.contentSent, err = svc.consumer.AnnounceQueue(
		sConf.Queue.ContentSent, sConf.Queue.ContentSent)
	if err != nil {

		log.WithFields(log.Fields{
			"queue": sConf.Queue.ContentSent,
			"error": err.Error(),
		}).Fatal("rbmq consumer: AnnounceQueue")
	}
	go svc.consumer.Handle(svc.contentSent, contentSent, sConf.ThreadsCount,
		sConf.Queue.ContentSent, sConf.Queue.ContentSent)

	// user actions consumer
	svc.userActions, err = svc.consumer.AnnounceQueue(
		sConf.Queue.UserActions, sConf.Queue.UserActions)
	if err != nil {
		log.WithFields(log.Fields{
			"queue": sConf.Queue.UserActions,
			"error": err.Error(),
		}).Fatal("rbmq consumer: AnnounceQueue")
	}
	go svc.consumer.Handle(svc.userActions, userActions, sConf.ThreadsCount,
		sConf.Queue.UserActions, sConf.Queue.UserActions)

	// CQR-s
	if err := initCQR(); err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("cqr init")
	}
}

type Metrics struct {
	AccessCampaign AccessCampaignMetrics
	ContentSent    ContentSentMetrics
	UserActions    UserActionsMetrics
}

type Service struct {
	db             *sql.DB
	consumer       *rabbit.Consumer
	contentSent    <-chan amqp_driver.Delivery
	accessCampaign <-chan amqp_driver.Delivery
	userActions    <-chan amqp_driver.Delivery
	ipDb           *geoip2.Reader
	sConfig        ServiceConfig
	tables         map[string]struct{}
	m              Metrics
}
type QueuesConfig struct {
	AccessCampaign string `default:"access_campaign" yaml:"access_campaign"`
	ContentSent    string `default:"content_sent" yaml:"content_sent"`
	UserActions    string `default:"user_actions" yaml:"user_actions"`
}
type ServiceConfig struct {
	GeoIpPath             string                `yaml:"geoip_path" default:"dev/GeoLite2-City.mmdb"`
	ThreadsCount          int                   `default:"1" yaml:"threads_count"`
	Consumer              rabbit.ConsumerConfig `yaml:"consumer"`
	Queue                 QueuesConfig          `yaml:"queues"`
	DbConf                db.DataBaseConfig     `yaml:"db"`
	Tables                []string              `yaml:"tables"`
	SubscriptionsLoadDays int                   `default:"10" yaml:"subscriptions_load_days"`
}

func initCQR() error {
	if err := memCampaign.Reload(); err != nil {
		return fmt.Errorf("memCampaign.Reload: %s", err.Error())
	}
	svc.tables = make(map[string]struct{}, len(svc.sConfig.Tables))
	for _, v := range svc.sConfig.Tables {
		svc.tables[v] = struct{}{}
	}
	return nil
}

type response struct {
	Success bool        `json:"success,omitempty"`
	Err     error       `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Status  int         `json:"-"`
}

func AddCQRHandlers(r *gin.Engine) {
	rg := r.Group("/cqr")
	rg.GET("", Reload)
}

func render(msg response, c *gin.Context) {
	if msg.Err != nil {
		c.Header("Error", msg.Err.Error())
		c.Error(msg.Err)
	}
	c.JSON(msg.Status, msg)
}

func Reload(c *gin.Context) {
	var err error
	r := response{Err: err, Status: http.StatusOK}

	table, exists := c.GetQuery("table")
	if !exists || table == "" {
		table, exists = c.GetQuery("t")
		if !exists || table == "" {
			err := errors.New("Table name required")
			r.Status = http.StatusBadRequest
			r.Err = err
			render(r, c)
			return
		}
	}
	r.Success, r.Err = CQR(table)
	render(r, c)
	return
}
func CQR(table string) (bool, error) {
	if len(table) == 0 {
		log.WithFields(log.Fields{
			"error": "No table name given",
		}).Errorf("CQR request")
		return false, nil
	}
	_, ok := svc.tables[table]
	if !ok {
		log.WithFields(log.Fields{
			"error": "table name doesn't match any",
		}).Errorf("CQR request")
		return false, nil
	}
	// should we re-build service
	switch {
	case strings.Contains(table, "campaigns"):
		if err := memCampaign.Reload(); err != nil {
			return false, fmt.Errorf("memCampaign.Reload: %s", err.Error())
		}

	default:
		return false, fmt.Errorf("CQR Request: Unknown table: %s", table)
	}

	return true, nil
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
