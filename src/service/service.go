package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

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

	svc.consumer = rabbit.NewConsumer(sConf.Consumer)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq connect: %s", err.Error())
	}

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

	if err := initCQR(); err != nil {
		log.WithField("error", err.Error()).Fatal("Init CQR")
	}
	svc.ipDb, err = geoip2.Open(sConf.GeoIpPath)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("Init GeoIp")
	}
}

type Service struct {
	db             *sql.DB
	consumer       *rabbit.Consumer
	contentSent    <-chan amqp_driver.Delivery
	accessCampaign <-chan amqp_driver.Delivery
	ipDb           *geoip2.Reader
	sConfig        ServiceConfig
	tables         map[string]struct{}
}
type QueuesConfig struct {
	AccessCampaign string `default:"access_campaign" yaml:"access_campaign"`
	ContentSent    string `default:"content_sent" yaml:"content_sent"`
}
type ServiceConfig struct {
	GeoIpPath    string                `yaml:"geoip_path" default:"dev/GeoLite2-City.mmdb"`
	ThreadsCount int                   `default:"1" yaml:"threads_count"`
	Consumer     rabbit.ConsumerConfig `yaml:"consumer"`
	Queue        QueuesConfig          `yaml:"queues"`
	DbConf       db.DataBaseConfig     `yaml:"db"`
	Tables       []string              `default:"subscriptions" yaml:"tables"`
}

func initCQR() error {
	if err := subscriptions.Reload(); err != nil {
		return fmt.Errorf("subscriptions.Reload: %s", err.Error())
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
		log.WithField("error", "No table name given").Errorf("CQR request")
		return false, nil
	}
	_, ok := svc.tables[table]
	if !ok {
		log.WithField("error", "table name doesn't match any").Errorf("CQR request")
		return false, nil
	}
	// should we re-build service
	switch {
	case strings.Contains(table, "subscriptions"):
		if err := subscriptions.Reload(); err != nil {
			return false, fmt.Errorf("subscriptions.Reload: %s", err.Error())
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
	record, err := svc.ipDb.City(net.ParseIP(ip))
	if err != nil {
		return IpInfo{}, fmt.Errorf("GeoIP Parse City: %s", err.Error())
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

// msisdn _ service_id ==> subscription_id

// Keep in memory all active campaigns
// Allow to get a subscription_id by msisdn and service_id
// Reload when changes to subscriptions made
// usage:
// subscripions_id, ok := subscripions.Map[ s.key() ]
var subscriptions = &Subscriptions{}

type Subscriptions struct {
	sync.RWMutex
	Map map[string]int64
}
type Subscription struct {
	Msisdn         string
	ServiceId      int64
	SubscriptionId int64
}

func (s Subscription) key() string {
	return fmt.Sprintf("%s-%d", s.Msisdn, s.ServiceId)
}
func (s *Subscriptions) Reload() error {
	query := fmt.Sprintf("select id, msisdn, id_service from "+
		"%ssubscriptions where status = $1", svc.sConfig.DbConf.TablePrefix)
	rows, err := svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("Subscriptions Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var records []Subscription
	for rows.Next() {
		record := Subscription{}

		if err := rows.Scan(
			&record.SubscriptionId,
			&record.Msisdn,
			&record.ServiceId,
		); err != nil {
			return err
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return fmt.Errorf("Subscriptions Reload RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]int64, len(records))
	for _, subscription := range records {
		s.Map[subscription.key()] = subscription.SubscriptionId
	}
	return nil
}
