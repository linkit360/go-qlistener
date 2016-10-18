package service

import (
	"database/sql"
	"fmt"
	"net"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/oschwald/geoip2-golang"

	"github.com/vostrok/db"
	"github.com/vostrok/rabbit"
)

var svc Service

const ACTIVE_STATUS = 1

func InitService(sConf ServiceConfig) {
	svc.db = db.Init(sConf.DbConf)
	svc.sConfig = sConf

	sConf.RBMQ.Metrics = rabbit.InitMetrics()
	svc.accessCampaign = rabbit.NewConsumer(sConf.Queue.AccessCampaign, sConf.RBMQ)
	svc.contentSent = rabbit.NewConsumer(sConf.Queue.ContentSent, sConf.RBMQ)
	if err := initCQR(); err != nil {
		log.WithField("error", err.Error()).Fatal("Init CQR")
	}
	var err error
	svc.ipDb, err = geoip2.Open(sConf.GeoIpPath)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("Init GeoIp")
	}
	go func() {
		contentSent()
	}()
}

type Service struct {
	db             *sql.DB
	accessCampaign rabbit.AMQPService
	contentSent    rabbit.AMQPService
	ipDb           *geoip2.Reader
	sConfig        ServiceConfig
	tables         map[string]struct{}
}
type QueuesConfig struct {
	AccessCampaign string `default:"access_campaign" yaml:"access_campaign"`
	ContentSent    string `default:"content_sent" yaml:"content_sent"`
}
type ServiceConfig struct {
	RBMQ        rabbit.RBMQConfig `yaml:"rabbit"`
	Queue       QueuesConfig      `yaml:"queues"`
	GeoIpPath   string            `yaml:"geoip_path" default:"dev/GeoLite2-City.mmdb"`
	DbConf      db.DataBaseConfig `yaml:"db"`
	TablePrefix string            `default:"xmp_" yaml:"table_prefix"`
	Tables      []string          `default:"subscriptions" yaml:"tables"`
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
	switch table {
	case "subscriptions":
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
func (s Subscriptions) Reload() error {
	query := fmt.Sprintf("select id, msisdn, id_service from "+
		"%ssubscriptions where status = $1", svc.sConfig.TablePrefix)
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
