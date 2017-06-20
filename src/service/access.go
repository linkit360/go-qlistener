package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/ua-parser/uap-go/uaparser"

	mid_client "github.com/linkit360/go-mid/rpcclient"
	mid "github.com/linkit360/go-mid/service"
	"github.com/linkit360/go-utils/structs"
)

type EventNotifyAccessCampaign struct {
	EventName string                       `json:"event_name,omitempty"`
	EventData structs.AccessCampaignNotify `json:"event_data,omitempty"`
}

func processAccessCampaign(deliveries <-chan amqp.Delivery) {

	for msg := range deliveries {
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.AccessCampaign.Name,
		})
		var ipInfo IpInfo
		var err error
		var os string
		var device string
		var browser string
		var begin time.Time
		var query string
		var IPs []string
		var ua *uaparser.Client
		var e EventNotifyAccessCampaign
		var t structs.AccessCampaignNotify

		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.AccessCampaign.Dropped.Inc()
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"body":  string(msg.Body),
				"msg":   "dropped",
			}).Error("failed")
			goto ack
		}
		t = e.EventData
		logCtx = logCtx.WithFields(log.Fields{
			"tid": t.Tid,
		})
		if t.CampaignHash == "" {
			logCtx.Error("no campaign hash")
		}
		if t.Tid == "" {
			logCtx.Error("no tid")
		}
		// todo: add check for every field
		if len(t.Msisdn) > 32 {
			logCtx.WithFields(log.Fields{
				"error": "msisdn is too long",
			}).Error("strange msisdn, truncating")
			t.Msisdn = t.Msisdn[:31]
		}
		if t.UrlPath == "" {
			logCtx.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
			}).Warn("no urlpath")
		}
		if t.Tid == "" {
			svc.m.AccessCampaign.Dropped.Inc()
			svc.m.AccessCampaign.Empty.Inc()
			logCtx.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
			}).Warn("no tid")
			goto ack
		}
		if t.CampaignHash == "" {
			logCtx.WithFields(log.Fields{
				"error": "no campaign hash",
				"msg":   "dropped",
			}).Warn("")
		}
		if t.CampaignCode == "" {
			if t.CampaignHash != "" {
				camp, err := mid_client.GetCampaignByHash(t.CampaignHash)
				if err != nil {
					svc.m.AccessCampaign.UnknownHash.Inc()

					err := fmt.Errorf("GetCampaignByHash: %s", err.Error())
					logCtx.WithField("errror", err.Error()).Error("cannot get campaign by hash")
				} else {
					t.CampaignCode = camp.Code
					t.ServiceCode = camp.ServiceCode
				}
			} else {
				logCtx.Error("campaign hash and id empty")
			}
		}
		if t.CampaignCode == "" {
			t.CampaignCode = "0"
		}
		if t.ServiceCode == "" {
			t.ServiceCode = "0"
		}
		if t.ContentCode == "" {
			t.ContentCode = "0"
		}

		IPs = strings.Split(t.IP, ", ")
		for _, ip := range IPs {
			ipInfo, err = geoIp(ip)
			if err == nil {
				break
			}
		}

		ua = svc.uaparser.Parse(t.UserAgent)
		os = ua.Os.ToString()
		device = ua.Device.ToString()
		browser = ua.UserAgent.ToString()

		if len(os) > 127 {
			logCtx.WithFields(log.Fields{
				"error": "os is too long",
				"os":    os,
			}).Error("truncating")
			os = os[:127]
		}
		if len(device) > 127 {
			logCtx.WithFields(log.Fields{
				"error":  "device is too long",
				"device": device,
			}).Error("truncating")
			device = device[:127]
		}
		if len(browser) > 127 {
			logCtx.WithFields(log.Fields{
				"error":   "browser is too long",
				"browser": browser,
			}).Error("truncating")
			browser = browser[:127]
		}
		if len(t.Referer) > 4091 {
			logCtx.WithFields(log.Fields{
				"error":   "referer is too long",
				"referer": t.Referer,
			}).Error("truncating")
			t.Referer = t.Referer[:4091]
		}
		if len(t.UserAgent) > 4091 {
			logCtx.WithFields(log.Fields{
				"error":     "UserAgent is too long",
				"UserAgent": t.UserAgent,
			}).Error("truncating")
			t.UserAgent = t.UserAgent[:4091]
		}
		if len(t.UrlPath) > 4091 {
			logCtx.WithFields(log.Fields{
				"error":   "UrlPath is too long",
				"UrlPath": t.UrlPath,
			}).Error("truncating")
			t.UrlPath = t.UrlPath[:4091]
		}
		if len(t.Headers) > 4091 {
			logCtx.WithFields(log.Fields{
				"error":   "Headers is too long",
				"Headers": t.Headers,
			}).Error("truncating")
			t.Headers = t.Headers[:4091]
		}
		begin = time.Now()
		query = fmt.Sprintf("INSERT INTO %scampaigns_access ("+
			"sent_at, "+
			"msisdn, "+
			"tid, "+
			"ip, "+
			"os, "+
			"device, "+
			"browser, "+
			"operator_code, "+
			"country_code, "+
			"supported, "+
			"user_agent, "+
			"referer, "+
			"url_path, "+
			"method, "+
			"headers, "+
			"error, "+
			"id_campaign, "+
			"id_service, "+
			"geoip_country, "+
			"geoip_iso, "+
			"geoip_city, "+
			"geoip_timezone, "+
			"geoip_latitude, "+
			"geoip_longitude, "+
			"geoip_metro_code, "+
			"geoip_postal_code, "+
			"geoip_subdivisions, "+
			"geoip_is_anonymous_proxy, "+
			"geoip_is_satellite_provider, "+
			"geoip_accuracy_radius "+
			")"+
			" values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,  "+
			" $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.SentAt,
			t.Msisdn,
			t.Tid,
			t.IP,
			os,
			device,
			browser,
			t.OperatorCode,
			t.CountryCode,
			t.Supported,
			t.UserAgent,
			t.Referer,
			t.UrlPath,
			t.Method,
			t.Headers,
			t.Error,
			t.CampaignCode,
			t.ServiceCode,
			ipInfo.Country,
			ipInfo.Iso,
			ipInfo.City,
			ipInfo.Timezone,
			ipInfo.Latitude,
			ipInfo.Latitude,
			ipInfo.MetroCode,
			ipInfo.PostalCode,
			ipInfo.Subdivisions,
			ipInfo.IsAnonymousProxy,
			ipInfo.IsSatelliteProvider,
			ipInfo.AccuracyRadius,
		); err != nil {
			svc.m.Common.DBErrors.Inc()
			svc.m.AccessCampaign.AddToDBErrors.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "requeue",
				"body":  string(msg.Body),
				"query": query,
			}).Error("failed")

			time.Sleep(time.Second)
			msg.Nack(false, true)
			continue
		}

		svc.m.AccessCampaign.AddToDbSuccess.Inc()
		svc.m.AccessCampaign.AddToDbDuration.Observe(time.Since(begin).Seconds())
		svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())

		logCtx.WithFields(log.Fields{
			"took": time.Since(begin).String(),
		}).Info("success")
		publishReporter(svc.sConfig.Queue.Hit, mid.Collect{
			Tid:          t.Tid,
			CampaignCode: t.CampaignCode,
			OperatorCode: t.OperatorCode,
			Msisdn:       t.Msisdn,
		})
	ack:
		if err := msg.Ack(false); err != nil {
			svc.m.Common.Errors.Inc()
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
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
