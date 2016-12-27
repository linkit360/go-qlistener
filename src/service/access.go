package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/ua-parser/uap-go/uaparser"
	"github.com/vostrok/dispatcherd/src/rbmq"
	inmem_client "github.com/vostrok/inmem/rpcclient"
)

type EventNotifyAccessCampaign struct {
	EventName string                    `json:"event_name,omitempty"`
	EventData rbmq.AccessCampaignNotify `json:"event_data,omitempty"`
}

func processAccessCampaign(deliveries <-chan amqp.Delivery) {

	for msg := range deliveries {
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.AccessCampaign.Name,
		})
		logCtx.WithField("body", string(msg.Body)).Debug("start process")
		var ipInfo IpInfo
		var err error
		var os string
		var device string
		var browser string
		var begin time.Time
		var query string
		var ua *uaparser.Client
		var e EventNotifyAccessCampaign
		var t rbmq.AccessCampaignNotify

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
			"tid":    t.Tid,
			"msisdn": t.Msisdn,
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
		if t.UrlPath == "" && t.Tid == "" && t.CampaignHash == "" {
			svc.m.AccessCampaign.Dropped.Inc()
			svc.m.AccessCampaign.Empty.Inc()
			logCtx.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
			}).Error("no urlpath, strange row, discarding")
			goto ack
		}
		if t.CampaignId == 0 {
			if t.CampaignHash != "" {
				camp, err := inmem_client.GetCampaignByHash(t.CampaignHash)
				if err != nil {
					svc.m.AccessCampaign.UnknownHash.Inc()

					err := fmt.Errorf("GetCampaignByHash: %s", err.Error())
					logCtx.WithField("errror", err.Error()).Error("cannot get campaign by hash")
				} else {
					t.CampaignId = camp.Id
					t.ServiceId = camp.ServiceId
				}
			} else {
				logCtx.Error("campaign hash and id empty")
			}
		}

		ipInfo, err = geoIp(t.IP)
		if err != nil {
			svc.m.AccessCampaign.ErrorsParseGeoIp.Inc()
			logCtx.WithFields(log.Fields{
				"IP":       t.IP,
				"parseErr": err.Error(),
			}).Debug("parse geo ip city, continued..")
		}
		err = nil

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
			"id_content, "+
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
			" $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)",
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
			t.CampaignId,
			t.ServiceId,
			t.ContentId,
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
			svc.m.DBErrors.Inc()
			svc.m.AccessCampaign.AddToDBErrors.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "requeue",
				"query": query,
			}).Error("add access campaign failed")
		nack:
			if err := msg.Nack(false, true); err != nil {
				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("cannot nack")
				time.Sleep(time.Second)
				goto nack
			}
			continue
		}
		svc.m.AccessCampaign.AddToDbSuccess.Inc()
		svc.m.AccessCampaign.AddToDbDuration.Observe(time.Since(begin).Seconds())
		svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())

		logCtx.WithFields(log.Fields{
			"took": time.Since(begin).String(),
		}).Info("success")

	ack:
		if err := msg.Ack(false); err != nil {
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
