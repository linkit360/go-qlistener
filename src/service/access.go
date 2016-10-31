package service

import (
	"fmt"

	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"github.com/streadway/amqp"

	"github.com/vostrok/dispatcherd/src/rbmq"
)

type AccessCampaignMetrics struct {
	Dropped                      metrics.Counter
	Empty                        metrics.Counter
	UnknownCampaignHash          metrics.Counter
	ErrorsParseGeoIp             metrics.Counter
	AccessCampaignCreateCount    metrics.Counter
	AccessCampaignCreateDBErrors metrics.Counter
}

func initAccessCampaignMetrics() AccessCampaignMetrics {
	return AccessCampaignMetrics{
		Dropped:                      expvar.NewCounter("dropped_access_campaign"),
		Empty:                        expvar.NewCounter("empty_access_campaign"),
		UnknownCampaignHash:          expvar.NewCounter("access_campaign_unknown_hash"),
		ErrorsParseGeoIp:             expvar.NewCounter("access_campaign_parse_geoip_errors"),
		AccessCampaignCreateCount:    expvar.NewCounter("access_campaign_create_count"),
		AccessCampaignCreateDBErrors: expvar.NewCounter("access_campaign_create_db_errors"),
	}
}

type EventNotifyAccessCampaign struct {
	EventName string                    `json:"event_name,omitempty"`
	EventData rbmq.AccessCampaignNotify `json:"event_data,omitempty"`
}

func accessCampaign(deliveries <-chan amqp.Delivery) {

	for msg := range deliveries {

		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyAccessCampaign
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.AccessCampaign.Dropped.Add(1)
			log.WithFields(log.Fields{
				"error":          err.Error(),
				"accessCampaign": string(msg.Body),
				"msg":            "dropped",
			}).Error("consume access campaign")
			msg.Ack(false)
			continue
		}
		t := e.EventData

		logCtx := log.WithField("accessCampaign", t)
		if t.CampaignHash == "" {
			logCtx.Error("no campaign hash")
		}
		if t.Tid == "" {
			logCtx.Error("no tid")
		}
		if t.UrlPath == "" && t.Tid == "" && t.CampaignHash == "" {
			svc.m.AccessCampaign.Dropped.Add(1)
			svc.m.AccessCampaign.Empty.Add(1)
			logCtx.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"tid":   t.Tid,
			}).Error("no urlpath, strange row, discarding")
			msg.Ack(false)
			continue
		}
		if t.CampaignId == 0 {
			camp, ok := memCampaign.Map[t.CampaignHash]
			if !ok {
				svc.m.AccessCampaign.UnknownCampaignHash.Add(1)
				logCtx.Error("unknown campaign hash")
			} else {
				t.CampaignId = camp.Id
				t.ServiceId = camp.ServiceId
			}
		}

		ipInfo, err := geoIp(t.IP)
		if err != nil {
			svc.m.AccessCampaign.ErrorsParseGeoIp.Add(1)
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"error": err.Error(),
			}).Error("parse geo ip city, continued..")
		}
		err = nil

		query := fmt.Sprintf("INSERT INTO %scampaigns_access ("+
			"msisdn, "+
			"tid, "+
			"ip, "+
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
			" $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)",
			svc.sConfig.DbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Msisdn,
			t.Tid,
			t.IP,
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
			svc.m.AccessCampaign.AccessCampaignCreateDBErrors.Add(1)
			logCtx.WithFields(log.Fields{
				"tid":   t.Tid,
				"error": err.Error(),
				"msg":   "requeue",
				"query": query,
			}).Error("add access campaign log failed")
			msg.Nack(false, true)
			continue
		}
		svc.m.AccessCampaign.AccessCampaignCreateCount.Add(1)
		logCtx.WithFields(log.Fields{
			"tid": t.Tid,
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
