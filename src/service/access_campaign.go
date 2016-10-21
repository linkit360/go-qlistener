package service

import (
	"fmt"
	"time"

	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/vostrok/dispatcherd/src/rbmq"
)

func accessCampaign(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		msg.Ack(false)
		var t rbmq.AccessCampaignNotify

		if err := json.Unmarshal(msg.Body, &t); err != nil {
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"contentsent": string(msg.Body)}).
				Error("consume access campaign")
			msg.Ack(true)
			continue
		}
		ipInfo, err := geoIp(t.IP)
		if err != nil {
			log.WithFields(log.Fields{
				"error":           err.Error(),
				"accessCcampaign": t,
			}).Error("parse geo ip city")
		}

		query := fmt.Sprintf("INSERT INTO %scampaigns_access ("+
			"msisdn, "+
			"tid, "+
			"ip, "+
			"operator_code"+
			"country_code, "+
			"supported, "+
			"user_agent, "+
			"referer, "+
			"url_path, "+
			"method, "+
			"headers, "+
			"content_error, "+
			"file_error, "+
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
			" $16, $17, $18, $19, $20, $21, $22, $23, $24, $35, $36, $37, $38 )",
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
			t.ContentServiceError,
			t.ContentFileError,
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
			log.WithFields(log.Fields{
				"access_log": t,
				"error":      err.Error()}).
				Error("add access campaign log failed")
			//not handled, back to rbmq
			//svc.recordContentGiven <- t
			//svc.recordContentGiven <- t
			time.Sleep(time.Second)
		} else {
			msg.Ack(true)
		}
	}
}
