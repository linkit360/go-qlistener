package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/vostrok/contentd/service"
)

func contentSent(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {

		var t service.ContentSentProperties
		if err := json.Unmarshal(msg.Body, &t); err != nil {
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			msg.Ack(false)
			continue
		}

		if t.SubscriptionId == 0 {
			s := Subscription{Msisdn: t.Msisdn, ServiceId: t.ServiceId}
			var ok bool
			t.SubscriptionId, ok = subscriptions.Map[s.key()]
			if !ok {
				// do not set id_subscriber: msisdn is enough
				query := fmt.Sprintf("INSERT INTO %ssubscriptions ( "+
					"status, "+
					"id_campaign, "+
					"id_service, "+
					"msisdn, "+
					"country_code, "+
					"operator_code) "+
					" values ($1, $2, $3, $4, $5, $6) RETURNING id",
					svc.sConfig.DbConf.TablePrefix)

				if err := svc.db.QueryRow(query,
					";",
					t.CampaignId,
					t.ServiceId,
					t.Msisdn,
					t.CountryCode,
					t.OperatorCode,
				).Scan(&t.SubscriptionId); err != nil {
					log.WithFields(log.Fields{
						"error":       err.Error(),
						"msg":         "requeue",
						"contentSent": t,
					}).Error("add new subscription for sentcontent")
					msg.Nack(false, true)
					continue
				}
			}
		}
		if t.SubscriptionId == 0 {
			log.WithFields(log.Fields{
				"error":       "UNEXPECTED CODE REACHED",
				"contentSent": t,
			}).Error("add content sent")
		}

		query := fmt.Sprintf("INSERT INTO %scontent_sent ("+
			"msisdn, "+
			"id_campaign, "+
			"id_service, "+
			"id_subscription, "+
			"id_content, "+
			"country_code, "+
			"operator_code "+
			") values ($1, $2, $3, $4, $5, $6, $7)",
			svc.sConfig.DbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Msisdn,
			t.CampaignId,
			t.ServiceId,
			t.SubscriptionId,
			t.ContentId,
			t.CountryCode,
			t.OperatorCode,
		); err != nil {
			log.WithFields(log.Fields{
				"contentSent": t,
				"msg":         "requeue",
				"error":       err.Error(),
			}).Error("add sent content")
			msg.Nack(false, true)
			continue
		}
		log.WithFields(log.Fields{
			"contentSent": t,
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
