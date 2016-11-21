package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/vostrok/contentd/service"
)

type EventNotifyContentSent struct {
	EventName string                        `json:"event_name,omitempty"`
	EventData service.ContentSentProperties `json:"event_data,omitempty"`
}

func processContentSent(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.ContentSent.Dropped.Inc()

			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			msg.Ack(false)
			continue
		}
		t := e.EventData

		if t.Msisdn == "" ||
			t.CampaignId == 0 ||
			t.ContentId == 0 {
			svc.m.ContentSent.Dropped.Inc()
			svc.m.ContentSent.Empty.Inc()

			log.WithFields(log.Fields{
				"error":       "Empty message",
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			msg.Ack(false)
			continue
		}
		// todo: add check for every field
		if len(t.Msisdn) > 32 {
			log.WithFields(log.Fields{
				"msisdn": t.Msisdn,
				"error":  "too long msisdn",
				"tid":    t.Tid,
			}).Error("strange msisdn, truncating")
			t.Msisdn = t.Msisdn[:31]
		}

		if t.SubscriptionId == 0 {
			log.WithFields(log.Fields{
				"tid": t.Tid,
			}).Debug("subscription id is empty")
		}

		query := fmt.Sprintf("INSERT INTO %scontent_sent ("+
			"msisdn, "+
			"tid, "+
			"id_campaign, "+
			"id_service, "+
			"id_subscription, "+
			"id_content, "+
			"country_code, "+
			"operator_code "+
			") values ($1, $2, $3, $4, $5, $6, $7, $8)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Msisdn,
			t.Tid,
			t.CampaignId,
			t.ServiceId,
			t.SubscriptionId,
			t.ContentId,
			t.CountryCode,
			t.OperatorCode,
		); err != nil {
			svc.m.DbErrors.Inc()
			svc.m.ContentSent.AddToDBErrors.Inc()

			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("add sent content")
			msg.Nack(false, true)
			continue
		}

		svc.m.ContentSent.AddToDbSuccess.Inc()

		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"queue": "content_sent",
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
