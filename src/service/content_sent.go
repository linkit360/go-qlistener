package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/vostrok/contentd/service"
	"time"
)

type EventNotifyContentSent struct {
	EventName string                        `json:"event_name,omitempty"`
	EventData service.ContentSentProperties `json:"event_data,omitempty"`
}

func processContentSent(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")
		var begin time.Time
		var t service.ContentSentProperties
		var query string

		var e EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.ContentSent.Dropped.Inc()

			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			goto ack
		}
		t = e.EventData

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
			goto ack
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

		begin = time.Now()
		query = fmt.Sprintf("INSERT INTO %scontent_sent ("+
			"sent_at, "+
			"msisdn, "+
			"tid, "+
			"id_campaign, "+
			"id_service, "+
			"id_subscription, "+
			"id_content, "+
			"country_code, "+
			"operator_code "+
			") values ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.SentAt,
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
		nack:
			if err := msg.Nack(false, true); err != nil {
				log.WithFields(log.Fields{
					"tid":   e.EventData.Tid,
					"error": err.Error(),
				}).Error("cannot nack")
				time.Sleep(time.Second)
				goto nack
			}
			continue
		}

		svc.m.ContentSent.AddToDbSuccess.Inc()

		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"took":  time.Since(begin).String(),
			"queue": "content_sent",
		}).Info("success")
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"tid":   e.EventData.Tid,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
