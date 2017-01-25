package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	inmem_service "github.com/vostrok/inmem/service"
)

type EventNotifyContentSent struct {
	EventName string                              `json:"event_name,omitempty"`
	EventData inmem_service.ContentSentProperties `json:"event_data,omitempty"`
}

func processContentSent(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.ContentSent.Name,
		})
		var begin time.Time
		var t inmem_service.ContentSentProperties
		var query string

		var e EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.ContentSent.Dropped.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("failed")
			goto ack
		}
		t = e.EventData
		logCtx = logCtx.WithFields(log.Fields{
			"tid": t.Tid,
		})
		if t.CampaignId == 0 ||
			t.ServiceId == 0 {
			svc.m.ContentSent.Dropped.Inc()
			svc.m.ContentSent.Empty.Inc()

			logCtx.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("failed")
			goto ack
		}
		// todo: add check for every field
		if len(t.Msisdn) > 32 {
			logCtx.WithFields(log.Fields{
				"msisdn": t.Msisdn,
				"error":  "too long msisdn",
			}).Error("strange msisdn")
			t.Msisdn = t.Msisdn[:31]
		}

		begin = time.Now()
		query = fmt.Sprintf("INSERT INTO %scontent_sent ("+
			"sent_at, "+
			"msisdn, "+
			"tid, "+
			"id_campaign, "+
			"id_service, "+
			"id_content, "+
			"id_subscription, "+
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
			t.ContentId,
			t.SubscriptionId,
			t.CountryCode,
			t.OperatorCode,
		); err != nil {
			svc.m.DBErrors.Inc()
			svc.m.ContentSent.AddToDBErrors.Inc()

			logCtx.WithFields(log.Fields{
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("failed")
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

		svc.m.ContentSent.AddToDbSuccess.Inc()
		svc.m.ContentSent.AddToDBDuration.Observe(time.Since(begin).Seconds())
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
