package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/vostrok/dispatcherd/src/rbmq"
	"time"
)

type EventNotifyUserActions struct {
	EventName string                 `json:"event_name,omitempty"`
	EventData rbmq.UserActionsNotify `json:"event_data,omitempty"`
}

func processUserActions(deliveries <-chan amqp.Delivery) {

	for msg := range deliveries {
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.UserActions.Name,
		})

		logCtx.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var query string
		var e EventNotifyUserActions
		var t rbmq.UserActionsNotify
		var begin time.Time

		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.UserActions.Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("consume")
			goto ack
		}

		t = e.EventData
		if t.Tid == "" || t.Action == "" {
			svc.m.UserActions.Dropped.Inc()
			svc.m.UserActions.Empty.Inc()

			logCtx.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("no tid or no action, strange row, discarding")
			goto ack
		}
		logCtx = logCtx.WithFields(log.Fields{
			"tid": t.Tid,
		})
		if len(t.Msisdn) > 32 {
			logCtx.WithFields(log.Fields{
				"msisdn": t.Msisdn,
				"error":  "too long",
			}).Error("strange msisdn, truncating")
			t.Msisdn = t.Msisdn[:31]
		}
		begin = time.Now()
		query = fmt.Sprintf("INSERT INTO %suser_actions ("+
			"sent_at, "+
			"id_campaign, "+
			"msisdn, "+
			"tid, "+
			"action, "+
			"error "+
			") values ($1, $2, $3, $4, $5, $6)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.SentAt,
			t.CampaignId,
			t.Msisdn,
			t.Tid,
			t.Action,
			t.Error,
		); err != nil {
			svc.m.DBErrors.Inc()
			svc.m.UserActions.AddToDBErrors.Inc()

			logCtx.WithFields(log.Fields{
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("add user action failed")
			msg.Nack(false, true)
			continue
		}

		svc.m.UserActions.AddToDbSuccess.Inc()
		svc.m.UserActions.AddToDBDuration.Observe(time.Since(begin).Seconds())
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
