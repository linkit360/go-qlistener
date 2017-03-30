package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/linkit360/go-dispatcherd/src/rbmq"
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
			}).Error("failed")
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
			}).Error("discarding")
			goto ack
		}
		logCtx = logCtx.WithFields(log.Fields{
			"tid": t.Tid,
		})
		if len(t.Msisdn) > 32 {
			logCtx.WithFields(log.Fields{
				"msisdn": t.Msisdn,
				"error":  "strange msisdn",
			}).Error("")
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
			svc.m.Common.DBErrors.Inc()
			svc.m.UserActions.AddToDBErrors.Inc()

			logCtx.WithFields(log.Fields{
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("failed")
			msg.Nack(false, true)
			continue
		}

		svc.m.UserActions.AddToDbSuccess.Inc()
		svc.m.UserActions.AddToDBDuration.Observe(time.Since(begin).Seconds())
		svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())

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
