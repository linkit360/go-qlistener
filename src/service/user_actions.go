package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/vostrok/dispatcherd/src/rbmq"
)

type EventNotifyUserActions struct {
	EventName string                 `json:"event_name,omitempty"`
	EventData rbmq.UserActionsNotify `json:"event_data,omitempty"`
}

func userActions(deliveries <-chan amqp.Delivery) {

	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyUserActions
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.UserActions.Dropped.Inc()

			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume user action ")
			msg.Ack(false)
			continue
		}

		t := e.EventData
		if t.Tid == "" || t.Action == "" {
			svc.m.UserActions.Dropped.Inc()
			svc.m.UserActions.Empty.Inc()

			log.WithFields(log.Fields{
				"error":      "Empty message",
				"msg":        "dropped",
				"userAction": string(msg.Body),
			}).Error("no tid or no action, strange row, discarding")
			msg.Ack(false)
			continue
		}

		query := fmt.Sprintf("INSERT INTO %suser_actions ("+
			"tid, "+
			"action, "+
			"error "+
			") values ($1, $2, $3)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Tid,
			t.Action,
			t.Error,
		); err != nil {
			svc.m.UserActions.CreateDBErrors.Inc()

			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("add user action")
			msg.Nack(false, true)
			continue
		}

		svc.m.UserActions.CreateCount.Inc()
		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"queue": "user_actions",
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
