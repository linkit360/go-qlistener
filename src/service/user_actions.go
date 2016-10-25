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
		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyUserActions
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume user action ")
			msg.Ack(false)
			continue
		}

		t := e.EventData
		query := fmt.Sprintf("INSERT INTO %suser_actions ("+
			"tid, "+
			"action, "+
			"error "+
			") values ($1, $2, $3)",
			svc.sConfig.DbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Tid,
			t.Action,
			t.Error,
		); err != nil {
			log.WithFields(log.Fields{
				"userAction": t,
				"msg":        "requeue",
				"error":      err.Error(),
			}).Error("add user action")
			msg.Nack(false, true)
			continue
		}
		log.WithFields(log.Fields{
			"userAction": t,
		}).Info("processed successfully")
		msg.Ack(false)
	}
}