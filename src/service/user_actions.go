package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

type UserActionNotify struct {
	Tid    string `json:"tid"`
	Error  string `json:"error"`
	Action string `json:"tid"`
}

func userActions(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {

		var t UserActionNotify
		if err := json.Unmarshal(msg.Body, &t); err != nil {
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume user action ")
			msg.Ack(false)
			continue
		}

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
