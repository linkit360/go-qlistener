package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"github.com/streadway/amqp"

	"github.com/vostrok/dispatcherd/src/rbmq"
	"time"
)

type UserActionsMetrics struct {
	Dropped                   metrics.Gauge
	Empty                     metrics.Gauge
	UserActionsCreateCount    metrics.Gauge
	UserActionsCreateDBErrors metrics.Gauge
}

func initUserActionsMetrics() UserActionsMetrics {
	return UserActionsMetrics{
		Dropped: expvar.NewGauge("dropped_user_actions"),
		Empty:   expvar.NewGauge("empty_user_actions"),
		UserActionsCreateCount:    expvar.NewGauge("user_actions_count"),
		UserActionsCreateDBErrors: expvar.NewGauge("user_actions_db_errors"),
	}
}

type EventNotifyUserActions struct {
	EventName string                 `json:"event_name,omitempty"`
	EventData rbmq.UserActionsNotify `json:"event_data,omitempty"`
}

func userActions(deliveries <-chan amqp.Delivery) {

	go func() {
		for range time.Tick(time.Second) {
			svc.m.UserActions.Dropped.Set(0)
			svc.m.UserActions.Empty.Set(0)
			svc.m.UserActions.UserActionsCreateCount.Set(0)
			svc.m.UserActions.UserActionsCreateDBErrors.Set(0)
		}
	}()

	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyUserActions
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.UserActions.Dropped.Add(1)

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
			svc.m.UserActions.Dropped.Add(1)
			svc.m.UserActions.Empty.Add(1)

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
			svc.m.UserActions.UserActionsCreateDBErrors.Add(1)

			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("add user action")
			msg.Nack(false, true)
			continue
		}

		svc.m.UserActions.UserActionsCreateCount.Add(1)
		log.WithFields(log.Fields{
			"tid": t.Tid,
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
