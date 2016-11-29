package service

import (
	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	rec "github.com/vostrok/utils/rec"
)

type EventNotifyRec struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func processMTManagerTasks(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyRec
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.MTManager.Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"rec":   string(msg.Body),
			}).Error("consume mt_manager")
			msg.Ack(false)
			continue
		}
		t := e.EventData

		if t.Msisdn == "" ||
			t.CampaignId == 0 ||
			t.SubscriptionId == 0 {
			svc.m.MTManager.Dropped.Inc()
			svc.m.MTManager.Empty.Inc()

			log.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"rec":   string(msg.Body),
			}).Error("consume mt_manager")
			msg.Ack(false)
			continue
		}

		var err error
		switch e.EventName {
		case "StartRetry":
			err = t.StartRetry()
		case "AddBlacklistedNumber":
			err = t.AddBlacklistedNumber()
		case "AddPostPaidNumber":
			err = t.AddPostPaidNumber()
		case "TouchRetry":
			err = t.TouchRetry()
		case "RemoveRetry":
			err = t.RemoveRetry()
		case "WriteSubscriptionStatus":
			err = t.WriteSubscriptionStatus()
		case "WriteTransaction":
			err = t.WriteTransaction()
		default:
			svc.m.MTManager.Dropped.Inc()

			log.WithFields(log.Fields{
				"event": e.EventName,
				"msg":   "dropped",
				"rec":   string(msg.Body),
			}).Error("consume mt_manager: unknown event")
			msg.Ack(false)
			continue
		}
		if err != nil {
			svc.m.MTManager.AddToDBErrors.Inc()
			log.WithFields(log.Fields{
				"event": e.EventName,
				"error": err.Error(),
				"rec":   string(msg.Body),
			}).Error("consume mt_manager")
			msg.Nack(false, true)
			continue
		}
		svc.m.MTManager.AddToDbSuccess.Inc()

		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"queue": "content_sent",
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
