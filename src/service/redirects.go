package service

import (
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	redirect_service "github.com/linkit360/go-partners/service"
)

type EventNotifyRedirects struct {
	EventName string                          `json:"event_name,omitempty"`
	EventData redirect_service.DestinationHit `json:"event_data,omitempty"`
}

func processRedirects(deliveries <-chan amqp.Delivery) {

	for msg := range deliveries {
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.Redirects.Name,
		})
		var query string
		var e EventNotifyRedirects
		var t redirect_service.DestinationHit
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
		if t.Tid == "" {
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
		query = "INSERT INTO tr.destinations_hits (" +
			"id_partner, " +
			"id_destination, " +
			"tid, " +
			"sent_at, " +
			"destination, " +
			"msisdn , " +
			"price_per_hit, " +
			"operator_code," +
			"country_code" +
			") values ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

		if _, err := svc.db.Exec(query,
			t.PartnerId,
			t.DestinationId,
			t.Tid,
			t.SentAt,
			t.Destination,
			t.Msisdn,
			t.PricePerHit,
			t.OperatorCode,
			t.CountryCode,
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
			svc.m.Common.Errors.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
