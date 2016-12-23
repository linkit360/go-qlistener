package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/vostrok/pixels/src/notifier"
)

type EventNotifyPixel struct {
	EventName string         `json:"event_name,omitempty"`
	EventData notifier.Pixel `json:"event_data,omitempty"`
}

func processPixels(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		var t notifier.Pixel
		var begin time.Time

		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyPixel
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.Pixels.Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("consume pixel")

			goto ack
		}
		t = e.EventData

		begin = time.Now()
		switch e.EventName {
		case "transaction":
			query := fmt.Sprintf("INSERT INTO %spixel_transactions ( "+
				"sent_at, "+
				"tid, "+
				"msisdn, "+
				"pixel, "+
				"endpoint, "+
				"id_campaign, "+
				"operator_code, "+
				"country_code, "+
				"publisher, "+
				"response_code "+
				") VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
				svc.dbConf.TablePrefix)

			begin := time.Now()
			if _, err := svc.db.Exec(query,
				t.SentAt,
				t.Tid,
				t.Msisdn,
				t.Pixel,
				t.Endpoint,
				t.CampaignId,
				t.OperatorCode,
				t.CountryCode,
				t.Publisher,
				t.ResponseCode,
			); err != nil {
				svc.m.DBErrors.Inc()
				svc.m.Pixels.AddToDBErrors.Inc()

				log.WithFields(log.Fields{
					"tid":   t.Tid,
					"pixel": t.Pixel,
					"query": query,
					"error": err.Error(),
					"msg":   "dropped",
				}).Error("record pixel transaction failed")
			nackTransaction:
				if err := msg.Nack(false, true); err != nil {
					log.WithFields(log.Fields{
						"tid":   e.EventData.Tid,
						"error": err.Error(),
					}).Error("cannot nack")
					time.Sleep(time.Second)
					goto nackTransaction
				}
				continue
			} else {
				log.WithFields(log.Fields{
					"tid":   t.Tid,
					"pixel": t.Pixel,
					"took":  time.Since(begin),
				}).Info("add pixel: success")
			}

		case "update":
			query := fmt.Sprintf("UPDATE %ssubscriptions SET "+
				" publisher = $1,  "+
				" pixel_sent = $2,  "+
				" pixel_sent_at = $3  "+
				" WHERE id = $4 ",
				svc.dbConf.TablePrefix)

			begin := time.Now()
			if _, err := svc.db.Exec(query,
				t.Publisher,
				t.Sent,
				time.Now(),
				t.SubscriptionId,
			); err != nil {
				svc.m.DBErrors.Inc()
				svc.m.Pixels.UpdateSubscriptionToDBErrors.Inc()

				log.WithFields(log.Fields{
					"tid":   t.Tid,
					"pixel": t.Pixel,
					"query": query,
					"error": err.Error(),
				}).Error("update subscription pixel sent")

			nackSubscriptions:
				if err := msg.Nack(false, true); err != nil {
					log.WithFields(log.Fields{
						"tid":   e.EventData.Tid,
						"error": err.Error(),
					}).Error("cannot nack")
					time.Sleep(time.Second)
					goto nackSubscriptions
				}
				continue
			} else {
				log.WithFields(log.Fields{
					"tid":   t.Tid,
					"pixel": t.Pixel,
					"took":  time.Since(begin),
				}).Info("update subscrption: success")
			}
		default:
			svc.m.MTManager.Dropped.Inc()

			log.WithFields(log.Fields{
				"event": e.EventName,
				"msg":   "dropped",
				"rec":   string(msg.Body),
			}).Error("consume mt_manager: unknown event")
			goto ack
		}

		svc.m.Pixels.AddToDBDuration.Observe(time.Since(begin).Seconds())
		svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
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
