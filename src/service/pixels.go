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
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.PixelSent.Name,
		})
		var e EventNotifyPixel
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.Pixels.Dropped.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("failed")

			goto ack
		}
		t = e.EventData
		logCtx = logCtx.WithFields(log.Fields{
			"tid":   t.Tid,
			"event": e.EventName,
		})

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
				svc.m.Common.DBErrors.Inc()
				svc.m.Pixels.AddToDBErrors.Inc()

				logCtx.WithFields(log.Fields{
					"query": query,
					"error": err.Error(),
					"msg":   "dropped",
				}).Error("failed")
			nackTransaction:
				if err := msg.Nack(false, true); err != nil {
					logCtx.WithFields(log.Fields{
						"error": err.Error(),
					}).Error("cannot nack")
					time.Sleep(time.Second)
					goto nackTransaction
				}
				continue
			} else {
				svc.m.Pixels.AddToDbSuccess.Inc()
				svc.m.Pixels.AddToDBDuration.Observe(time.Since(begin).Seconds())
				logCtx.WithFields(log.Fields{
					"took": time.Since(begin),
				}).Info("success")
			}

		case "update":
			query := fmt.Sprintf("UPDATE %ssubscriptions SET "+
				" pixel = $1,  "+
				" publisher = $2,  "+
				" pixel_sent = $3,  "+
				" pixel_sent_at = $4  "+
				" WHERE id = $5 ",
				svc.dbConf.TablePrefix)

			begin := time.Now()
			if _, err := svc.db.Exec(query,
				t.Pixel,
				t.Publisher,
				t.Sent,
				time.Now(),
				t.SubscriptionId,
			); err != nil {
				svc.m.Common.DBErrors.Inc()
				svc.m.Pixels.UpdateSubscriptionToDBErrors.Inc()

				logCtx.WithFields(log.Fields{
					"query": query,
					"error": err.Error(),
				}).Error("failed")

				msg.Nack(false, true)
				continue
			} else {
				svc.m.Pixels.UpdateDBDuration.Observe(time.Since(begin).Seconds())
				logCtx.WithFields(log.Fields{
					"took": time.Since(begin),
				}).Info("success")
			}
		case "buffer":
			query := fmt.Sprintf("INSERT INTO %spixel_buffer ( "+
				"sent_at, "+
				"id_campaign, "+
				"tid, "+
				"pixel "+
				") VALUES ( $1, $2, $3, $4)",
				svc.dbConf.TablePrefix)

			begin := time.Now()
			if _, err := svc.db.Exec(query,
				t.SentAt,
				t.CampaignId,
				t.Tid,
				t.Pixel,
			); err != nil {
				svc.m.Common.DBErrors.Inc()
				svc.m.Pixels.BufferAddToDBErrors.Inc()

				logCtx.WithFields(log.Fields{
					"query": query,
					"error": err.Error(),
					"msg":   "dropped",
				}).Error("failed")
			nackBuffer:
				if err := msg.Nack(false, true); err != nil {
					logCtx.WithFields(log.Fields{
						"event": e.EventName,
						"error": err.Error(),
					}).Error("cannot nack")
					time.Sleep(time.Second)
					goto nackBuffer
				}
				continue
			} else {
				svc.m.Pixels.BufferAddToDBDuration.Observe(time.Since(begin).Seconds())
				svc.m.Pixels.BufferAddToDbSuccess.Inc()
				logCtx.WithFields(log.Fields{
					"took": time.Since(begin),
				}).Info("success")
			}
		case "remove_buffered":
			query := fmt.Sprintf("delete from %spixel_buffer WHERE id_campaign = $1 AND pixel = $2 ",
				svc.dbConf.TablePrefix)

			begin := time.Now()
			if _, err := svc.db.Exec(query, t.CampaignId, t.Pixel); err != nil {
				svc.m.Common.DBErrors.Inc()

				logCtx.WithFields(log.Fields{
					"query": query,
					"error": err.Error(),
					"msg":   "dropped",
				}).Error("failed")
				msg.Nack(false, true)
				continue
			} else {
				logCtx.WithFields(log.Fields{
					"took": time.Since(begin),
				}).Info("success")
			}
		default:
			svc.m.Pixels.Dropped.Inc()

			logCtx.WithFields(log.Fields{
				"event": e.EventName,
				"msg":   "dropped",
			}).Error("unknown event")
			goto ack
		}
		svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
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
