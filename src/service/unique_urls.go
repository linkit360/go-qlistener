package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/linkit360/go-utils/structs"
)

func processUniqueUrls(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.UniqueUrls.Name,
		})
		var begin time.Time
		var t structs.ContentSentProperties
		var query string

		var e structs.EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.UniqueUrls.Dropped.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("failed")
			goto ack
		}
		t = e.EventData
		logCtx = logCtx.WithFields(log.Fields{
			"tid": t.Tid,
		})

		if e.EventName == "create" {
			if t.CampaignCode == "" ||
				t.ServiceCode == "" {
				svc.m.UniqueUrls.Dropped.Inc()
				svc.m.UniqueUrls.Empty.Inc()

				logCtx.WithFields(log.Fields{
					"error": "Empty message",
					"msg":   "dropped",
					"body":  string(msg.Body),
				}).Error("failed")
				goto ack
			}
			// todo: add check for every field
			if len(t.Msisdn) > 32 {
				logCtx.WithFields(log.Fields{
					"msisdn": t.Msisdn,
					"error":  "too long msisdn",
				}).Error("strange msisdn")
				t.Msisdn = t.Msisdn[:31]
			}
			if t.CampaignCode == "" {
				t.CampaignCode = "0"
				logCtx.WithFields(log.Fields{}).Warn("no campaign")
			}
			if t.ServiceCode == "" {
				t.ServiceCode = "0"
				logCtx.WithFields(log.Fields{}).Warn("no service")
			}
			if t.ContentId == "" {
				t.ContentId = "0"
				logCtx.WithFields(log.Fields{}).Warn("no content")
			}

			begin = time.Now()
			query = fmt.Sprintf("INSERT INTO %scontent_unique_urls ("+
				"sent_at, "+
				"msisdn, "+
				"tid, "+
				"id_campaign, "+
				"id_service, "+
				"id_content, "+
				"id_subscription, "+
				"country_code, "+
				"operator_code, "+
				"content_path, "+
				"content_name, "+
				"unique_url "+
				") values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
				svc.dbConf.TablePrefix)

			if _, err := svc.db.Exec(query,
				t.SentAt,
				t.Msisdn,
				t.Tid,
				t.CampaignCode,
				t.ServiceCode,
				t.ContentId,
				t.SubscriptionId,
				t.CountryCode,
				t.OperatorCode,
				t.ContentPath,
				t.ContentName,
				t.UniqueUrl,
			); err != nil {
				svc.m.Common.DBErrors.Inc()
				svc.m.UniqueUrls.AddToDBErrors.Inc()

				logCtx.WithFields(log.Fields{
					"query": query,
					"msg":   "requeue",
					"error": err.Error(),
				}).Error("failed")
				time.Sleep(time.Second)
				msg.Nack(false, true)
				continue
			}
			svc.m.UniqueUrls.AddToDbSuccess.Inc()
			svc.m.UniqueUrls.AddToDBDuration.Observe(time.Since(begin).Seconds())
			svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
		}

		if e.EventName == "delete" {
			begin = time.Now()
			if t.UniqueUrl == "" {
				logCtx.WithFields(log.Fields{
					"error": "Empty message",
					"msg":   "dropped",
					"body":  string(msg.Body),
				}).Error("failed")
				goto ack
			}
			query = fmt.Sprintf("DELETE FROM %scontent_unique_urls WHERE unique_url = $1",
				svc.dbConf.TablePrefix)

			if _, err := svc.db.Exec(query, t.UniqueUrl); err != nil {
				svc.m.Common.DBErrors.Inc()
				svc.m.UniqueUrls.DeleteUniqUrlErrors.Inc()

				logCtx.WithFields(log.Fields{
					"query": query,
					"msg":   "requeue",
					"error": err.Error(),
				}).Error("failed")
				time.Sleep(time.Second)
				msg.Nack(false, true)
				continue
			}

			svc.m.UniqueUrls.DeleteUniqUrlSuccess.Inc()
			svc.m.UniqueUrls.DeleteFromDBDuration.Observe(time.Since(begin).Seconds())

			query = fmt.Sprintf("DELETE FROM %scontent_unique_urls "+
				"WHERE sent_at < (CURRENT_TIMESTAMP - %d * INTERVAL '1 day' )",
				svc.dbConf.TablePrefix,
				svc.sConfig.UniqueUrlsCleanupDays,
			)

			if _, err := svc.db.Exec(query); err != nil {
				svc.m.Common.DBErrors.Inc()
				svc.m.UniqueUrls.DeleteUniqUrlErrors.Inc()

				logCtx.WithFields(log.Fields{
					"query": query,
					"msg":   "requeue",
					"error": err.Error(),
				}).Error("failed")
				time.Sleep(time.Second)
				msg.Nack(false, true)
				continue
			}
		}

		logCtx.WithFields(log.Fields{
			"event": e.EventName,
			"took":  time.Since(begin).String(),
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
