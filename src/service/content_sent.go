package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"github.com/streadway/amqp"

	"github.com/vostrok/contentd/service"
	"time"
)

type ContentSentMetrics struct {
	Dropped                  metrics.Gauge
	Empty                    metrics.Gauge
	SubscriptionCreateErrors metrics.Gauge
	SubscriptionCreateCount  metrics.Gauge
	ContentSentCreateCount   metrics.Gauge
	ContentSentCreateErrors  metrics.Gauge
}

func initContentSentMetrics() ContentSentMetrics {
	return ContentSentMetrics{
		Dropped: expvar.NewGauge("dropped_content_sent"),
		Empty:   expvar.NewGauge("empty_content_sent"),
		SubscriptionCreateErrors: expvar.NewGauge("content_sent_subscription_create_count"),
		SubscriptionCreateCount:  expvar.NewGauge("content_sent_subscription_create_errors"),
		ContentSentCreateCount:   expvar.NewGauge("content_sent_create_count"),
		ContentSentCreateErrors:  expvar.NewGauge("content_sent_create_errors"),
	}
}

type EventNotifyContentSent struct {
	EventName string                        `json:"event_name,omitempty"`
	EventData service.ContentSentProperties `json:"event_data,omitempty"`
}

func contentSent(deliveries <-chan amqp.Delivery) {

	go func() {
		for range time.Tick(time.Second) {
			svc.m.ContentSent.Dropped.Set(0)
			svc.m.ContentSent.Empty.Set(0)
			svc.m.ContentSent.SubscriptionCreateErrors.Set(0)
			svc.m.ContentSent.SubscriptionCreateCount.Set(0)
			svc.m.ContentSent.ContentSentCreateCount.Set(0)
			svc.m.ContentSent.ContentSentCreateErrors.Set(0)
		}
	}()

	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			//svc.m.ContentSent.Dropped.Add(1)

			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			msg.Ack(false)
			continue
		}
		t := e.EventData

		if t.Msisdn == "" ||
			t.CampaignId == 0 ||
			t.ContentId == 0 {
			//svc.m.ContentSent.Dropped.Add(1)
			//svc.m.ContentSent.Empty.Add(1)

			log.WithFields(log.Fields{
				"error":       "Empty message",
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			msg.Ack(false)
			continue
		}
		// todo: add check for every field
		if len(t.Msisdn) > 32 {
			log.WithFields(log.Fields{
				"msisdn": t.Msisdn,
				"error":  "too long msisdn",
				"tid":    t.Tid,
			}).Error("strange msisdn, truncating")
			t.Msisdn = t.Msisdn[:31]
		}

		// do not set id_subscriber: msisdn is enough
		if t.SubscriptionId == 0 {
			query := fmt.Sprintf("INSERT INTO %ssubscriptions ( "+
				"result, "+
				"id_campaign, "+
				"id_service, "+
				"msisdn, "+
				"publisher, "+
				"pixel, "+
				"tid, "+
				"country_code, "+
				"operator_code, "+
				"paid_hours, "+
				"delay_hours, "+
				"price "+
				") values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) "+
				"RETURNING id",
				svc.dbConf.TablePrefix)

			if err := svc.db.QueryRow(query,
				"",
				t.CampaignId,
				t.ServiceId,
				t.Msisdn,
				t.Publisher,
				t.Pixel,
				t.Tid,
				t.CountryCode,
				t.OperatorCode,
				t.PaidHours,
				t.DelayHours,
				t.Price,
			).Scan(&t.SubscriptionId); err != nil {
				//svc.m.ContentSent.SubscriptionCreateErrors.Add(1)
				log.WithFields(log.Fields{
					"tid":   t.Tid,
					"error": err.Error(),
					"query": query,
					"msg":   "requeue",
				}).Error("add new subscription for sentcontent")
				msg.Nack(false, true)
				continue
			}
			log.WithFields(log.Fields{
				"tid": t.Tid,
			}).Info("added new subscription")
			//svc.m.ContentSent.SubscriptionCreateCount.Add(1)
		}

		if t.SubscriptionId == 0 {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"error": "UNEXPECTED CODE REACHED",
			}).Error("add content sent")
		}

		query := fmt.Sprintf("INSERT INTO %scontent_sent ("+
			"msisdn, "+
			"tid, "+
			"id_campaign, "+
			"id_service, "+
			"id_subscription, "+
			"id_content, "+
			"country_code, "+
			"operator_code "+
			") values ($1, $2, $3, $4, $5, $6, $7, $8)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Msisdn,
			t.Tid,
			t.CampaignId,
			t.ServiceId,
			t.SubscriptionId,
			t.ContentId,
			t.CountryCode,
			t.OperatorCode,
		); err != nil {
			//svc.m.ContentSent.ContentSentCreateErrors.Add(1)
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("add sent content")
			msg.Nack(false, true)
			continue
		}

		//svc.m.ContentSent.ContentSentCreateCount.Add(1)
		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"queue": "content_sent",
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
