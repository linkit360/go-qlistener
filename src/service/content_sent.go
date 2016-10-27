package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"github.com/streadway/amqp"

	"github.com/vostrok/contentd/service"
)

type ContentSentMetrics struct {
	Dropped                  metrics.Counter
	Empty                    metrics.Counter
	SubscriptionCreateErrors metrics.Counter
	SubscriptionCreateCount  metrics.Counter
	ContentSentCreateCount   metrics.Counter
	ContentSentCreateErrors  metrics.Counter
}

func initContentSentMetrics() ContentSentMetrics {
	return ContentSentMetrics{
		Dropped: expvar.NewCounter("dropped_content_sent"),
		Empty:   expvar.NewCounter("empty_content_sent"),
		SubscriptionCreateErrors: expvar.NewCounter("content_sent_subscription_create_count"),
		SubscriptionCreateCount:  expvar.NewCounter("content_sent_subscription_create_errors"),
		ContentSentCreateCount:   expvar.NewCounter("content_sent_create_count"),
		ContentSentCreateErrors:  expvar.NewCounter("content_sent_create_errors"),
	}
}

type EventNotifyContentSent struct {
	EventName string                        `json:"event_name,omitempty"`
	EventData service.ContentSentProperties `json:"event_data,omitempty"`
}

func contentSent(deliveries <-chan amqp.Delivery) {

	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.ContentSent.Dropped.Add(1)

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
			svc.m.ContentSent.Dropped.Add(1)
			svc.m.ContentSent.Empty.Add(1)

			log.WithFields(log.Fields{
				"error":       "Empty message",
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			msg.Ack(false)
			continue
		}

		if t.SubscriptionId == 0 {
			s := Subscription{Msisdn: t.Msisdn, ServiceId: t.ServiceId}
			var ok bool
			t.SubscriptionId, ok = memSubscriptions.Map[s.key()]
			if !ok {
				// do not set id_subscriber: msisdn is enough
				query := fmt.Sprintf("INSERT INTO %ssubscriptions ( "+
					"result, "+
					"id_campaign, "+
					"id_service, "+
					"msisdn, "+
					"tid, "+
					"country_code, "+
					"operator_code, "+
					"price "+
					") values ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id",
					svc.sConfig.DbConf.TablePrefix)

				if err := svc.db.QueryRow(query,
					"",
					t.CampaignId,
					t.ServiceId,
					t.Msisdn,
					t.Tid,
					t.CountryCode,
					t.OperatorCode,
					t.Price,
				).Scan(&t.SubscriptionId); err != nil {
					svc.m.ContentSent.SubscriptionCreateErrors.Add(1)
					log.WithFields(log.Fields{
						"error":       err.Error(),
						"query":       query,
						"msg":         "requeue",
						"contentSent": t,
					}).Error("add new subscription for sentcontent")
					msg.Nack(false, true)
					continue
				}
				svc.m.ContentSent.SubscriptionCreateCount.Add(1)
			}
		}
		if t.SubscriptionId == 0 {
			log.WithFields(log.Fields{
				"error":       "UNEXPECTED CODE REACHED",
				"contentSent": t,
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
			svc.sConfig.DbConf.TablePrefix)

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
			svc.m.ContentSent.ContentSentCreateErrors.Add(1)
			log.WithFields(log.Fields{
				"contentSent": t,
				"query":       query,
				"msg":         "requeue",
				"error":       err.Error(),
			}).Error("add sent content")
			msg.Nack(false, true)
			continue
		}

		svc.m.ContentSent.ContentSentCreateCount.Add(1)
		log.WithFields(log.Fields{
			"contentSent": t,
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
