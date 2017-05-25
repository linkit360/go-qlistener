package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

type OperatorTransactionLog struct {
	Tid              string    `json:"tid,omitempty"`
	Msisdn           string    `json:"msisdn,omitempty"`
	OperatorToken    string    `json:"token,omitempty"`
	OperatorTime     time.Time `json:"operator_time,omitempty"`
	OperatorCode     int64     `json:"operator_code,omitempty"`
	CountryCode      int64     `json:"country_code,omitempty"`
	ServiceCode      string    `json:"service_code,omitempty"`
	CampaignCode     string    `json:"campaign_code,omitempty"`
	SubscriptionId   int64     `json:"id_subscription,omitempty"`
	Error            string    `json:"error,omitempty"`
	Price            int       `json:"price,omitempty"`
	RequestBody      string    `json:"request_body,omitempty"`
	ResponseBody     string    `json:"response_body,omitempty"`
	ResponseDecision string    `json:"response_decision,omitempty"`
	ResponseCode     int       `json:"response_code,omitempty"`
	SentAt           time.Time `json:"sent_at,omitempty"`
	Notice           string    `json:"notice,omitempty"`
	Type             string    `json:"type,omitempty"`
}

type EventNotifyOperatorTransaction struct {
	EventName string                 `json:"event_name,omitempty"`
	EventData OperatorTransactionLog `json:"event_data,omitempty"`
}

func operatorTransactions(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		var begin time.Time
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.TransactionLog.Name,
		})
		var query string
		var t OperatorTransactionLog

		var e EventNotifyOperatorTransaction
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.Operator.Dropped.Inc()
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("failed")
			goto ack
		}
		t = e.EventData

		if t.Tid == "" {
			logCtx.Warn("no tid")
		} else {
			logCtx = logCtx.WithFields(log.Fields{
				"tid": t.Tid,
			})
		}
		if t.RequestBody == "" {
			logCtx.Warn("no request body")
		}
		if t.ResponseBody == "" {
			logCtx.Warn("no response body")
		}
		if t.RequestBody == "" && t.ResponseBody == "" {
			svc.m.Operator.Dropped.Inc()
			svc.m.Operator.Empty.Inc()

			logCtx.WithField("dropped", true).
				Error("no response body and no request body")
			goto ack
		}
		if t.OperatorToken == "" {
			logCtx.Warn("no operator token")
		}
		if t.OperatorCode == 0 {
			logCtx.Warn("no operator code")
		}
		if t.CountryCode == 0 {
			logCtx.Warn("no country code")
		}
		if t.Price == 0 {
			logCtx.Warn("no price")
		}
		if t.ServiceCode == "" {
			t.ServiceCode = "0"
			logCtx.Warn("no service code")
		}
		if t.CampaignCode == "" {
			t.CampaignCode = "0"
			logCtx.Warn("no campaign code")
		}
		if t.SubscriptionId == 0 {
			logCtx.Warn("no subscription id")
		}
		if t.ResponseCode == 0 {
			logCtx.Warn("no response code")
		}

		// todo: add check for every field
		if len(t.Msisdn) > 32 {
			logCtx.WithFields(log.Fields{
				"error": "too long",
			}).Error("strange msisdn")
			t.Msisdn = t.Msisdn[:31]
		}
		if t.Type == "" {
			logCtx.Warn("no transaction type")
			t.Type = "charge"
		}

		t.Notice = strings.Replace(t.Notice, "0x00", "", -1)

		begin = time.Now()
		query = fmt.Sprintf("INSERT INTO %soperator_transaction_log ("+
			"tid, "+
			"msisdn, "+
			"operator_code, "+
			"country_code, "+
			"operator_token, "+
			"operator_time, "+
			"error, "+
			"price, "+
			"id_service, "+
			"id_subscription, "+
			"id_campaign, "+
			"request_body, "+
			"response_body, "+
			"response_decision, "+
			"response_code,  "+
			"sent_at, "+
			"notice, "+
			"type "+
			")"+
			" values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "+
			"$11, $12, $13, $14, $15, $16, $17, $18)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Tid,
			t.Msisdn,
			t.OperatorCode,
			t.CountryCode,
			t.OperatorToken,
			t.OperatorTime,
			t.Error,
			t.Price,
			t.ServiceCode,
			t.SubscriptionId,
			t.CampaignCode,
			t.RequestBody,
			t.ResponseBody,
			t.ResponseDecision,
			t.ResponseCode,
			t.SentAt,
			t.Notice,
			t.Type,
		); err != nil {
			svc.m.Common.DBErrors.Inc()
			svc.m.Operator.AddToDBErrors.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "requeue",
				"query": query,
				"t":     fmt.Sprintf("%#v", t),
			}).Error("failed")
		nack:
			if err := msg.Nack(false, true); err != nil {
				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("cannot nack")
				time.Sleep(time.Second)
				goto nack
			}
			continue
		}

		svc.m.Operator.AddToDbSuccess.Inc()
		svc.m.Operator.AddToDBDuration.Observe(time.Since(begin).Seconds())
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
