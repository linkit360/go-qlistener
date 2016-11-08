package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

type OperatorTransactionLog struct {
	Tid            string `json:"tid"`
	Msisdn         string `json:"msisdn"`
	OperatorToken  string `json:"token"`
	OperatorCode   int64  `json:"operator_code"`
	CountryCode    int64  `json:"country_code"`
	Error          string `json:"error"`
	Price          int    `json:"price"`
	ServiceId      int64  `json:"id_service"`
	SubscriptionId int64  `json:"id_subscription"`
	CampaignId     int64  `json:"id_campaign"`
	RequestBody    string `json:"request_body"`
	ResponseBody   string `json:"response_body"`
	ResponseCode   int    `json:"response_code"`
}

type EventNotifyOperatorTransaction struct {
	EventName string                 `json:"event_name,omitempty"`
	EventData OperatorTransactionLog `json:"event_data,omitempty"`
}

func operatorTransactions(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {

		log.WithField("body", string(msg.Body)).Debug("start process operator transaction")

		var e EventNotifyOperatorTransaction
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			log.WithFields(log.Fields{
				"error":                err.Error(),
				"operator_transaction": string(msg.Body),
				"msg": "dropped",
			}).Error("consume operator transaction")
			msg.Ack(false)
			continue
		}
		t := e.EventData

		logCtx := log.WithField("operator_transaction", t)
		if t.RequestBody == "" {
			logCtx.Error("no request body")
		}
		if t.ResponseBody == "" {
			logCtx.Error("no response body")
		}
		if t.RequestBody == "" || t.ResponseBody == "" {
			msg.Ack(false)
			continue
		}
		if t.Tid == "" {
			logCtx.Error("no tid")
		}
		if t.OperatorToken == "" {
			logCtx.Error("no operator token")
		}
		if t.OperatorCode == 0 {
			logCtx.Error("no operator code")
		}

		if t.CountryCode == 0 {
			logCtx.Error("no country code")
		}

		if t.Price == 0 {
			logCtx.Error("no price")
		}

		if t.ServiceId == 0 {
			logCtx.Error("no service id")
		}

		if t.SubscriptionId == 0 {
			logCtx.Error("no subscription id")
		}

		if t.CampaignId == 0 {
			logCtx.Error("no campaign id")
		}

		if t.ResponseCode == 0 {
			logCtx.Error("no response code")
		}

		query := fmt.Sprintf("INSERT INTO %soperator_transaction_log ("+
			"tid, "+
			"msisdn, "+
			"operator_code, "+
			"country_code, "+
			"operator_token, "+
			"error, "+
			"price, "+
			"id_service, "+
			"id_subscription, "+
			"id_campaign, "+
			"request_body, "+
			"response_body, "+
			"response_code "+
			")"+
			" values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
			svc.dbConf.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Tid,
			t.Msisdn,
			t.OperatorCode,
			t.CountryCode,
			t.OperatorToken,
			t.Error,
			t.Price,
			t.ServiceId,
			t.SubscriptionId,
			t.CampaignId,
			t.RequestBody,
			t.ResponseBody,
			t.ResponseCode,
		); err != nil {
			logCtx.WithFields(log.Fields{
				"tid":   t.Tid,
				"error": err.Error(),
				"msg":   "requeue",
				"query": query,
			}).Error("add operator transaction failed")
			msg.Nack(false, true)
			continue
		}
		logCtx.WithFields(log.Fields{
			"tid":   t.Tid,
			"queue": "operator_transactions",
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
