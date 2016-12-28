package service

import (
	"encoding/json"
	"fmt"
	"time"

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
		logCtx := log.WithFields(log.Fields{
			"q": svc.sConfig.Queue.MTManager.Name,
		})
		var err error
		var t rec.Record
		var e EventNotifyRec

		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.MTManager.Dropped.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"rec":   string(msg.Body),
			}).Error("failed")
			goto ack
		}
		t = e.EventData

		if t.Msisdn == "" ||
			t.CampaignId == 0 ||
			t.SubscriptionId == 0 {
			svc.m.MTManager.Dropped.Inc()
			svc.m.MTManager.Empty.Inc()

			logCtx.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"rec":   string(msg.Body),
			}).Error("failed")
			goto ack
		}
		logCtx = logCtx.WithFields(log.Fields{
			"tid": t.Tid,
		})
		switch e.EventName {
		case "StartRetry":
			err = startRetry(t)
		case "AddBlacklistedNumber":
			err = addBlacklistedNumber(t)
		case "AddPostPaidNumber":
			err = addPostPaidNumber(t)
		case "TouchRetry":
			err = touchRetry(t)
		case "RemoveRetry":
			err = removeRetry(t)
		case "WriteSubscriptionStatus":
			err = writeSubscriptionStatus(t)
		case "WriteTransaction":
			err = writeTransaction(t)
		default:
			svc.m.MTManager.Dropped.Inc()

			logCtx.WithFields(log.Fields{
				"event": e.EventName,
				"msg":   "dropped",
				"rec":   string(msg.Body),
			}).Error("unknown event")
			goto ack
		}

		if err != nil {
			svc.m.DBErrors.Inc()
			svc.m.MTManager.AddToDBErrors.Inc()

			logCtx.WithFields(log.Fields{
				"event": e.EventName,
				"error": err.Error(),
			}).Error("failed")
			msg.Nack(false, true)
			continue
		} else {
			svc.m.MTManager.AddToDbSuccess.Inc()
		}

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

func writeTransaction(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			log.WithFields(fields).Error("write transaction")
		} else {
			log.WithFields(fields).Debug("write transaction")
		}

	}()
	query := fmt.Sprintf("INSERT INTO %stransactions ("+
		"tid, "+
		"sent_at, "+
		"msisdn, "+
		"result, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign, "+
		"operator_token, "+
		"price "+
		") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		svc.dbConf.TablePrefix,
	)
	if _, err = svc.db.Exec(
		query,
		r.Tid,
		r.SentAt,
		r.Msisdn,
		r.Result,
		r.OperatorCode,
		r.CountryCode,
		r.ServiceId,
		r.SubscriptionId,
		r.CampaignId,
		r.OperatorToken,
		int(r.Price),
	); err != nil {
		err = fmt.Errorf("db.Exec: %s, Query: %s", err.Error(), query)
		return
	}

	svc.m.MTManager.WriteTransactionDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func writeSubscriptionStatus(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		log.WithFields(fields).Debug("write subscription status")
	}()
	query := fmt.Sprintf("UPDATE %ssubscriptions SET "+
		"result = $1, "+
		"attempts_count = attempts_count + 1, "+
		"last_pay_attempt_at = $2 "+
		"where id = $3",
		svc.dbConf.TablePrefix,
	)

	lastPayAttemptAt := r.SentAt
	_, err = svc.db.Exec(query,
		r.SubscriptionStatus,
		lastPayAttemptAt,
		r.SubscriptionId,
	)
	if err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	svc.m.MTManager.WriteSubscriptionStatusDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func removeRetry(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":    r.Tid,
			"result": r.Result,
			"took":   time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			log.WithFields(fields).Error("remove retry")
		} else {
			log.WithFields(fields).Debug("remove retry")
		}
	}()
	query := fmt.Sprintf(`INSERT INTO
	%sretries_expired(
		  status,
		  tid ,
		  created_at ,
		  last_pay_attempt_at ,
		  attempts_count ,
		  keep_days ,
		  delay_hours ,
		  msisdn ,
		  operator_code ,
		  country_code ,
		  id_service ,
		  id_subscription ,
		  id_campaign
	)
  	SELECT
		  status,
		  tid ,
		  created_at ,
		  last_pay_attempt_at ,
		  attempts_count ,
		  keep_days ,
		  delay_hours ,
		  msisdn ,
		  operator_code ,
		  country_code ,
		  id_service ,
		  id_subscription ,
		  id_campaign
  	FROM %sretries WHERE id = $1`,
		svc.dbConf.TablePrefix,
		svc.dbConf.TablePrefix,
	)
	if _, err = svc.db.Exec(query, r.RetryId); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}
	query = fmt.Sprintf("DELETE FROM %sretries WHERE id = $1", svc.dbConf.TablePrefix)

	if _, err = svc.db.Exec(query, r.RetryId); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}
	svc.m.MTManager.RemoveRetryDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func touchRetry(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		log.WithFields(fields).Debug("touch retry")
	}()

	lastPayAttemptAt := r.SentAt
	query := fmt.Sprintf("UPDATE %sretries SET "+
		"status = '', "+
		"last_pay_attempt_at = $1, "+
		"attempts_count = attempts_count + 1 "+
		"WHERE id = $2",
		svc.dbConf.TablePrefix,
	)
	if _, err = svc.db.Exec(query, lastPayAttemptAt, r.RetryId); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	svc.m.MTManager.TouchRetryDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func startRetry(r rec.Record) (err error) {

	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
			log.WithFields(fields).Error("add retry")
		} else {
			log.WithFields(fields).Debug("add retry")
		}
	}()
	if r.KeepDays == 0 {
		err = fmt.Errorf("Retry Keep Days required, service id: %s", r.ServiceId)
		return
	}
	if r.DelayHours == 0 {
		err = fmt.Errorf("Retry Delay Hours required, service id: %s", r.ServiceId)
		return
	}
	query := fmt.Sprintf("INSERT INTO  %sretries ("+
		"tid, "+
		"keep_days, "+
		"delay_hours, "+
		"msisdn, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign, "+
		"price "+
		") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		svc.dbConf.TablePrefix,
	)
	if _, err = svc.db.Exec(query,
		&r.Tid,
		&r.KeepDays,
		&r.DelayHours,
		&r.Msisdn,
		&r.OperatorCode,
		&r.CountryCode,
		&r.ServiceId,
		&r.SubscriptionId,
		&r.CampaignId,
		&r.Price,
	); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	svc.m.MTManager.StartRetryDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func addBlacklistedNumber(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		log.WithFields(fields).Debug("add blacklisted")
	}()

	query := fmt.Sprintf("INSERT INTO  %smsisdn_blacklist ( msisdn ) VALUES ($1)", svc.dbConf.TablePrefix)

	if _, err = svc.db.Exec(query, &r.Msisdn); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	svc.m.MTManager.AddBlacklistedNumberDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func addPostPaidNumber(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		err = nil
		log.WithFields(fields).Debug("add postpaid")
	}()

	query := fmt.Sprintf("INSERT INTO %smsisdn_postpaid ( msisdn ) VALUES ($1)",
		svc.dbConf.TablePrefix,
	)
	if _, err = svc.db.Exec(query, &r.Msisdn); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	svc.m.MTManager.AddPostPaidNumberDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}
