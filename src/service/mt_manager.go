package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	mid "github.com/linkit360/go-mid/service"
	rec "github.com/linkit360/go-utils/rec"
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
		logCtx = logCtx.WithFields(log.Fields{
			"e": e.EventName,
		})

		if (e.EventName != "Unsubscribe" && e.EventName != " UnsubscribeAll") &&
			(t.Msisdn == "" || t.ServiceCode == "") {
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
		case "Unsubscribe":
			err = unsubscribe(t)
		case "UnsubscribeAll":
			err = unsubscribeAll(t)
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
		case "WriteSubscriptionPeriodic":
			err = writeSubscriptionPeriodic(t)
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
			svc.m.Common.DBErrors.Inc()
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
			log.WithFields(fields).Error("write transaction error")
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
		r.ServiceCode,
		r.SubscriptionId,
		r.CampaignCode,
		r.OperatorToken,
		int(r.Price),
	); err != nil {
		err = fmt.Errorf("db.Exec: %s, Query: %s", err.Error(), query)
		return
	}

	publishReporter(svc.sConfig.Queue.Transaction, mid.Collect{
		Tid:               r.Tid,
		CampaignCode:      r.CampaignCode,
		OperatorCode:      r.OperatorCode,
		Msisdn:            r.Msisdn,
		Price:             r.Price,
		TransactionResult: r.Result,
		AttemptsCount:     r.AttemptsCount,
	})

	svc.m.MTManager.WriteTransactionDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func unsubscribe(r rec.Record) (err error) {
	begin := time.Now()
	r.SubscriptionStatus = "canceled"
	defer func() {
		fields := log.Fields{
			"tid":        r.Tid,
			"service_id": r.ServiceCode,
			"msisdn":     r.Msisdn,
			"result":     r.SubscriptionStatus,
			"took":       time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		log.WithFields(fields).Debug("unsubscribe")
	}()
	query := fmt.Sprintf("UPDATE %ssubscriptions SET "+
		"result = $1, "+
		"attempts_count = attempts_count + 1, "+
		"last_pay_attempt_at = $2 "+
		"WHERE id = ("+
		"	SELECT id FROM %ssubscriptions "+
		"	WHERE msisdn = $3 AND id_service = $4 and result != 'canceled'"+
		"	ORDER BY id LIMIT 1)",
		svc.dbConf.TablePrefix,
		svc.dbConf.TablePrefix,
	)

	lastPayAttemptAt := r.SentAt
	var res sql.Result
	res, err = svc.db.Exec(query,
		r.SubscriptionStatus,
		lastPayAttemptAt,
		r.Msisdn,
		r.ServiceCode,
	)
	if err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	count, err := res.RowsAffected()
	if err != nil {
		err = fmt.Errorf("res.RowsAffected: %s", err.Error())
		return
	}
	if count > 0 {
		r.Result = r.SubscriptionStatus
		publishReporter(svc.sConfig.Queue.Outflow, mid.Collect{
			Tid:               r.Tid,
			CampaignCode:      r.CampaignCode,
			OperatorCode:      r.OperatorCode,
			Msisdn:            r.Msisdn,
			Price:             r.Price,
			TransactionResult: r.Result,
			AttemptsCount:     r.AttemptsCount,
		})
	}
	svc.m.MTManager.UnsubscribeDuration.Observe(time.Since(begin).Seconds())
	svc.m.Common.DBUpdateDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func unsubscribeAll(r rec.Record) (err error) {
	begin := time.Now()
	r.SubscriptionStatus = "purged"
	if r.OutFlowReason == "" {
		r.OutFlowReason = "purge request"
	}
	fields := log.Fields{
		"tid":    r.Tid,
		"result": r.SubscriptionStatus,
		"took":   time.Since(begin),
	}
	defer func() {
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		log.WithFields(fields).Debug("unsubscribe")
	}()

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"id_campaign, "+
		"operator_code, "+
		"attempts_count "+
		"FROM %ssubscriptions "+
		"WHERE msisdn = $1 AND "+
		"result NOT IN ('canceled', 'purged', 'rejected', 'blacklisted', 'postpaid')",
		svc.dbConf.TablePrefix,
	)
	rowsUns, err := svc.db.Query(query, r.Msisdn)
	if err != nil {
		svc.m.Common.DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rowsUns.Close()

	var unsubscribedRecs []rec.Record
	for rowsUns.Next() {
		t := rec.Record{}
		if err := rowsUns.Scan(
			&t.SubscriptionId,
			&t.CampaignCode,
			&t.OperatorCode,
			&t.AttemptsCount,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		unsubscribedRecs = append(unsubscribedRecs, t)
	}

	query = fmt.Sprintf("UPDATE %ssubscriptions SET "+
		"result = $1, "+
		"outflow_reason = $2, "+
		"last_pay_attempt_at = $3 "+
		"WHERE msisdn = $4 AND "+
		"result NOT IN ('canceled', 'purged', 'rejected', 'blacklisted', 'postpaid')",
		svc.dbConf.TablePrefix,
	)

	lastPayAttemptAt := r.SentAt

	var res sql.Result
	res, err = svc.db.Exec(query,
		r.SubscriptionStatus,
		r.OutFlowReason,
		lastPayAttemptAt,
		r.Msisdn,
	)
	if err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	if count, err := res.RowsAffected(); err == nil {
		if count != int64(len(unsubscribedRecs)) {
			fields["affected"] = count
			fields["unsubs"] = len(unsubscribedRecs)
			log.WithFields(fields).Error("unsubscribe count mismatch")
			delete(fields, "affected")
			delete(fields, "unsubs")
		}
		if count == 0 {
			log.WithFields(fields).Debug("nothing was purged")
		}
	} else {
		err = fmt.Errorf("res.RowsAffected: %s", err.Error())
		fields["error"] = err.Error()
		log.WithFields(fields).Debug("cannot get count affected purge request")
		delete(fields, "error")
	}

	for _, t := range unsubscribedRecs {
		t.Result = t.SubscriptionStatus
		publishReporter(svc.sConfig.Queue.Outflow, mid.Collect{
			Tid:               r.Tid,
			CampaignCode:      r.CampaignCode,
			OperatorCode:      r.OperatorCode,
			Msisdn:            r.Msisdn,
			Price:             r.Price,
			TransactionResult: r.Result,
			AttemptsCount:     r.AttemptsCount,
		})
	}
	if len(unsubscribedRecs) == 0 {
		log.WithFields(fields).Debug("nothing to purge")
	}

	svc.m.MTManager.UnsubscribeAllDuration.Observe(time.Since(begin).Seconds())
	svc.m.Common.DBUpdateDuration.Observe(time.Since(begin).Seconds())
	return nil
}
func writeSubscriptionPeriodic(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":      r.Tid,
			"periodic": r.Periodic,
			"id":       r.SubscriptionId,
			"took":     time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		log.WithFields(fields).Debug("write subscription periodic")
	}()
	query := fmt.Sprintf("UPDATE %ssubscriptions SET periodic = $1 WHERE id = $2",
		svc.dbConf.TablePrefix,
	)
	_, err = svc.db.Exec(query,
		r.Periodic,
		r.SubscriptionId,
	)
	if err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}
	svc.m.MTManager.WriteSubscriptionPeriodicDuration.Observe(time.Since(begin).Seconds())
	svc.m.Common.DBUpdateDuration.Observe(time.Since(begin).Seconds())
	return nil
}

func writeSubscriptionStatus(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":    r.Tid,
			"result": r.SubscriptionStatus,
			"took":   time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			fields["rec"] = fmt.Sprintf("%#v", r)
		}
		log.WithFields(fields).Debug("write subscription status")
	}()
	query := fmt.Sprintf("UPDATE %ssubscriptions SET "+
		"result = $1, "+
		"last_pay_attempt_at = $2, "+
		"attempts_count = attempts_count + 1 "+
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
	// in case if it was unsub/unreg, it would catch, otherwise not.
	r.Result = r.SubscriptionStatus
	publishReporter(svc.sConfig.Queue.Outflow, mid.Collect{
		Tid:               r.Tid,
		CampaignCode:      r.CampaignCode,
		OperatorCode:      r.OperatorCode,
		Msisdn:            r.Msisdn,
		Price:             r.Price,
		TransactionResult: r.Result,
		AttemptsCount:     r.AttemptsCount,
	})

	svc.m.MTManager.WriteSubscriptionStatusDuration.Observe(time.Since(begin).Seconds())
	svc.m.Common.DBUpdateDuration.Observe(time.Since(begin).Seconds())
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
	var query string

	if r.SubscriptionStatus != "paid" {
		query = fmt.Sprintf(`INSERT INTO
	%sretries_expired(
		  status,
		  tid,
		  created_at ,
		  price,
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
		  price,
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
	}

	query = fmt.Sprintf("DELETE FROM %sretries WHERE id = $1", svc.dbConf.TablePrefix)

	if _, err = svc.db.Exec(query, r.RetryId); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}
	svc.m.MTManager.RemoveRetryDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
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
	svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
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
	if r.RetryDays == 0 {
		err = fmt.Errorf("Retry Keep Days required, service id: %s", r.ServiceCode)
		return
	}
	if r.DelayHours == 0 {
		err = fmt.Errorf("Retry Delay Hours required, service id: %s", r.ServiceCode)
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
		&r.RetryDays,
		&r.DelayHours,
		&r.Msisdn,
		&r.OperatorCode,
		&r.CountryCode,
		&r.ServiceCode,
		&r.SubscriptionId,
		&r.CampaignCode,
		&r.Price,
	); err != nil {
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	svc.m.MTManager.StartRetryDuration.Observe(time.Since(begin).Seconds())
	svc.m.MTManager.AddToDBDuration.Observe(time.Since(begin).Seconds())
	svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
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
	svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
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
	svc.m.Common.DBInsertDuration.Observe(time.Since(begin).Seconds())
	return nil
}
