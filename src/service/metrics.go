package service

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/utils/metrics"
)

var appName string

func newMetrics(name string) Metrics {
	appName = name
	m := Metrics{
		DBErrors:         m.NewGauge("", "", "db_errors", "db errors"),
		DBInsertDuration: m.NewSummary(appName+"_insert_db_duration_seconds", "db insert duration seconds"),
		AccessCampaign:   initAccessCampaignMetrics(),
		ContentSent:      initContentSentMetrics(),
		MTManager:        initMtManagerMetrics(),
		Operator:         initOperatorsMetrics(),
		Pixels:           initPixelMetrics(),
		UserActions:      initUserActionsMetrics(),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.DBErrors.Update()
		}
	}()
	return m
}

// todo: add_to_db_success add_to_db_errors
type Metrics struct {
	DBErrors         m.Gauge
	DBInsertDuration prometheus.Summary
	AccessCampaign   *accessCampaignMetrics
	ContentSent      *contentSentMetrics
	UserActions      *userActionsMetrics
	Operator         *operatorMetrics
	MTManager        *mtManagerMetrics
	Pixels           *pixelMetrics
}

// Access Campaign metrics
type accessCampaignMetrics struct {
	Dropped          m.Gauge
	Empty            m.Gauge
	UnknownHash      m.Gauge
	ErrorsParseGeoIp m.Gauge
	AddToDbSuccess   m.Gauge
	AddToDbDuration  prometheus.Summary
	AddToDBErrors    m.Gauge
}

func newAddToDBDuration(name string) prometheus.Summary {
	return m.NewSummary(appName+"_"+name+"_add_to_db_duration_seconds", name+" duration seconds")
}
func newUpdateDBDuration(name string) prometheus.Summary {
	return m.NewSummary(appName+"_"+name+"_update_db_duration_seconds", name+" duration seconds")
}
func newGaugeAccessCampaign(name, help string) m.Gauge {
	return m.NewGauge(appName, "access_campaign", ""+name, "access campaign "+help)
}
func initAccessCampaignMetrics() *accessCampaignMetrics {
	m := &accessCampaignMetrics{
		Dropped:          newGaugeAccessCampaign("dropped", "dropped msgs"),
		Empty:            newGaugeAccessCampaign("empty", "dmpty msgs"),
		UnknownHash:      newGaugeAccessCampaign("unknown_hash", "dnknown campaign hash"),
		ErrorsParseGeoIp: newGaugeAccessCampaign("parse_geoip_errors", "parse geoip error"),
		AddToDbSuccess:   newGaugeAccessCampaign("add_to_db_success", "create access campaign"),
		AddToDbDuration:  newAddToDBDuration("access_campaign"),
		AddToDBErrors:    newGaugeAccessCampaign("add_to_db_errors", "access campaign db errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.UnknownHash.Update()
			m.AddToDbSuccess.Update()
			m.ErrorsParseGeoIp.Update()
			m.AddToDBErrors.Update()
		}
	}()
	return m
}

// Content Sent metrics
func newGaugeContentSent(name, help string) m.Gauge {
	return m.NewGauge(appName, "content_sent", name, "content sent "+help)
}

type contentSentMetrics struct {
	Dropped         m.Gauge
	Empty           m.Gauge
	AddToDbSuccess  m.Gauge
	AddToDBDuration prometheus.Summary
	AddToDBErrors   m.Gauge
}

func initContentSentMetrics() *contentSentMetrics {
	m := &contentSentMetrics{
		Dropped:         newGaugeContentSent("dropped", "dropped msgs"),
		Empty:           newGaugeContentSent("empty", "empty msgs"),
		AddToDbSuccess:  newGaugeContentSent("add_to_db_success", "add to db errors"),
		AddToDBDuration: newAddToDBDuration("content_sent"),
		AddToDBErrors:   newGaugeContentSent("add_to_db_errors", "add to db errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.AddToDbSuccess.Update()
			m.AddToDBErrors.Update()
		}
	}()
	return m
}

func newGaugeMTManager(name, help string) m.Gauge {
	return m.NewGauge(appName, "mt_manager", name, "mt manager "+help)
}

type mtManagerMetrics struct {
	Dropped                           m.Gauge
	Empty                             m.Gauge
	AddToDbSuccess                    m.Gauge
	AddToDBErrors                     m.Gauge
	AddToDBDuration                   prometheus.Summary
	AddBlacklistedNumberDuration      prometheus.Summary
	AddPostPaidNumberDuration         prometheus.Summary
	StartRetryDuration                prometheus.Summary
	TouchRetryDuration                prometheus.Summary
	RemoveRetryDuration               prometheus.Summary
	WriteSubscriptionStatusDuration   prometheus.Summary
	WriteSubscriptionPeriodicDuration prometheus.Summary
	UnsubscribeDuration               prometheus.Summary
	WriteTransactionDuration          prometheus.Summary
}

func newDuration(name string) prometheus.Summary {
	return m.NewSummary(appName+"_"+name+"_duration_seconds", name)
}

func initMtManagerMetrics() *mtManagerMetrics {
	m := &mtManagerMetrics{
		Dropped:                           newGaugeMTManager("dropped", "dropped msgs"),
		Empty:                             newGaugeMTManager("empty", "empty msgs"),
		AddToDbSuccess:                    newGaugeMTManager("add_to_db_success", "add to db success"),
		AddToDBErrors:                     newGaugeMTManager("add_to_db_errors", "add to db errors"),
		AddToDBDuration:                   newAddToDBDuration("mt_manager_db"),
		AddBlacklistedNumberDuration:      newDuration("add_blacklisted_db"),
		AddPostPaidNumberDuration:         newDuration("add_postpaid_db"),
		StartRetryDuration:                newDuration("start_retry_db"),
		TouchRetryDuration:                newDuration("touch_retry_db"),
		RemoveRetryDuration:               newDuration("remove_retry_db"),
		WriteSubscriptionStatusDuration:   newDuration("write_subscription_status_db"),
		WriteSubscriptionPeriodicDuration: newDuration("write_subscription_periodic_db"),
		UnsubscribeDuration:               newDuration("unsubscribe"),
		WriteTransactionDuration:          newDuration("write_transaction_db"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.AddToDbSuccess.Update()
			m.AddToDBErrors.Update()
		}
	}()
	return m
}

// user actions metrics
func newGaugeUserActions(name, help string) m.Gauge {
	return m.NewGauge(appName, "user_actions", name, "user actions "+help)
}

type userActionsMetrics struct {
	Dropped         m.Gauge
	Empty           m.Gauge
	AddToDbSuccess  m.Gauge
	AddToDBDuration prometheus.Summary
	AddToDBErrors   m.Gauge
}

func initUserActionsMetrics() *userActionsMetrics {
	m := &userActionsMetrics{
		Dropped:         newGaugeUserActions("dropped", "dropped msgs"),
		Empty:           newGaugeUserActions("empty", "empty msgs"),
		AddToDbSuccess:  newGaugeUserActions("add_to_db_success", "create records count"),
		AddToDBDuration: newAddToDBDuration("user_actions"),
		AddToDBErrors:   newGaugeUserActions("add_to_db_errors", "create record: database errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.AddToDbSuccess.Update()
			m.AddToDBErrors.Update()
		}
	}()
	return m
}

// operator transaction log metrics
func newGaugeOperator(name, help string) m.Gauge {
	return m.NewGauge(appName, "operator", name, "operator transaction logs "+help)
}

type operatorMetrics struct {
	Dropped         m.Gauge
	Empty           m.Gauge
	AddToDbSuccess  m.Gauge
	AddToDBDuration prometheus.Summary
	AddToDBErrors   m.Gauge
}

func initOperatorsMetrics() *operatorMetrics {
	m := &operatorMetrics{
		Dropped:         newGaugeOperator("dropped", "dropped msgs"),
		Empty:           newGaugeOperator("empty", "empty msgs"),
		AddToDbSuccess:  newGaugeOperator("add_to_db_success", "create records count"),
		AddToDBDuration: newAddToDBDuration("operator_transactions"),
		AddToDBErrors:   newGaugeOperator("add_to_db_errors", "create record: database errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.AddToDbSuccess.Update()
			m.AddToDBErrors.Update()
		}
	}()
	return m
}

// pixel transaction log metrics
func newGaugePixels(name, help string) m.Gauge {
	return m.NewGauge(appName, "pixel", name, "pixel "+help)
}

type pixelMetrics struct {
	Dropped                      m.Gauge
	Empty                        m.Gauge
	AddToDbSuccess               m.Gauge
	AddToDBDuration              prometheus.Summary
	AddToDBErrors                m.Gauge
	UpdateSubscriptionSuccess    m.Gauge
	UpdateDBDuration             prometheus.Summary
	UpdateSubscriptionToDBErrors m.Gauge
}

func initPixelMetrics() *pixelMetrics {
	m := &pixelMetrics{
		Dropped:                      newGaugePixels("dropped", "dropped msgs"),
		Empty:                        newGaugePixels("empty", "empty msgs"),
		AddToDbSuccess:               newGaugePixels("add_to_db_success", "create records count"),
		AddToDBDuration:              newAddToDBDuration("pixel_transactions"),
		AddToDBErrors:                newGaugePixels("add_to_db_errors", "create record: database errors"),
		UpdateSubscriptionSuccess:    newGaugePixels("update_subscriptions_db_success", "pixels: update subscriptions success"),
		UpdateDBDuration:             newUpdateDBDuration("subscription_pixel_sent"),
		UpdateSubscriptionToDBErrors: newGaugePixels("update_subscriptions_db_errors", "pixels: update subscriptions errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.AddToDbSuccess.Update()
			m.AddToDBErrors.Update()
			m.UpdateSubscriptionSuccess.Update()
			m.UpdateSubscriptionToDBErrors.Update()
		}
	}()
	return m
}
