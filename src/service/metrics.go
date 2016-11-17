package service

import (
	"time"

	m "github.com/vostrok/metrics"
)

func newMetrics() Metrics {
	m := Metrics{
		DbErrors:       m.NewGauge("", "", "db_errors", "db errors"),
		AccessCampaign: initAccessCampaignMetrics(),
		ContentSent:    initContentSentMetrics(),
		UserActions:    initUserActionsMetrics(),
		Operator:       initOperatorsMetrics(),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.DbErrors.Update()
		}
	}()
	return m
}

// todo: add_to_db_success
// todo: add_to_db_errors
type Metrics struct {
	DbErrors       m.Gauge
	AccessCampaign *accessCampaignMetrics
	ContentSent    *contentSentMetrics
	UserActions    *userActionsMetrics
	Operator       *operatorMetrics
}

// Access Campaign metrics
type accessCampaignMetrics struct {
	Dropped          m.Gauge
	Empty            m.Gauge
	UnknownHash      m.Gauge
	ErrorsParseGeoIp m.Gauge
	AddToDbSuccess   m.Gauge
	AddToDBErrors    m.Gauge
}

func newGaugeAccessCampaign(name, help string) m.Gauge {
	return m.NewGauge("", "access_campaign", ""+name, "access campaign "+help)
}
func initAccessCampaignMetrics() *accessCampaignMetrics {
	m := &accessCampaignMetrics{
		Dropped:          newGaugeAccessCampaign("dropped", "dropped msgs"),
		Empty:            newGaugeAccessCampaign("empty", "dmpty msgs"),
		UnknownHash:      newGaugeAccessCampaign("unknown_hash", "dnknown campaign hash"),
		ErrorsParseGeoIp: newGaugeAccessCampaign("parse_geoip_errors", "parse geoip error"),
		AddToDbSuccess:   newGaugeAccessCampaign("add_to_db_success", "create access campaign"),
		AddToDBErrors:    newGaugeAccessCampaign("add_to_db_errors", "access campaign db errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.UnknownHash.Update()
			m.ErrorsParseGeoIp.Update()
			m.AddToDbSuccess.Update()
			m.AddToDBErrors.Update()
		}
	}()
	return m
}

// Content Sent metrics
func newGaugeContentSent(name, help string) m.Gauge {
	return m.NewGauge("", "content_sent", name, "content sent "+help)
}
func newGaugeAddedSubscription(name, help string) m.Gauge {
	return m.NewGauge("", "new_subscription", name, "new subscription "+help)
}

type contentSentMetrics struct {
	Dropped                    m.Gauge
	Empty                      m.Gauge
	SubscriptionsAddToDBErrors m.Gauge
	SubscriptionAddToDbSuccess m.Gauge
	AddToDbSuccess             m.Gauge
	AddToDBErrors              m.Gauge
}

func initContentSentMetrics() *contentSentMetrics {
	m := &contentSentMetrics{
		Dropped: newGaugeContentSent("dropped", "dropped msgs"),
		Empty:   newGaugeContentSent("empty", "empty msgs"),
		SubscriptionsAddToDBErrors: newGaugeAddedSubscription("add_to_db_errors", "add to db errors"),
		SubscriptionAddToDbSuccess: newGaugeAddedSubscription("add_to_db_success", "add to db count"),
		AddToDbSuccess:             newGaugeContentSent("add_to_db_success", "add to db errors"),
		AddToDBErrors:              newGaugeContentSent("add_to_db_errors", "add to db errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.SubscriptionsAddToDBErrors.Update()
			m.SubscriptionAddToDbSuccess.Update()
			m.AddToDbSuccess.Update()
			m.AddToDBErrors.Update()
		}
	}()
	return m
}

// user actions metrics
func newGaugeUserActions(name, help string) m.Gauge {
	return m.NewGauge("", "user_actions", name, "user actions "+help)
}

type userActionsMetrics struct {
	Dropped        m.Gauge
	Empty          m.Gauge
	AddToDbSuccess m.Gauge
	AddToDBErrors  m.Gauge
}

func initUserActionsMetrics() *userActionsMetrics {
	m := &userActionsMetrics{
		Dropped:        newGaugeUserActions("dropped", "dropped msgs"),
		Empty:          newGaugeUserActions("empty", "empty msgs"),
		AddToDbSuccess: newGaugeUserActions("add_to_db_success", "create records count"),
		AddToDBErrors:  newGaugeUserActions("add_to_db_errors", "create record: database errors"),
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
	return m.NewGauge("", "operator", name, "operator transaction logs "+help)
}

type operatorMetrics struct {
	Dropped        m.Gauge
	Empty          m.Gauge
	AddToDbSuccess m.Gauge
	AddToDBErrors  m.Gauge
}

func initOperatorsMetrics() *operatorMetrics {
	m := &operatorMetrics{
		Dropped:        newGaugeOperator("dropped", "dropped msgs"),
		Empty:          newGaugeOperator("empty", "empty msgs"),
		AddToDbSuccess: newGaugeOperator("add_to_db_success", "create records count"),
		AddToDBErrors:  newGaugeOperator("add_to_db_errors", "create record: database errors"),
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
