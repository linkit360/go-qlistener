package service

import (
	"time"

	m "github.com/vostrok/metrics"
)

// Access Campaign metrics
type accessCampaignMetrics struct {
	Dropped          m.Gauge
	Empty            m.Gauge
	UnknownHash      m.Gauge
	ErrorsParseGeoIp m.Gauge
	CreateCount      m.Gauge
	CreateDBErrors   m.Gauge
}

func newGaugeAccessCampaign(name, help string) m.Gauge {
	return m.NewCustomMetric("access_campaign", name, "access campaign "+help)
}
func initAccessCampaignMetrics() accessCampaignMetrics {
	m := accessCampaignMetrics{
		Dropped:          newGaugeAccessCampaign("dropped", "dropped msgs"),
		Empty:            newGaugeAccessCampaign("empty", "dmpty msgs"),
		UnknownHash:      newGaugeAccessCampaign("unknown_hash", "dnknown campaign hash"),
		ErrorsParseGeoIp: newGaugeAccessCampaign("parse_geoip_errors", "parse geoip error"),
		CreateCount:      newGaugeAccessCampaign("create_count", "create access campaign"),
		CreateDBErrors:   newGaugeAccessCampaign("create_db_errors", "access campaign db errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.UnknownHash.Update()
			m.ErrorsParseGeoIp.Update()
			m.CreateCount.Update()
			m.CreateDBErrors.Update()
		}
	}()
	return m
}

// Content Sent metrics
func newGaugeContentSent(name, help string) m.Gauge {
	return m.NewCustomMetric("content_sent", name, "content sent "+help)
}

type contentSentMetrics struct {
	Dropped                  m.Gauge
	Empty                    m.Gauge
	SubscriptionCreateErrors m.Gauge
	SubscriptionCreateCount  m.Gauge
	CreateCount              m.Gauge
	CreateErrors             m.Gauge
}

func initContentSentMetrics() contentSentMetrics {
	m := contentSentMetrics{
		Dropped: newGaugeContentSent("dropped", "dropped msgs"),
		Empty:   newGaugeContentSent("empty", "empty msgs"),
		SubscriptionCreateErrors: newGaugeContentSent("subscription_create_count", "subscritpion create"),
		SubscriptionCreateCount:  newGaugeContentSent("subscription_create_errors", "subscription create db errors"),
		CreateCount:              newGaugeContentSent("create_count", "create count"),
		CreateErrors:             newGaugeContentSent("create_errors", "create db errors count"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.SubscriptionCreateErrors.Update()
			m.SubscriptionCreateCount.Update()
			m.CreateCount.Update()
			m.CreateErrors.Update()
		}
	}()
	return m
}

// user actions metrics
func newGaugeUserAcrtions(name, help string) m.Gauge {
	return m.NewCustomMetric("user_actions", name, "user actions "+help)
}

type userActionsMetrics struct {
	Dropped        m.Gauge
	Empty          m.Gauge
	CreateCount    m.Gauge
	CreateDBErrors m.Gauge
}

func initUserActionsMetrics() userActionsMetrics {
	m := userActionsMetrics{
		Dropped:        newGaugeUserAcrtions("dropped", "dropped msgs"),
		Empty:          newGaugeUserAcrtions("empty", "empty msgs"),
		CreateCount:    newGaugeUserAcrtions("create_count", "create records count"),
		CreateDBErrors: newGaugeUserAcrtions("create_db_errors", "create record: database errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.CreateCount.Update()
			m.CreateDBErrors.Update()
		}

	}()
	return m
}

// operator transaction log metrics
func newGaugeOperator(name, help string) m.Gauge {
	return m.NewCustomMetric("operator", name, "operator "+help)
}

type operatorMetrics struct {
	Dropped        m.Gauge
	Empty          m.Gauge
	CreateCount    m.Gauge
	CreateDBErrors m.Gauge
}

func initOperatorsMetrics() operatorMetrics {
	m := operatorMetrics{
		Dropped:        newGaugeOperator("dropped", "dropped msgs"),
		Empty:          newGaugeOperator("empty", "empty msgs"),
		CreateCount:    newGaugeOperator("create_count", "create records count"),
		CreateDBErrors: newGaugeOperator("create_db_errors", "create record: database errors"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			m.Dropped.Update()
			m.Empty.Update()
			m.CreateCount.Update()
			m.CreateDBErrors.Update()
		}

	}()
	return m
}
