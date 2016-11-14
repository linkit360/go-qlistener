package service

import (
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/metrics"
)

func newMetrics() Metrics {
	return Metrics{
		DbError:        m.NewCounter("db_errors", "db errors"),
		AccessCampaign: initAccessCampaignMetrics(),
		ContentSent:    initContentSentMetrics(),
		UserActions:    initUserActionsMetrics(),
		Operator:       initOperatorsMetrics(),
	}
}

// todo: add_to_db_success
// todo: add_to_db_errors
type Metrics struct {
	DbError        prometheus.Counter
	AccessCampaign *accessCampaignMetrics
	ContentSent    *contentSentMetrics
	UserActions    *userActionsMetrics
	Operator       *operatorMetrics
}

// Access Campaign metrics
type accessCampaignMetrics struct {
	Dropped          prometheus.Counter
	Empty            prometheus.Counter
	UnknownHash      prometheus.Counter
	ErrorsParseGeoIp prometheus.Counter
	AddToDbSuccess   prometheus.Counter
	AddToDBErrors    prometheus.Counter
}

func newCounterAccessCampaign(name, help string) prometheus.Counter {
	return m.NewCounter("access_campaign_"+name, "access campaign "+help)
}
func initAccessCampaignMetrics() *accessCampaignMetrics {
	m := &accessCampaignMetrics{
		Dropped:          newCounterAccessCampaign("dropped", "dropped msgs"),
		Empty:            newCounterAccessCampaign("empty", "dmpty msgs"),
		UnknownHash:      newCounterAccessCampaign("unknown_hash", "dnknown campaign hash"),
		ErrorsParseGeoIp: newCounterAccessCampaign("parse_geoip_errors", "parse geoip error"),
		AddToDbSuccess:   newCounterAccessCampaign("add_to_db_success", "create access campaign"),
		AddToDBErrors:    newCounterAccessCampaign("add_to_db_errors", "access campaign db errors"),
	}
	return m
}

// Content Sent metrics
func newCounterContentSent(name, help string) prometheus.Counter {
	return m.NewCounter("content_sent_"+name, "content sent "+help)
}
func newCounterAddedSubscription(name, help string) prometheus.Counter {
	return m.NewCounter("new_subscription_"+name, "new subscription "+help)
}

type contentSentMetrics struct {
	Dropped                    prometheus.Counter
	Empty                      prometheus.Counter
	SubscriptionsAddToDBErrors prometheus.Counter
	SubscriptionAddToDbSuccess prometheus.Counter
	AddToDbSuccess             prometheus.Counter
	AddToDBErrors              prometheus.Counter
}

func initContentSentMetrics() *contentSentMetrics {
	m := &contentSentMetrics{
		Dropped: newCounterContentSent("dropped", "dropped msgs"),
		Empty:   newCounterContentSent("empty", "empty msgs"),
		SubscriptionsAddToDBErrors: newCounterAddedSubscription("add_to_db_errors", "add to db errors"),
		SubscriptionAddToDbSuccess: newCounterAddedSubscription("add_to_db_success", "add to db count"),
		AddToDbSuccess:             newCounterContentSent("add_to_db_success", "add to db errors"),
		AddToDBErrors:              newCounterContentSent("add_to_db_errors", "add to db errors"),
	}
	return m
}

// user actions metrics
func newCounterUserAcrtions(name, help string) prometheus.Counter {
	return m.NewCounter("user_actions_"+name, "user actions "+help)
}

type userActionsMetrics struct {
	Dropped        prometheus.Counter
	Empty          prometheus.Counter
	AddToDbSuccess prometheus.Counter
	AddToDBErrors  prometheus.Counter
}

func initUserActionsMetrics() *userActionsMetrics {
	m := &userActionsMetrics{
		Dropped:        newCounterUserAcrtions("dropped", "dropped msgs"),
		Empty:          newCounterUserAcrtions("empty", "empty msgs"),
		AddToDbSuccess: newCounterUserAcrtions("add_to_db_success", "create records count"),
		AddToDBErrors:  newCounterUserAcrtions("add_to_db_errors", "create record: database errors"),
	}
	return m
}

// operator transaction log metrics
func newCounterOperator(name, help string) prometheus.Counter {
	return m.NewCounter("operator_"+name, "operator transaction logs "+help)
}

type operatorMetrics struct {
	Dropped        prometheus.Counter
	Empty          prometheus.Counter
	AddToDbSuccess prometheus.Counter
	AddToDBErrors  prometheus.Counter
}

func initOperatorsMetrics() *operatorMetrics {
	m := &operatorMetrics{
		Dropped:        newCounterOperator("dropped", "dropped msgs"),
		Empty:          newCounterOperator("empty", "empty msgs"),
		AddToDbSuccess: newCounterOperator("add_to_db_success", "create records count"),
		AddToDBErrors:  newCounterOperator("add_to_db_errors", "create record: database errors"),
	}
	return m
}
