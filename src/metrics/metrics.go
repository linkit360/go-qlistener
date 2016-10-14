package metrics

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
)

var M AppMetrics

type AppMetrics struct {
	ContentSentCount     metrics.Counter
	ContentSentErrors    metrics.Counter
	AccessCampaignCount  metrics.Counter
	AccessCampaignErrors metrics.Counter
}

func Init() AppMetrics {
	M = AppMetrics{
		ContentSentCount:     expvar.NewCounter("content_sent_count"),
		ContentSentErrors:    expvar.NewCounter("content_sent_errors"),
		AccessCampaignCount:  expvar.NewCounter("access_campaign_count"),
		AccessCampaignErrors: expvar.NewCounter("access_campaign_errors"),
	}
	return M
}
