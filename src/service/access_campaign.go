package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/vostrok/dispatcherd/src/rbmq"
)

func accessCampaign() {
	for {

		var t rbmq.AccessCampaignNotify

		query := fmt.Sprintf("INSERT INTO %scampaigns_access ("+
			"msisdn, "+
			"ip, "+
			"operator_code"+
			"country_code, "+
			"supported, "+
			"user_agent, "+
			"referer, "+
			"url_path, "+
			"method, "+
			"headers, "+
			"content_error, "+
			"file_error, "+
			"id_campaign, "+
			"id_service, "+
			"id_content, "+
			")"+
			" values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)", svc.sConfig.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Msisdn,
			t.IP,
			t.OperatorCode,
			t.CountryCode,
			t.Supported,
			t.UserAgent,
			t.Referer,
			t.UrlPath,
			t.Method,
			t.Headers,
			t.ContentServiceError,
			t.ContentFileError,
			t.CampaignId,
			t.ServiceId,
			t.ContentId,
		); err != nil {
			log.WithFields(log.Fields{
				"access_log": t,
				"error":      err.Error()}).
				Error("add access campaign log failed")
			// not handled, back to rbmq
			//svc.recordContentGiven <- t
			//svc.recordContentGiven <- t
			time.Sleep(time.Second)
		}
	}
}
