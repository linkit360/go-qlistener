package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/vostrok/contentd/service"
)

func contentSent() {
	for {
		var t service.ContentSentProperties

		// receive from rbmq
		//t = <-svc.recordContentGiven

		if t.SubscriptionId == 0 {
			s := Subscription{Msisdn: t.Msisdn, ServiceId: t.ServiceId}
			var ok bool
			t.SubscriptionId, ok = subscriptions.Map[s.key()]
			if !ok {
				// do not set id_subscriber: msisdn is unique enough
				query := fmt.Sprintf("INSERT INTO %ssubscriptions ( "+
					"status, "+
					"id_campaign, "+
					"id_service, "+
					"msisdn, "+
					"country_code, "+
					"operator_code) "+
					" values ($1, $2, $3, $4, $5, $6) RETURNING id",
					svc.sConfig.TablePrefix)

				if err := svc.db.QueryRow(query,
					";",
					t.CampaignId,
					t.ServiceId,
					t.Msisdn,
					t.CountryCode,
					t.OperatorCode,
				).Scan(&t.SubscriptionId); err != nil {

					// not handled, back to rbmq
					//svc.recordContentGiven <- t

					log.WithFields(log.Fields{
						"error":        err.Error(),
						"subscription": t}).
						Error("add new subscription")
					time.Sleep(time.Second)
					continue
				}
			}
		}
		if t.SubscriptionId == 0 {
			log.WithFields(log.Fields{
				"error":        "UNEXPECTED CODE REACHED",
				"subscription": t}).
				Error("add content sent")
		}

		query := fmt.Sprintf("INSERT INTO %scontent_sent ("+
			"msisdn, "+
			"id_campaign, "+
			"id_service, "+
			"id_subscription, "+
			"id_content, "+
			"country_code, "+
			"operator_code)"+
			" values ($1, $2, $3, $4, $5)", svc.sConfig.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Msisdn,
			t.CampaignId,
			t.ServiceId,
			t.SubscriptionId,
			t.ContentId,
			t.CountryCode,
			t.OperatorCode,
		); err != nil {
			log.WithFields(log.Fields{
				"content": t,
				"error":   err.Error()}).
				Error("add sent content")
			// not handled, back to rbmq
			//svc.recordContentGiven <- t
			//svc.recordContentGiven <- t
			time.Sleep(time.Second)
		}
	}

}

//type ContentSentProperties struct {
//	Msisdn         string `json:"msisdn"`
//	ContentPath    string `json:"content_path"`
//	CapmaignHash   string `json:"capmaign_hash"`
//	CampaignId     int64  `json:"campaign_id"`
//	ContentId      int64  `json:"content_id"`
//	ServiceId      int64  `json:"service_id"`
//	SubscriptionId int64  `json:"subscription_id"`
//	CountryCode    int64  `json:"country_code"`
//	OperatorCode   int64  `json:"operator_code"`
//}
