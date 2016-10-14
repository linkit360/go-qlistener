package service

import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/rabbit"
)

var svc Service

const ACTIVE_STATUS = 1

func InitService(sConf ServiceConfig) {
	initDatabase(sConf.dbConf)
	svc.sConfig = sConf

	svc.accessCampaign = rabbit.NewConsumer(sConf.Queue.AccessCampaign, sConf.RBMQ)
	svc.contentSent = rabbit.NewConsumer(sConf.Queue.ContentSent, sConf.RBMQ)
	if err := initCQR(); err != nil {
		log.WithField("error", err.Error()).Fatal("Init CQR")
	}
	go func() {
		contentSent()
	}()
}

type Service struct {
	db             *sql.DB
	dbConfig       DataBaseConfig
	accessCampaign rabbit.AMQPService
	contentSent    rabbit.AMQPService
	sConfig        ServiceConfig
	tables         map[string]struct{}
}
type QueuesConfig struct {
	AccessCampaign string `default:"access_campaign" yaml:"access_campaign"`
	ContentSent    string `default:"content_sent" yaml:"content_sent"`
}
type ServiceConfig struct {
	RBMQ        rabbit.RBMQConfig `yaml:"rabbit"`
	Queue       QueuesConfig      `yaml:"queues"`
	dbConf      DataBaseConfig    `yaml:"db"`
	TablePrefix string            `default:"xmp_" yaml:"table_prefix"`
	Tables      []string          `default:"subscriptions" yaml:"tables"`
}

func initCQR() error {
	if err := subscriptions.Reload(); err != nil {
		return fmt.Errorf("subscriptions.Reload: %s", err.Error())
	}
	svc.tables = make(map[string]struct{}, len(svc.sConfig.Tables))
	for _, v := range svc.sConfig.Tables {
		svc.tables[v] = struct{}{}
	}
	return nil
}

func CQR(table string) (bool, error) {
	if len(table) == 0 {
		log.WithField("error", "No table name given").Errorf("CQR request")
		return false, nil
	}
	_, ok := svc.tables[table]
	if !ok {
		log.WithField("error", "table name doesn't match any").Errorf("CQR request")
		return false, nil
	}
	// should we re-build service
	switch table {
	case "subscriptions":
		if err := subscriptions.Reload(); err != nil {
			return false, fmt.Errorf("subscriptions.Reload: %s", err.Error())
		}
	default:
		return false, fmt.Errorf("CQR Request: Unknown table: %s", table)
	}

	return true, nil
}

// msisdn _ service_id ==> subscription_id

// Keep in memory all active campaigns
// Allow to get a subscription_id by msisdn and service_id
// Reload when changes to subscriptions made
// usage:
// subscripions_id, ok := subscripions.Map[ s.key() ]
var subscriptions = &Subscriptions{}

type Subscriptions struct {
	sync.RWMutex
	Map map[string]int64
}
type Subscription struct {
	Msisdn         string
	ServiceId      int64
	SubscriptionId int64
}

func (s Subscription) key() string {
	return fmt.Sprintf("%s-%d", s.Msisdn, s.ServiceId)
}
func (s Subscriptions) Reload() error {
	query := fmt.Sprintf("select msisdn, id_service, id_subscription from "+
		"%ssubscriptions where status = $1", svc.sConfig.TablePrefix)
	rows, err := svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("Subscriptions Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var records []Subscription
	for rows.Next() {
		record := Subscription{}

		if err := rows.Scan(
			&record.Msisdn,
			&record.ServiceId,
			&record.SubscriptionId,
		); err != nil {
			return err
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return fmt.Errorf("Subscriptions Reload RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]int64, len(records))
	for _, subscription := range records {
		s.Map[subscription.key()] = subscription.SubscriptionId
	}
	return nil
}
