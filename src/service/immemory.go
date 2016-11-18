package service

import (
	"fmt"
	"sync"
	"time"

	"database/sql"
	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/vostrok/utils/cqr"
)

var inMemConf = []cqr.CQRConfig{
	{
		Table:      "campaigns",
		ReloadFunc: memCampaign.Reload,
	},
}

func initInMem() error {
	return cqr.InitCQR(inMemConf)
}

func AddCQRHandlers(r *gin.Engine) {
	cqr.AddCQRHandler(reloadCQRFunc, r)
}
func reloadCQRFunc(c *gin.Context) {
	cqr.CQRReloadFunc(inMemConf, c)(c)
}

// Tasks:
// Keep in memory all active campaigns
// Allow to get a service_id by campaign hash fastly
// Reload when changes to campaigns are done
var memCampaign = &Campaigns{}

type Campaigns struct {
	sync.RWMutex
	Map map[string]Campaign
}
type Campaign struct {
	Hash      string
	Id        int64
	ServiceId int64
}

func (s *Campaigns) Reload() (err error) {
	log.WithFields(log.Fields{}).Debug("campaigns reload...")
	begin := time.Now()
	defer func(err error) {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("campaigns reload")
	}(err)

	query := fmt.Sprintf("select id, hash, service_id_1 from %scampaigns where status = $1",
		svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []Campaign
	for rows.Next() {
		record := Campaign{}

		if err = rows.Scan(
			&record.Id,
			&record.Hash,
			&record.ServiceId,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[string]Campaign, len(records))
	for _, campaign := range records {
		s.Map[campaign.Hash] = campaign
	}
	return nil
}
