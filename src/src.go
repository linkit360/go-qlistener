package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/contrib/expvar"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/qlistener/src/config"
	"github.com/vostrok/qlistener/src/metrics"
	"github.com/vostrok/qlistener/src/newrelic"
	"github.com/vostrok/qlistener/src/service"
)

func RunServer() {
	appConfig := config.LoadConfig()
	metrics.Init()
	newrelic.Init(appConfig.NewRelic)
	service.InitService(appConfig.Service)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()
	rg := r.Group("/debug")
	rg.GET("/vars", expvar.Handler())
	r.Run(":" + appConfig.Server.Port)
}
