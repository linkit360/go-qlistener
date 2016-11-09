package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"runtime"

	log "github.com/Sirupsen/logrus"

	"github.com/gin-gonic/gin"

	m "github.com/vostrok/metrics"
	"github.com/vostrok/qlistener/src/config"
	"github.com/vostrok/qlistener/src/service"
)

func RunServer() {
	appConfig := config.LoadConfig()
	m.Init(appConfig.Name)

	service.InitService(appConfig.Service, appConfig.DbConf, appConfig.Consumer)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()
	service.AddCQRHandlers(r)

	m.AddHandler(r)

	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("Qlistener init")
}
