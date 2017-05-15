package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"runtime"

	log "github.com/Sirupsen/logrus"

	"github.com/gin-gonic/gin"

	"github.com/linkit360/go-qlistener/src/config"
	"github.com/linkit360/go-qlistener/src/service"
	m "github.com/linkit360/go-utils/metrics"
	"os"
)

func RunServer() {
	appConfig := config.LoadConfig()

	service.InitService(
		appConfig.AppName,
		appConfig.Service,
		appConfig.InMemClientConfig,
		appConfig.ReporterConfig,
		appConfig.DbConf,
		appConfig.Consumer,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()

	m.AddHandler(r)

	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("Qlistener init")
}

func OnExit() {
	log.WithField("pid", os.Getpid()).Info("on exit")
	service.OnExit()
}
