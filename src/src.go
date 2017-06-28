package src

// purpose is to save in database
import (
	"runtime"

	log "github.com/sirupsen/logrus"

	"github.com/gin-gonic/gin"

	"github.com/linkit360/go-qlistener/src/config"
	"github.com/linkit360/go-qlistener/src/service"
	m "github.com/linkit360/go-utils/metrics"
)

func RunServer() {
	appConfig := config.LoadConfig()

	service.InitService(
		appConfig.AppName,
		appConfig.Service,
		appConfig.MidConf,
		appConfig.Notifier,
		appConfig.DbConf,
		appConfig.Consumer,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()

	m.AddHandler(r)

	r.Run(appConfig.Server.Host + ":" + appConfig.Server.Port)
	log.WithField("dsn", appConfig.Server.Host+":"+appConfig.Server.Port).Info("init")
}
