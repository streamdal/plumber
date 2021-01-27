// Singleton so that it's easier to use in other packages
package stats

import (
	"sync"
	"time"

	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
)

var (
	ReportInterval = 10 * time.Second

	looper   director.Looper
	mutex    *sync.Mutex
	counters map[string]int
)

func Start(reportInterval time.Duration) {
	ReportInterval = reportInterval
	looper = director.NewTimedLooper(director.FOREVER, ReportInterval, make(chan error, 1))
	mutex = &sync.Mutex{}
	counters = make(map[string]int, 0)

	logrus.Debug("Launching stats reporter")

	go func() {
		looper.Loop(func() error {
			mutex.Lock()
			defer mutex.Unlock()

			for counterName, counterValue := range counters {
				perSecond := counterValue / int(ReportInterval.Seconds())

				logrus.Infof("STATS [%s]: %d / %s (%d/s)\n", counterName, counterValue,
					ReportInterval, perSecond)

				// Reset it
				counters[counterName] = 0
			}

			return nil
		})
	}()
}

func Incr(name string, value int) {
	mutex.Lock()
	defer mutex.Unlock()

	counters[name] += value
}
