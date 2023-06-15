// Singleton so that it's easier to use in other packages
package prometheus

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
)

const (
	PlumberRelayRate        = "plumber_relay_rate"
	PlumberRelayErrors      = "plumber_relay_errors"
	PlumberRelayTotalEvents = "plumber_relay_total_events"
	PlumberReadErrors       = "plumber_read_errors"
	PlumberGRPCErrors       = "plumber_grpc_errors"
	PlumberRelayWorkers     = "plumber_relay_workers"
	PlumberTunnels          = "plumber_tunnels"
	DataQualBytes           = "dataqual_bytes"
	DataQualMessages        = "dataqual_messages"
)

var (
	ReportInterval = 10 * time.Second

	mutex    = &sync.RWMutex{}
	counters = make(map[string]float64, 0)

	prometheusMutex    = &sync.Mutex{}
	prometheusCounters = make(map[string]prometheus.Counter)
	prometheusGauges   = make(map[string]prometheus.Gauge)

	looper director.Looper
)

func GetLocalCounter(name string) (float64, bool) {
	mutex.RLock()
	defer mutex.RUnlock()

	c, ok := counters[name]
	return c, ok
}

func ResetLocalCounter(name string) {
	mutex.Lock()
	defer mutex.Unlock()

	counters[name] = 0
}

// Start initiates CLI stats reporting
func Start(reportIntervalSeconds int32) {
	interval := time.Duration(reportIntervalSeconds) * time.Second

	looper = director.NewImmediateTimedLooper(director.FOREVER, interval, make(chan error, 1))

	logrus.Debugf("Launching stats reporter ('%s' interval)", interval)

	go func() {
		looper.Loop(func() error {
			mutex.Lock()
			defer mutex.Unlock()

			for counterName, counterValue := range counters {
				perSecond := counterValue / interval.Seconds()

				logrus.Infof("STATS [%s]: %.2f / %s (%.2f/s)\n", counterName, counterValue,
					interval, perSecond)

				if strings.HasSuffix(counterName, "relay-producer") {
					SetPromGauge("plumber_relay_rate", perSecond)
					IncrPromCounter("plumber_relay_total", counterValue)
				}

				// Reset it
				counters[counterName] = 0
			}

			return nil
		})
	}()
}

// InitPrometheusMetrics sets up prometheus counters/gauges
func InitPrometheusMetrics() {
	prometheusMutex.Lock()
	defer prometheusMutex.Unlock()

	prometheusGauges[PlumberRelayRate] = promauto.NewGauge(prometheus.GaugeOpts{
		Name: PlumberRelayRate,
		Help: "Current rare of messages being relayed to Streamdal",
	})

	prometheusGauges[PlumberRelayWorkers] = promauto.NewGauge(prometheus.GaugeOpts{
		Name: PlumberRelayWorkers,
		Help: "Number of active relays",
	})

	prometheusCounters[PlumberRelayTotalEvents] = promauto.NewCounter(prometheus.CounterOpts{
		Name: PlumberRelayTotalEvents,
		Help: "Total number of events relayed to Streamdal",
	})

	prometheusCounters[PlumberRelayErrors] = promauto.NewCounter(prometheus.CounterOpts{
		Name: PlumberRelayErrors,
		Help: "Total number of errors while relaying events to Streamdal",
	})

	prometheusCounters[PlumberReadErrors] = promauto.NewCounter(prometheus.CounterOpts{
		Name: PlumberReadErrors,
		Help: "Number of errors when reading messages",
	})

	prometheusCounters[PlumberGRPCErrors] = promauto.NewCounter(prometheus.CounterOpts{
		Name: PlumberGRPCErrors,
		Help: "Number of errors when making GRPC calls",
	})

	prometheusCounters[DataQualBytes] = promauto.NewCounter(prometheus.CounterOpts{
		Name: DataQualBytes,
		Help: "Number of bytes that have passed through data quality checks",
	})

	prometheusCounters[DataQualMessages] = promauto.NewCounter(prometheus.CounterOpts{
		Name: DataQualMessages,
		Help: "Number of messages that have passed through data quality checks",
	})
}

// IncrPromCounter increments a prometheus counter by the given amount
func IncrPromCounter(key string, amount float64) {
	prometheusMutex.Lock()
	defer prometheusMutex.Unlock()
	_, ok := prometheusCounters[key]
	if ok {
		prometheusCounters[key].Add(amount)
	}
}

// IncrPromGauge decrements a prometheus gauge by 1
func IncrPromGauge(key string) {
	prometheusMutex.Lock()
	defer prometheusMutex.Unlock()
	_, ok := prometheusGauges[key]
	if ok {
		prometheusGauges[key].Inc()
	}
}

// DecrPromGauge decrements a prometheus gauge by 1
func DecrPromGauge(key string) {
	prometheusMutex.Lock()
	defer prometheusMutex.Unlock()
	_, ok := prometheusGauges[key]
	if ok {
		prometheusGauges[key].Dec()
	}
}

// SetPromGauge sets a prometheus gauge value
func SetPromGauge(key string, amount float64) {
	prometheusMutex.Lock()
	defer prometheusMutex.Unlock()

	_, ok := prometheusGauges[key]
	if ok {
		prometheusGauges[key].Set(amount)
	}
}

// Incr increments a counter by the given amount
func Incr(name string, value float64) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := counters[name]; !ok {
		counters[name] = 0
	}

	counters[name] += value
}

// Decr decrements a counter by the given amount
func Decr(name string, value float64) {
	mutex.Lock()
	defer mutex.Unlock()

	counters[name] -= value
}

// Mute stops reporting given stats
func Mute(name string) {
	mutex.Lock()
	defer mutex.Unlock()

	delete(counters, name)
}
