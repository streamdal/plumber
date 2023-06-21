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
)

var (
	ReportInterval = 10 * time.Second

	mutex    = &sync.RWMutex{}
	counters = make(map[string]float64, 0)

	prometheusMutex       = &sync.RWMutex{}
	prometheusCounters    = make(map[string]prometheus.Counter)
	prometheusVecCounters = make(map[string]*prometheus.CounterVec)
	prometheusGauges      = make(map[string]prometheus.Gauge)

	looper director.Looper
)

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

	prometheusVecCounters[VecCounterName("dataqual", "publish")] = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "plumber",
		Subsystem: "dataqual",
		Name:      "publish",
		Help:      "Number of messages published",
	}, []string{"data_source", "type"})

	prometheusVecCounters[VecCounterName("dataqual", "consume")] = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "plumber",
		Subsystem: "dataqual",
		Name:      "consume",
		Help:      "Number of messages published",
	}, []string{"data_source", "type"})

	prometheusVecCounters[VecCounterName("dataqual", "size_exceeded")] = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "plumber",
		Subsystem: "dataqual",
		Name:      "size_exceeded",
		Help:      "Number of messages that were ignored by rules because the payload is too large",
	}, []string{"data_source"})

	prometheusVecCounters[VecCounterName("dataqual", "rule")] = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "plumber",
		Subsystem: "dataqual",
		Name:      "rule",
		Help:      "Message count and bytes by rule set and rule",
	}, []string{"rule_id", "ruleset_id", "type"})

	prometheusVecCounters[VecCounterName("dataqual", "failure_trigger")] = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "plumber",
		Subsystem: "dataqual",
		Name:      "failure_trigger",
		Help:      "Number of events/bytes that triggered a failure rule, for each failure rule type",
	}, []string{"rule_id", "ruleset_id", "type", "failure_mode"})
}

// IncrPromCounter increments a prometheus counter by the given amount
func IncrPromCounter(key string, amount float64) {
	key = strings.Replace(key, "-", "_", -1)
	prometheusMutex.RLock()
	defer prometheusMutex.RUnlock()
	c, ok := prometheusCounters[key]
	if !ok {
		prometheusCounters[key] = promauto.NewCounter(prometheus.CounterOpts{
			Name: key,
			Help: "Auto-created counter",
		})
	}

	c.Add(amount)
}

func VecCounterName(subsystem, name string) string {
	return subsystem + "_" + name
}

func GetVecCounter(subsystem, name string) *prometheus.CounterVec {
	prometheusMutex.Lock()
	defer prometheusMutex.Unlock()
	c, ok := prometheusVecCounters[VecCounterName(subsystem, name)]
	if ok {
		return c
	}

	return nil
}

// IncrPromGauge decrements a prometheus gauge by 1
func IncrPromGauge(key string) {
	prometheusMutex.Lock()
	defer prometheusMutex.Unlock()

	if _, ok := prometheusGauges[key]; !ok {
		prometheusGauges[key] = promauto.NewGauge(prometheus.GaugeOpts{
			Name: key,
			Help: "Auto-created gauge",
		})
	}

	prometheusGauges[key].Inc()
}

// DecrPromGauge decrements a prometheus gauge by 1
func DecrPromGauge(key string) {
	prometheusMutex.RLock()
	defer prometheusMutex.RUnlock()
	c, ok := prometheusGauges[key]
	if ok {
		c.Dec()
	}
}

// SetPromGauge sets a prometheus gauge value
func SetPromGauge(key string, amount float64) {
	prometheusMutex.RLock()
	defer prometheusMutex.RUnlock()

	c, ok := prometheusGauges[key]
	if ok {
		c.Set(amount)
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
