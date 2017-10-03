package tracing

import (
	"time"
	"sync"
	"github.com/21stio/go-utils/statistics"
	"github.com/rs/zerolog"
)

var dataPoints = make(map[string][]float64)

var lock = sync.Mutex{}

func Trace(label string, start time.Time) {
	lock.Lock()
	defer lock.Unlock()
	stats, ok := dataPoints[label]
	if !ok {
		stats = []float64{}
	}

	stats = append(stats, float64(time.Now().Sub(start).Seconds()))

	dataPoints[label] = stats
}

func GetTraceStats() (stats map[string]statistics.Stats, err error) {
	stats = map[string]statistics.Stats{}
	for key, value := range dataPoints {
		stat, err_i := statistics.GetStats(value)
		if err_i != nil {
			err = err_i
			return
		}
		stats[key] = stat
	}

	return
}

func FlushDatapoints() () {
	dataPoints = make(map[string][]float64)
}

func LoopLogStats(log zerolog.Logger, interval time.Duration) {
	for true {
		time.Sleep(interval)

		logStats(log, interval)
	}
}

func logStats(log zerolog.Logger, interval time.Duration) {
	lock.Lock()
	defer lock.Unlock()

	stats, err := GetTraceStats()
	if err != nil {
		pkg(log.Error(), "LoopLogStats").
			Err(err).
			Msg("")

		return
	}
	FlushDatapoints()

	pkg(log.Info(), "LoopLogStats").
		Dur("interval", interval).
		Interface("stats", stats).
		Msg("")
}
