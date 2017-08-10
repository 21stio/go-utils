package queue

import (
	"time"
	"github.com/21stio/go-utils/statistics"
)

type DataPoint struct {
	TimePoint time.Time
	Value     float64
}

func NewTimeSeries(max int) (q TimeSeries) {
	fifo := NewFixedFifo(max)

	q = TimeSeries{
		fifo: &fifo,
	}

	return
}

type TimeSeries struct {
	fifo *FixedFifo
}

func (q *TimeSeries) Push(t time.Time, n float64) {
	v := DataPoint{
		TimePoint: t,
		Value:     n,
	}

	q.fifo.Push(v)

	return
}

func (q *TimeSeries) GetDataPoints(start time.Time, end time.Time) (dataPoints []DataPoint) {
	rawDataPoints := q.fifo.GetValues()

	for _, val := range rawDataPoints {
		val, _ := val.(DataPoint)
		if val.TimePoint.After(start) && val.TimePoint.Before(end) {
			dataPoints = append(dataPoints, val)
		}
	}

	return dataPoints
}

func (q *TimeSeries) GetStats(start time.Time, end time.Time) (stats statistics.Stats, err error) {
	dataPoints := q.GetDataPoints(start, end)

	values := []float64{}
	for _, val := range dataPoints {
		values = append(values, val.Value)
	}

	stats, err = statistics.GetStats(values)

	return
}