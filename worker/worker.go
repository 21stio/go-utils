package worker

import (
	"sync"
	"github.com/21stio/go-utils/sync2"
	"github.com/21stio/go-utils/queue"
	"time"
	"github.com/21stio/go-utils/statistics"
	"math"
	"github.com/21stio/go-utils/log2"
)

var pkg = "github.com/21stio/go-utils/worker"

type PoolStats struct {
	Err           statistics.Stats
	NewTasks      statistics.Stats
	TasksDuration statistics.Stats
}

type ScaleStrategy float64

const (
	WORKER_STRATEGY ScaleStrategy = iota
	TASKS_STRATEGY
)

func (p *Pool) startWorker() {
	log2.Log(log2.DEBUG, pkg, "Worker.startWorker", "", log2.Fields{})

	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	for {
		select {
		case task, ok := <-p.tasksChan:
			if !ok {
				return
			}
			start := time.Now()

			err := p.function(task)
			if err != nil {
				p.errChan <- err
				p.errTimeSeries.Push(time.Now(), 1)
			}

			duration := time.Now().Sub(start).Seconds()

			p.tasksDurationTimeSeries.Push(time.Now(), duration)

			if p.scaleStrategy == TASKS_STRATEGY {
				time.Sleep(p.taskWaitDuration)
			}
		case <-p.quitChan:
			return
		}
	}
}

func New(function func(interface{}) (error)) (pool Pool) {
	pool.function = function
	pool.tasksChan = make(chan interface{}, 100)
	pool.errChan = make(chan error, 100)
	pool.quitChan = make(chan bool, 100)
	pool.errTimeSeries = queue.NewTimeSeries(1000)
	pool.tasksDurationTimeSeries = queue.NewTimeSeries(1000)
	pool.newTasksTimeSeries = queue.NewTimeSeries(1000)

	return
}

type Pool struct {
	scaleStrategy           ScaleStrategy
	desiredTasks            int64
	desiredTasksPerDuration time.Duration
	taskWaitDuration        time.Duration
	applyLoopStarted        bool
	tasksChan               chan interface{}
	errChan                 chan error
	quitChan                chan bool
	errTimeSeries           *queue.TimeSeries
	tasksDurationTimeSeries *queue.TimeSeries
	newTasksTimeSeries      *queue.TimeSeries
	waitGroup               sync2.CountWG
	function                func(interface{}) (error)
	scaleLock               sync.Mutex
}

func (p *Pool) GetWorkerCount() (int64) {
	log2.Log(log2.DEBUG, pkg, "Worker.GetWorkerCount", "", log2.Fields{})

	return p.waitGroup.Count
}

func (p *Pool) StartWorker() {
	log2.Log(log2.DEBUG, pkg, "Worker.StartWorker", "", log2.Fields{})

	go p.startWorker()
}

func (p *Pool) StopWorker() {
	log2.Log(log2.DEBUG, pkg, "Worker.StopWorker", "", log2.Fields{})

	p.quitChan <- true
}

func (p *Pool) AddTask(task interface{}) {
	log2.Log(log2.DEBUG, pkg, "Worker.AddTask", "", log2.Fields{})

	p.tasksChan <- task
	p.newTasksTimeSeries.Push(time.Now(), 1)
}

func (p *Pool) GetStats(duration time.Duration) (stats PoolStats, err error) {
	log2.Log(log2.DEBUG, pkg, "Worker.GetStats", "", log2.Fields{})

	end := time.Now()
	start := end.Add(-duration)

	stats.Err, err = p.errTimeSeries.GetStats(start, end)
	if err != nil {
		return
	}

	stats.TasksDuration, err = p.tasksDurationTimeSeries.GetStats(start, end)
	if err != nil {
		return
	}

	stats.NewTasks, err = p.newTasksTimeSeries.GetStats(start, end)
	if err != nil {
		return
	}

	return
}

func (p *Pool) ScaleWorker(desired int64) {
	log2.Log(log2.DEBUG, pkg, "Worker.ScaleWorker", "", log2.Fields{"desired": desired})

	p.scaleLock.Lock()
	defer p.scaleLock.Unlock()

	diff := desired - p.GetWorkerCount()

	absoluteDiff := diff
	if absoluteDiff < 0 {
		absoluteDiff = -absoluteDiff
	}

	negativeDiff := diff < 0
	for i := int64(1); i <= absoluteDiff; i++ {
		if negativeDiff {
			p.StopWorker()

			continue
		}

		p.StartWorker()
	}
}

func (p *Pool) SetStrategy(strategy ScaleStrategy) {
	log2.Log(log2.DEBUG, pkg, "Worker.SetStrategy", "", log2.Fields{})

	p.scaleStrategy = strategy
}

func (p *Pool) ScaleTasksPer(duration time.Duration, desired int64) (err error) {
	log2.Log(log2.DEBUG, pkg, "Worker.ScaleTasksPerMinute", "", log2.Fields{})

	p.desiredTasks = desired
	p.desiredTasksPerDuration = duration
	p.scaleStrategy = TASKS_STRATEGY

	if p.applyLoopStarted == false {
		go p.startApplyLoop()
		p.applyLoopStarted = true
	}

	return
}

func (p *Pool) startApplyLoop() {
	log2.Log(log2.DEBUG, pkg, "Worker.startApplyLoop", "", log2.Fields{})

	for true {
		p.applyScaleTasksPerMinute()
		time.Sleep(5 * time.Second)
	}
}

func (p *Pool) applyScaleTasksPerMinute() (err error) {
	log2.Log(log2.DEBUG, pkg, "Worker.applyScaleTasksPerMinute", "", log2.Fields{})

	stats, err := p.GetStats(1 * time.Minute)
	if err != nil {
		return
	}

	averageTaskDuration := float64(1)
	if stats.TasksDuration.Average != float64(0) && !math.IsNaN(stats.TasksDuration.Average) {
		averageTaskDuration = stats.TasksDuration.Average
	}

	workerCount, taskWaitDuration := calculateParameters(p.desiredTasksPerDuration, p.desiredTasks, time.Duration(averageTaskDuration*1000*1000)*time.Microsecond)

	log2.Log(log2.DEBUG, pkg, "Worker.applyScaleTasksPerMinute", "", log2.Fields{
		"workerCount":      workerCount,
		"taskWaitDuration": taskWaitDuration,
	})

	p.taskWaitDuration = taskWaitDuration

	p.ScaleWorker(workerCount)

	return
}

func (p *Pool) GetTasksQueueLength() (int) {
	return len(p.tasksChan)
}

func calculateParameters(desiredTasksPerDuration time.Duration, desiredTasksPer int64, averageTaskDuration time.Duration) (workerCount int64, waitDuration time.Duration) {
	log2.Log(log2.DEBUG, pkg, "calculateParameters", "", log2.Fields{
		"desiredTasks":            desiredTasksPer,
		"desiredTasksPerDuration": desiredTasksPerDuration,
		"averageTaskDuration":     averageTaskDuration,
	})

	taskDurationSum := time.Duration(desiredTasksPer) * averageTaskDuration

	a := float64(taskDurationSum) / desiredTasksPerDuration.Seconds() / float64(time.Second)
	b := math.Ceil(a)
	c := int64(b)
	workerCount = c

	workerDurationSum := time.Duration(workerCount) * desiredTasksPerDuration
	durationDiff := workerDurationSum - taskDurationSum

	waitDuration = durationDiff / time.Duration(desiredTasksPer)

	return
}
