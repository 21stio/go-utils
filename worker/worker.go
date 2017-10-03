package worker

import (
	"sync"
	"github.com/21stio/go-utils/sync2"
	"github.com/21stio/go-utils/queue"
	"time"
	"github.com/21stio/go-utils/statistics"
	"math"
	"github.com/rs/zerolog"
	"sync/atomic"
)

type PoolStats struct {
	Err              statistics.Stats
	NewTasks         statistics.Stats
	TasksDuration    statistics.Stats
	WorkerCount      int64
	TasksQueueLength int64
}

const (
	WORKER_STRATEGY string = "worker"
	TASKS_STRATEGY         = "tasks"
)

func New(log zerolog.Logger, isLogStats bool, interval time.Duration, function func(zerolog.Logger, interface{}) (error)) (pool Pool) {
	pool.log = log
	pool.function = function
	pool.tasksChan = make(chan interface{}, 100)
	pool.errChan = make(chan error, 100)
	pool.quitChan = make(chan bool, 100)
	pool.errTimeSeries = queue.NewTimeSeries(1000)
	pool.tasksDurationTimeSeries = queue.NewTimeSeries(1000)
	pool.newTasksTimeSeries = queue.NewTimeSeries(1000)
	pool.waitGroup = &sync2.CountWG{}

	go pool.loopLogStats(isLogStats, interval)

	return
}

type Pool struct {
	log                     zerolog.Logger
	scaleStrategy           string
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
	waitGroup               *sync2.CountWG
	function                func(zerolog.Logger, interface{}) (error)
	scaleLock               sync.Mutex
	nextWorkerId            int64
}

func (p *Pool) GetWorkerCount() (int64) {
	pkg(p.log.Debug(), "Pool.GetWorkerCount").
		Msg("")

	return p.waitGroup.Count
}

func (p *Pool) loopLogStats(isLogStats bool, interval time.Duration) {
	pkg(p.log.Debug(), "Pool.loopLogStats").
		Bool("isLogStats", isLogStats).
		Dur("interval", interval).
		Msg("")

	for isLogStats {
		p.logStats(interval)

		time.Sleep(interval)
	}
}

func (p *Pool) logStats(interval time.Duration) {
	pkg(p.log.Debug(), "Pool.logStats").
		Msg("")

	stats, err := p.GetStats(interval)
	if err != nil {
		pkg(p.log.Error(), "Pool.logStats").
			Err(err)
	}

	pkg(p.log.Info(), "logStats").
		Dur("interval", interval).
		Str("type", "Err").
		Interface("stats", stats.Err).
		Msg("")

	pkg(p.log.Info(), "logStats").
		Dur("interval", interval).
		Str("type", "NewTasks").
		Interface("stats", stats.NewTasks).
		Msg("")

	pkg(p.log.Info(), "logStats").
		Dur("interval", interval).
		Str("type", "TasksDuration").
		Interface("stats", stats.TasksDuration).
		Msg("")

	pkg(p.log.Info(), "logStats").
		Dur("interval", interval).
		Str("type", "WorkerCount").
		Int64("count", stats.WorkerCount).
		Msg("")

	pkg(p.log.Info(), "logStats").
		Dur("interval", interval).
		Str("type", "TasksQueueLength").
		Int64("count", stats.TasksQueueLength).
		Msg("")
}

func (p *Pool) StartWorker() {
	pkg(p.log.Debug(), "StartWorker").
		Msg("")

	go p.startWorker()
}

func (p *Pool) startWorker() {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()

	id := atomic.AddInt64(&p.nextWorkerId, 1)

	log := p.log.With().Int64("workerId", id).Logger()

	for {
		select {
		case task, ok := <-p.tasksChan:
			if !ok {
				return
			}
			start := time.Now()

			err := p.function(log, task)
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

func (p *Pool) StopWorker() {
	pkg(p.log.Debug(), "Pool.StopWorker").
		Msg("")

	p.quitChan <- true
}

func (p *Pool) AddTask(task interface{}) {
	pkg(p.log.Debug(), "Pool.AddTask").
		Msg("")

	p.tasksChan <- task
	p.newTasksTimeSeries.Push(time.Now(), 1)
}

func (p *Pool) GetStats(duration time.Duration) (stats PoolStats, err error) {
	pkg(p.log.Debug(), "Pool.GetStats").
		Msg("")

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

	stats.WorkerCount = p.GetWorkerCount()
	stats.TasksQueueLength = p.GetTasksQueueLength()

	return
}

func (p *Pool) ScaleWorker(desired int64) {
	pkg(p.log.Info(), "Pool.ScaleWorker").
		Int64("desired", desired).
		Msg("")

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

func (p *Pool) SetStrategy(strategy string) {
	pkg(p.log.Debug(), "Pool.SetStrategy").
		Str("strategy", strategy).
		Msg("")

	p.scaleStrategy = strategy
}

func (p *Pool) ScaleTasksPer(duration time.Duration, desired int64) (err error) {
	pkg(p.log.Debug(), "Pool.ScaleTasksPer").
		Dur("duration", duration).
		Int64("desired", desired).
		Msg("")

	p.desiredTasks = desired
	p.desiredTasksPerDuration = duration
	p.scaleStrategy = TASKS_STRATEGY

	if p.applyLoopStarted == false {
		go p.loopApplyScaleTasksPerMinute()
		p.applyLoopStarted = true
	}

	return
}

func (p *Pool) loopApplyScaleTasksPerMinute() {
	pkg(p.log.Debug(), "Pool.loopApplyScaleTasksPerMinute").
		Msg("")

	for true {
		p.applyScaleTasksPerMinute()
		time.Sleep(5 * time.Second)
	}
}

func (p *Pool) applyScaleTasksPerMinute() (err error) {
	pkg(p.log.Debug(), "Pool.applyScaleTasksPerMinute").
		Msg("")

	stats, err := p.GetStats(1 * time.Minute)
	if err != nil {
		return
	}

	averageTaskDuration := float64(1)
	if stats.TasksDuration.Average != float64(0) && !math.IsNaN(stats.TasksDuration.Average) {
		averageTaskDuration = stats.TasksDuration.Average
	}

	workerCount, taskWaitDuration := calculateParameters(p.log, p.desiredTasksPerDuration, p.desiredTasks, time.Duration(averageTaskDuration*1000*1000)*time.Microsecond)

	pkg(p.log.Debug(), "Pool.applyScaleTasksPerMinute").
		Int64("workerCount", workerCount).
		Dur("taskWaitDuration", taskWaitDuration).
		Msg("")

	p.taskWaitDuration = taskWaitDuration

	if workerCount != p.GetWorkerCount() {
		p.ScaleWorker(workerCount)
	}

	return
}

func (p *Pool) GetTasksQueueLength() (int64) {
	return int64(len(p.tasksChan))
}

func calculateParameters(log zerolog.Logger, desiredTasksPerDuration time.Duration, desiredTasksPer int64, averageTaskDuration time.Duration) (workerCount int64, waitDuration time.Duration) {
	pkg(log.Debug(), "calculateParameters").
		Int64("desiredTasksPer", desiredTasksPer).
		Dur("desiredTasksPerDuration", desiredTasksPerDuration).
		Dur("averageTaskDuration", averageTaskDuration).
		Msg("")

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
