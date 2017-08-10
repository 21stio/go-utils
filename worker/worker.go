package worker

import (
	"sync"
	"sync/atomic"
)

type CountWG struct {
	sync.WaitGroup
	Count int64
}

func (cg *CountWG) Add(delta int) {
	atomic.AddInt64(&cg.Count, 1)
	cg.WaitGroup.Add(delta)
}

func (cg *CountWG) Done() {
	atomic.AddInt64(&cg.Count, -1)
	cg.WaitGroup.Done()
}

func startWorker(tasks <-chan interface{}, quit <-chan bool, errChan chan<- error, wg *CountWG, function func(interface{}) (error)) {
	wg.Add(1)
	defer wg.Done()
	for {
		select {
		case task, ok := <-tasks:
			if !ok {
				return
			}
			err := function(task)
			if err != nil {
				errChan <- err
			}
		case <-quit:
			return
		}
	}
}

func New(function func(interface{}) (error)) (pool Pool) {
	pool.function = function
	pool.tasksChan = make(chan interface{}, 100)
	pool.errChan = make(chan error, 100)
	pool.quitChan = make(chan bool, 100)
	pool.startChan = make(chan bool, 100)

	return
}

type Pool struct {
	tasksChan chan interface{}
	errChan   chan error
	quitChan  chan bool
	startChan chan bool
	waitGroup CountWG
	function  func(interface{}) (error)
	scaleLock sync.Mutex
}

func (p *Pool) GetWorkerCount() (int64) {
	return p.waitGroup.Count
}

func (p *Pool) StartWorker() {
	go startWorker(p.tasksChan, p.quitChan, p.errChan, &p.waitGroup, p.function)
}

func (p *Pool) StopWorker() {
	p.quitChan <- true
}

func (p *Pool) AddTask(task interface{}) {
	p.tasksChan <- task
}

func (p *Pool) Scale(to int64) {
	p.scaleLock.Lock()
	defer p.scaleLock.Unlock()

	diff := to - p.GetWorkerCount()

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