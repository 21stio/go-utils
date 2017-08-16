package sync2

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