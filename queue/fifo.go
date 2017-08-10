package queue

import (
	"sync"
)

func NewFixedFifo(max int) (q FixedFifo) {
	q = FixedFifo{
		max: max,
	}

	return
}

type FixedFifo struct {
	lock   sync.Mutex
	values []interface{}
	max    int
}

func (q *FixedFifo) Push(value interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.Len() >= q.max {
		q.lock.Unlock()
		q.Pop()
		q.lock.Lock()
	}

	q.values = append(q.values, value)

	return
}

func (q *FixedFifo) GetValues() (values []interface{}) {
	return q.values
}

func (q *FixedFifo) Pop() (n interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()

	n = (q.values)[0]
	q.values = (q.values)[1:]
	return
}

func (q *FixedFifo) Len() int {
	return len(q.values)
}
