package worker

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/21stio/go-utils/testing2"
	"github.com/21stio/go-utils/log2"
	"fmt"
	"k8s.io/kubernetes/pkg/util/rand"
)

func TestMain(m *testing.M) {
	go log2.HandleEntries(log2.DEBUG, map[string]map[string]bool{
		"github.com/21stio/go-utils/worker": {
			"Worker.AddTask": true,
			"Worker.GetStats": true,
		},
	}, log2.Print)

	m.Run()
}

func produce(pool Pool) {
	for true {
		pool.AddTask("a")
		time.Sleep(100 * time.Millisecond)
	}
}

func report(t *testing.T, pool Pool) {
	for true {
		stats, err := pool.GetStats(10 * time.Second)
		if err != nil {
			t.Error(err)

			return
		}

		log2.Log(log2.DEBUG, pkg, "report", fmt.Sprintf("%+v", stats.TasksDuration), log2.Fields{})

		time.Sleep(2000 * time.Millisecond)
	}
}

func TestPool(t *testing.T) {
	testBag := testing2.TestBag{}

	pool := New(func(i interface{}) error {
		time.Sleep(time.Duration(rand.IntnRange(100, 1000)) * time.Millisecond)

		return nil
	})

	go produce(pool)
	go report(t, pool)


	pool.ScaleTasksPer(10 * time.Second, 20)
	time.Sleep(15 * time.Second)
	stats, err := pool.GetStats(10 * time.Second)
	if err != nil {
		t.Error(err)
	}
	testBag.AddResult(assert.InDelta(t, 20, stats.TasksDuration.Count, 20 * 0.1))


	pool.ScaleTasksPer(10 * time.Second, 30)
	time.Sleep(15 * time.Second)
	stats, err = pool.GetStats(10 * time.Second)
	if err != nil {
		t.Error(err)
	}
	testBag.AddResult(assert.InDelta(t, 30, stats.TasksDuration.Count, 30 * 0.1))


	pool.ScaleTasksPer(10 * time.Second, 10)
	time.Sleep(15 * time.Second)
	stats, err = pool.GetStats(10 * time.Second)
	if err != nil {
		t.Error(err)
	}
	testBag.AddResult(assert.InDelta(t, 10, stats.TasksDuration.Count, 10 * 0.1))
}

func TestCalculateParameter(t *testing.T) {
	testBag := testing2.TestBag{}


	desiredTasksPerMinute := int64(10)
	averageTaskDuration := 10 * time.Second

	expectedWorkerCount := int64(2)
	expectedWaitDuration := 2 * time.Second

	actualWorkerCount, actualWaitDuration := calculateParameters(60*time.Second, desiredTasksPerMinute, averageTaskDuration)

	testBag.AddResult(assert.Equal(t, expectedWorkerCount, actualWorkerCount))
	testBag.AddResult(assert.Equal(t, expectedWaitDuration, actualWaitDuration))


	desiredTasksPerMinute = int64(5)
	averageTaskDuration = 5 * time.Second

	expectedWorkerCount = int64(1)
	expectedWaitDuration = 7 * time.Second

	actualWorkerCount, actualWaitDuration = calculateParameters(60*time.Second, desiredTasksPerMinute, averageTaskDuration)

	testBag.AddResult(assert.Equal(t, expectedWorkerCount, actualWorkerCount))
	testBag.AddResult(assert.Equal(t, expectedWaitDuration, actualWaitDuration))


	desiredTasksPerMinute = int64(100)
	averageTaskDuration = 600 * time.Millisecond

	expectedWorkerCount = int64(1)
	expectedWaitDuration = 0 * time.Second

	actualWorkerCount, actualWaitDuration = calculateParameters(60*time.Second, desiredTasksPerMinute, averageTaskDuration)

	testBag.AddResult(assert.Equal(t, expectedWorkerCount, actualWorkerCount))
	testBag.AddResult(assert.Equal(t, expectedWaitDuration, actualWaitDuration))


	if testBag.HasFailed() {
		t.Fail()
	}
}