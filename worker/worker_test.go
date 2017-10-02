package worker

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/21stio/go-utils/testing2"
	"github.com/rs/zerolog"
	"os"
	"sync"
	"fmt"
)

var log zerolog.Logger

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	zerolog.LevelFieldName = "l"
	zerolog.TimestampFieldName = "t"

	log = zerolog.New(os.Stdout)

	m.Run()
}

func produce(pool Pool) {
	for true {
		pool.AddTask("a")
		time.Sleep(100 * time.Nanosecond)
	}
}

func TestPool(t *testing.T) {
	testBag := testing2.TestBag{}

	per := 10 * time.Second

	wg := &sync.WaitGroup{}

	for i, taskCount := range []float64{1, 2, 3, 4, 5} {
		wg.Add(1)

		go func(i int, count float64, wg *sync.WaitGroup) {
			llog := log.With().
				Str("pool", fmt.Sprintf("test%v", i)).
				Logger()

			pool := New(llog, true, per, func(log zerolog.Logger, i interface{}) error {
				time.Sleep(time.Duration(500 * time.Millisecond))

				log.Info().Msg("")

				return nil
			})

			go produce(pool)

			pool.ScaleTasksPer(per, int64(count))
			time.Sleep(10 * time.Second)
			stats, err := pool.GetStats(per)
			if err != nil {
				t.Error(err)
			}
			testBag.AddResult(assert.InDelta(t, count, stats.TasksDuration.Count, count*0.01))

			wg.Done()
		}(i, taskCount, wg)
	}

	wg.Wait()
}



func TestCalculateParameter(t *testing.T) {
	testBag := testing2.TestBag{}

	desiredTasksPerMinute := int64(10)
	averageTaskDuration := 10 * time.Second

	expectedWorkerCount := int64(2)
	expectedWaitDuration := 2 * time.Second

	actualWorkerCount, actualWaitDuration := calculateParameters(log, 60*time.Second, desiredTasksPerMinute, averageTaskDuration)

	testBag.AddResult(assert.Equal(t, expectedWorkerCount, actualWorkerCount))
	testBag.AddResult(assert.Equal(t, expectedWaitDuration, actualWaitDuration))

	desiredTasksPerMinute = int64(5)
	averageTaskDuration = 5 * time.Second

	expectedWorkerCount = int64(1)
	expectedWaitDuration = 7 * time.Second

	actualWorkerCount, actualWaitDuration = calculateParameters(log, 60*time.Second, desiredTasksPerMinute, averageTaskDuration)

	testBag.AddResult(assert.Equal(t, expectedWorkerCount, actualWorkerCount))
	testBag.AddResult(assert.Equal(t, expectedWaitDuration, actualWaitDuration))

	desiredTasksPerMinute = int64(100)
	averageTaskDuration = 600 * time.Millisecond

	expectedWorkerCount = int64(1)
	expectedWaitDuration = 0 * time.Second

	actualWorkerCount, actualWaitDuration = calculateParameters(log, 60*time.Second, desiredTasksPerMinute, averageTaskDuration)

	testBag.AddResult(assert.Equal(t, expectedWorkerCount, actualWorkerCount))
	testBag.AddResult(assert.Equal(t, expectedWaitDuration, actualWaitDuration))

	if testBag.HasFailed() {
		t.Fail()
	}
}
