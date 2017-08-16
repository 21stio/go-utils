package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/21stio/go-utils/testing2"
)

func TestGetStats(t *testing.T) {
	testBag := testing2.TestBag{}

	ts := NewTimeSeries(100)

	now := time.Now()

	ts.Push(now.Add(-0 * time.Millisecond), 0)
	ts.Push(now.Add(-1 * time.Millisecond), 1)
	ts.Push(now.Add(-2 * time.Millisecond), 2)
	ts.Push(now.Add(-3 * time.Millisecond), 3)
	ts.Push(now.Add(-4 * time.Millisecond), 4)
	ts.Push(now.Add(-5 * time.Millisecond), 5)
	ts.Push(now.Add(-6 * time.Millisecond), 6)
	ts.Push(now.Add(-7 * time.Millisecond), 7)
	ts.Push(now.Add(-8 * time.Millisecond), 8)
	ts.Push(now.Add(-9 * time.Millisecond), 9)

	stats, err := ts.GetStats(now.Add(-100 * time.Hour), now.Add(100 * time.Hour))
	if err != nil {
		t.Error(err)

		return
	}

	testBag.AddResult(assert.Equal(t, float64(4.5), stats.Average))
	testBag.AddResult(assert.Equal(t, float64(4.5), stats.Median))
	testBag.AddResult(assert.Equal(t, float64(45), stats.Sum))
	testBag.AddResult(assert.Equal(t, float64(0), stats.Min))
	testBag.AddResult(assert.Equal(t, float64(9), stats.Max))
	testBag.AddResult(assert.Equal(t, uint64(10), stats.Count))

	t.Logf("%+v", stats)

	if testBag.HasFailed() {
		t.Fail()
	}
}