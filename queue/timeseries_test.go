package queue

import (
	"testing"
	"time"
)

func TestGetStats(t *testing.T) {
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

	t.Logf("%+v", stats)
}