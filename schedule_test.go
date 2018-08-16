package schedule

import (
	"testing"
	"time"
)

// SignallingJob is an implementation of Job that sends on a channel when
// its Run method is called.
type signallingJob struct {
	C chan bool
	t *testing.T
}

func (s *signallingJob) Run() {
	s.t.Logf("run called")
	s.C <- true
}

func newSignallingJob(t *testing.T) *signallingJob {
	return &signallingJob{
		C: make(chan bool),
		t: t,
	}
}

func TestSchedule(t *testing.T) {
	s := newSignallingJob(t)

	Schedule(s, After(1*time.Millisecond))
	count := 0

	select {
	case <-s.C:
		count++
	case <-time.After(10 * time.Millisecond):
		t.Logf("timed out")
	}

	if count != 1 {
		t.Errorf("wanted %d, got %d", 1, count)
	}
}

func TestCancelJob(t *testing.T) {
	s := newSignallingJob(t)

	cancelFn := Schedule(s, Every(1*time.Millisecond))
	count := 0

loop:
	for {
		select {
		case <-s.C:
			t.Logf("count was %d, incrementing to %d", count, count+1)
			count++
			if count == 3 {
				t.Logf("cancelling")
				cancelFn()
			}
			if count > 3 {
				break loop
			}
		case <-time.After(10 * time.Millisecond):
			t.Logf("breaking out of loop due to timeout")
			break loop
		}
	}
	if count > 3 {
		t.Errorf("wanted count to remain at 3, got %d", count)
	}
}

func TestRoughDuration(t *testing.T) {
	for i := 0; i < 100000; i++ {
		d := roughDuration(10 * time.Minute)
		if d < 9*time.Minute || d > 11*time.Minute {
			t.Fatalf("got duration out of range: %s", d)
		}
	}
}
