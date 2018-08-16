package schedule

import (
	"math/rand"
	"sync"
	"time"
)

// A Job is something that can be run at a scheduled time
type Job interface {
	Run()
}

// JobFunc adapts a function to the Job interface
type JobFunc func()

func (fn JobFunc) Run() {
	fn()
}

// A IntervalFunc is a function that returns the next time interval
// to be used for running a job. It may return different intervals
// on each call.
type IntervalFunc func() time.Duration

// A CancelFunc is a function that may be used to cancel all future
// executions of a Job
type CancelFunc func()

// Every returns a ScheduleFunc that schedules a job to run
// every d nanoseconds.
func Every(d time.Duration) IntervalFunc {
	return func() time.Duration {
		return d
	}
}

// RoughlyEvery returns a ScheduleFunc that schedules a job to run
// repeatedly at random intervals around ±10% of d
func RoughlyEvery(d time.Duration) IntervalFunc {
	return func() time.Duration {
		return roughDuration(d)
	}
}

// After returns a ScheduleFunc that schedules a job to run
// once after interval d. Note that this is essentially the
// same as time.After, which should be preferred for simple
// use cases.
func After(d time.Duration) IntervalFunc {
	done := false
	return func() time.Duration {
		if done {
			return 0
		}
		done = true
		return d
	}
}

// RoughlyAfter returns a ScheduleFunc that schedules a job to run
// once after a random interval around ±10% of d
func RoughlyAfter(d time.Duration) IntervalFunc {
	done := false
	return func() time.Duration {
		if done {
			return 0
		}
		done = true
		return roughDuration(d)
	}
}

// EveryAfter schedules a job to run every e nanoseconds
// but waits until a has passed before starting the schedule.
// a and e should be > 0
func EveryAfter(e, a time.Duration) IntervalFunc {
	var start bool
	return func() time.Duration {
		if start {
			return e
		}
		start = true
		return a
	}
}

// roughDuration returns a duration that is +/- 5% of the supplied duration
func roughDuration(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.90 + rand.Float64()/5))
}

// Schedule adds job to the default scheduler and returns a function that can be
// used to cancel all future executions of the job.
func Schedule(job Job, sf IntervalFunc) CancelFunc {
	defaultSchedulerMutex.Lock()
	defer defaultSchedulerMutex.Unlock()
	return defaultScheduler.Schedule(job, sf)
}

// CancelAll cancels all scheduled jobs managed by the default scheduler
func CancelAll() {
	defaultSchedulerMutex.Lock()
	defer defaultSchedulerMutex.Unlock()
	defaultScheduler.CancelAll()
}

var (
	defaultScheduler      = &Scheduler{}
	defaultSchedulerMutex sync.Mutex
)

// A Scheduler manages a set of scheduled jobs
type Scheduler struct {
	jobs []*scheduledJob
}

// Schedule schedules job to run after the interval returned by sf. After
// each execution of the job, sf is called to determine the next schedule
// interval. If sf returns zero then the job will not be scheduled to run.
// Schedule returns a function that can be used to cancel all future
// executions of the job.
func (s *Scheduler) Schedule(job Job, sf IntervalFunc) CancelFunc {
	interval := sf()
	if interval == 0 {
		// Nothing to do
		return noop
	}

	sj := &scheduledJob{
		job: job,
		sf:  sf,
	}
	sj.schedule(interval)

	s.jobs = append(s.jobs, sj)
	return func() {
		sj.cancel()
		// Note that sj remains in the jobs slice so wont be garbage collected
		// Expectation is that cancelling is infrequent and/or exceptional
		// behaviour so this won't matter in practice. The job's timer
		// should be garbage collected since we're calling stop on it.
	}
}

// CancelAll cancels all scheduled jobs managed by this scheduler
func (s *Scheduler) CancelAll() {
	for _, sj := range s.jobs {
		sj.cancel()
	}
	s.jobs = []*scheduledJob{}
}

func noop() {}

type scheduledJob struct {
	mu    sync.Mutex
	job   Job
	sf    IntervalFunc
	timer *time.Timer
}

func (sj *scheduledJob) schedule(d time.Duration) {
	if sj == nil {
		return
	}
	sj.mu.Lock()
	sj.timer = time.AfterFunc(d, func() {

		sj.mu.Lock()
		timer := sj.timer
		job := sj.job
		intervalFn := sj.sf
		sj.mu.Unlock()

		if timer == nil {
			// Job was cancelled after we had entered
			// this function and started acquiring
			// lock so don't proceed
			return
		}

		job.Run()
		next := intervalFn()

		if next != 0 {
			sj.schedule(next)
		}
	})
	sj.mu.Unlock()
}

func (sj *scheduledJob) cancel() {
	if sj == nil {
		return
	}
	sj.mu.Lock()
	defer sj.mu.Unlock()
	if sj.timer == nil {
		return
	}
	sj.timer.Stop()
	sj.timer = nil
}
