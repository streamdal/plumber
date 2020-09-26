package director

import (
	// "fmt"
	"time"
)

const (
	FOREVER = -1
	ONCE    = 1
)

// A Looper is used in place of a direct call to "for {}" and implements some
// controls over how the loop will be run. The Loop() function is the main call
// used by dependant routines. Common patterns like Quit and Done channels are
// easily implemented in a Looper.
type Looper interface {
	Loop(fn func() error)
	Wait() error
	Done(err error)
	Quit()
}

// A TimedLooper is a Looper that runs on a timed schedule, using a Timer
// underneath. It also implements Quit and Done channels to allow external
// routines to more easily control and synchronize the loop.
//
// If you pass in a DoneChan at creation time, it will send a nil on the
// channel when the loop has completed successfully or an error if the loop
// resulted in an error condition.
type TimedLooper struct {
	Count     int
	Interval  time.Duration
	DoneChan  chan error
	quitChan  chan bool
	Immediate bool
}

func NewTimedLooper(count int, interval time.Duration, done chan error) *TimedLooper {
	return &TimedLooper{
		Count:     count,
		Interval:  interval,
		DoneChan:  done,
		quitChan:  make(chan bool),
		Immediate: false,
	}
}

// Same as a TimedLooper, except it will execute an iteration of the loop
// immediately after calling on Loop() (as opposed to waiting until the tick)
func NewImmediateTimedLooper(count int, interval time.Duration, done chan error) *TimedLooper {
	return &TimedLooper{
		Count:     count,
		Interval:  interval,
		DoneChan:  done,
		quitChan:  make(chan bool),
		Immediate: true,
	}
}

func (l *TimedLooper) Wait() error {
	return <-l.DoneChan
}

// Signal a dependant routine that we're done with our work
func (l *TimedLooper) Done(err error) {
	if l.DoneChan != nil {
		l.DoneChan <- err
	}
}

// The main method of the Looper. This call takes a function with a single
// return value, an error. If the error is nil, the Looper will run the next
// iteration. If it's an error, it will not run the next iteration, will clean
// up any internals that need to be, and will invoke done().
func (l *TimedLooper) Loop(fn func() error) {
	i := 0

	var stop bool
	stopFunc := func(err error) {
		l.Done(err)
		stop = true
	}

	runIteration := func() {
		err := fn()
		if err != nil {
			stopFunc(err)
			return
		}

		// We have to make sure not to increment if we started
		// at -1 otherwise we quit on maxint rollover.
		if l.Count != FOREVER {
			i = i + 1
			if i >= l.Count {
				stopFunc(nil)
				return
			}
		}
	}

	// Immediatelly run our function if we've been instantiated via
	// NewImmediateTimedLooper
	if l.Immediate {
		runIteration()
	}

	ticker := time.NewTicker(l.Interval)
	defer ticker.Stop()
	for {
		// The execution loop needs to be able to stop automatically after
		// l.Count iterations. It does so when runIteration invokes stopFunc,
		// which sets `stop` to false.
		if stop {
			break
		}

		select {
		case <-ticker.C:
			runIteration()
		case <-l.quitChan:
			stopFunc(nil)
			break
		}
	}
}

// Quit() signals to the Looper to not run the next iteration and to call
// done() and return as quickly as possible. It is does not intervene between
// iterations.
func (l *TimedLooper) Quit() {
	go func() {
		l.quitChan <- true
	}()
}

// A FreeLooper is like a TimedLooper but doesn't wait between iterations.
type FreeLooper struct {
	Count    int
	DoneChan chan error
	quitChan chan bool
}

func NewFreeLooper(count int, done chan error) *FreeLooper {
	return &FreeLooper{
		Count:    count,
		DoneChan: done,
		quitChan: make(chan bool),
	}
}

func (l *FreeLooper) Wait() error {
	return <-l.DoneChan
}

// This is used internally, but can also be used by controlling routines to
// signal that a job is completed. The FreeLooper doesn's support its use
// outside the internals.
func (l *FreeLooper) Done(err error) {
	if l.DoneChan != nil {
		l.DoneChan <- err
	}
}

func (l *FreeLooper) Loop(fn func() error) {
	i := 0

	for {
		err := fn()
		if err != nil {
			l.Done(err)
			return
		}

		// We have to make sure not to increment if we started at -1 otherwise
		// we quit on maxint rollover.
		if l.Count != FOREVER {
			i = i + 1
			if i >= l.Count {
				l.Done(nil)
				return
			}
		}

		select {
		case <-l.quitChan:
			l.Done(nil)
			return
		default:
		}
	}
}

// Quit() signals to the Looper to not run the next iteration and to call
// Done() and return as quickly as possible. It is does not intervene between
// iterations. It is a non-blocking operation.
func (l *FreeLooper) Quit() {
	go func() {
		l.quitChan <- true
	}()
}
