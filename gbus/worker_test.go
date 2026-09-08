package gbus

import (
	"sync"
	"testing"
)

// Stop must be safe to call any number of times, from any number of goroutines.
// It used to close worker.stop unconditionally, so a second Stop (graceful
// shutdown racing the connection-loss supervisor) panicked with
// "close of closed channel" and took the process down.
func TestWorkerStopIsIdempotent(t *testing.T) {
	w := &worker{
		stop:    make(chan bool),
		stopped: make(chan struct{}),
	}
	go func() {
		<-w.stop
		close(w.stopped)
	}()

	const callers = 8
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Stop(); err != nil {
				t.Errorf("Stop returned error: %v", err)
			}
		}()
	}
	wg.Wait()
}

// A worker whose Start never completed (e.g. the broker died mid-start) has a
// nil stop channel; Stop used to panic with "close of nil channel".
func TestWorkerStopWithUnstartedWorkerDoesNotPanic(t *testing.T) {
	w := &worker{}
	if err := w.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}
