package gbus

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newLoopTestOutbox(resendOnNack bool, resendsBufferSize int) *AMQPOutbox {
	out := &AMQPOutbox{}
	//build the confirm machinery directly, without a broker channel
	out.stop = make(chan bool, 1)
	out.pending = map[uint64]pendingConfirmation{
		1: {exchange: "e", routingKey: "k", amqpMessage: amqp.Publishing{MessageId: "m1"}},
	}
	out.results = make(map[uint64]chan error)
	out.locker = &sync.Mutex{}
	out.ack = make(chan uint64, 10)
	out.nack = make(chan uint64, 10)
	out.resends = make(chan pendingConfirmation, resendsBufferSize)
	out.resendOnNack = resendOnNack
	return out
}

// G6: a nack arriving while the resends buffer is full must be processed
// (the old code sent to out.resends while holding out.locker, which Post also
// needs, deadlocking the entire publish path once the buffer filled up)
func TestNackWithFullResendsBufferDoesNotDeadlock(t *testing.T) {
	out := newLoopTestOutbox(true, 1)
	out.resends <- pendingConfirmation{amqpMessage: amqp.Publishing{MessageId: "dummy"}} //fill the buffer
	go out.confirmationLoop()
	out.nack <- 1

	deadline := time.Now().Add(2 * time.Second)
	for {
		out.locker.Lock()
		_, stillPending := out.pending[1]
		out.locker.Unlock()
		if !stillPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deadlock: nack not processed while the resends buffer is full")
		}
		time.Sleep(5 * time.Millisecond)
	}
	out.shutdown()
}

// G6: when the resends buffer is full the nacked message is dropped (not
// enqueued and not blocking) with an error log
func TestEnqueueResendDropsWhenBufferFull(t *testing.T) {
	out := newLoopTestOutbox(true, 1)
	dummy := pendingConfirmation{amqpMessage: amqp.Publishing{MessageId: "dummy"}}
	nacked := pendingConfirmation{exchange: "e", routingKey: "k", amqpMessage: amqp.Publishing{MessageId: "m1"}}

	done := make(chan struct{})
	go func() {
		out.enqueueResend(dummy)
		out.enqueueResend(nacked) //buffer is now full: must return immediately, dropping nacked
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueueResend blocked on a full resends buffer")
	}

	got := <-out.resends
	if got.amqpMessage.MessageId != "dummy" {
		t.Fatalf("expected the buffered message to be the dummy, got %q", got.amqpMessage.MessageId)
	}
}

// G7: shutting down an outbox that never started a confirmation loop must not hang
func TestShutdownWithoutConfirmationLoopCompletes(t *testing.T) {
	out := &AMQPOutbox{}
	if err := out.init(nil, false /*confirm*/, true /*resendOnNack*/, 100); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		out.shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown hung when confirms are off and no confirmation loop exists")
	}

	//a second shutdown must be safe too
	out.shutdown()
}
