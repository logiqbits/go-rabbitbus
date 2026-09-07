package tests

import (
	"testing"
	"time"

	"github.com/logiqbits/go-rabbitbus/gbus"
	"github.com/logiqbits/go-rabbitbus/gbus/builder"
	"github.com/logiqbits/go-rabbitbus/gbus/policy"
	"github.com/logiqbits/go-rabbitbus/gbus/serialization"
	amqp "github.com/rabbitmq/amqp091-go"
)

func rawChannel(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(connStr)
	if err != nil {
		t.Fatalf("failed to connect to broker: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatalf("failed to open channel: %v", err)
	}
	return conn, ch
}

func cleanupQueue(ch *amqp.Channel, name string) {
	_, _ = ch.QueueDelete(name, false, false, false)
}

func cleanupExchange(ch *amqp.Channel, name string) {
	_ = ch.ExchangeDelete(name, false, false)
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", desc)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func buildHardeningBus(svcName string, configure func(gbus.Builder) gbus.Builder) gbus.Bus {
	//mirrors the production backend builder chain so it must keep compiling and working
	b := builder.New().
		Bus(connStr).
		WithSerializer(serialization.NewJsonSerializer()).
		WithPolicies(&policy.Durable{}).
		WithConfirms().
		WithResendsBufferSize(100).
		WorkerNum(5, 1)
	if configure != nil {
		b = configure(b)
	}
	return b.Build(svcName)
}

// G1: a message carrying an x-death header on a non-transactional bus with no
// deadletter handler must not panic the consumer; it is rejected and the worker survives
func TestDeadletterHeaderWithoutHandlerDoesNotKillConsumer(t *testing.T) {
	svc := "hardening-g1-svc"
	b := buildHardeningBus(svc, nil)

	//the consumer must still be alive: a normal command gets processed
	proceed := make(chan bool, 1)
	if err := b.HandleMessage(Command1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}); err != nil {
		t.Fatalf("failed to register handler: %v", err)
	}

	if err := b.Start(); err != nil {
		t.Fatalf("failed to start bus: %v", err)
	}
	defer b.Shutdown()

	conn, ch := rawChannel(t)
	defer conn.Close()
	defer cleanupQueue(ch, svc)

	//publish a poison message with an x-death header directly to the service queue
	xDeath := []interface{}{amqp.Table{
		"queue":        svc,
		"reason":       "rejected",
		"count":        int64(1),
		"exchange":     "",
		"routing-keys": []interface{}{svc},
		"time":         time.Now(),
	}}
	if err := ch.Publish("", svc, false, false, amqp.Publishing{
		Body:      []byte("{}"),
		MessageId: "poison-1",
		Headers:   amqp.Table{"x-death": xDeath},
	}); err != nil {
		t.Fatalf("failed to publish poison message: %v", err)
	}

	//the poison must be rejected (requeue=false) so it leaves the queue;
	//with the old code the worker panicked and the message stayed unacked forever
	waitFor(t, 5*time.Second, "poison message to be rejected from the queue", func() bool {
		depth, err := b.QueueDepth(svc)
		return err == nil && depth == 0
	})

	if err := b.Send(noopTraceContext(), svc, gbus.NewBusMessage(Command1{Data: "after-poison"})); err != nil {
		t.Fatalf("failed to send command: %v", err)
	}
	select {
	case <-proceed:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer is dead: command was not processed after x-death poison message")
	}
}

// G5: with WithNoHandlerAction(NoHandlerReject) a message with no registered handler
// is rejected (requeue=false) and routed to the dead-letter exchange instead of being acked away
func TestNoHandlerRejectDeadlettersMessage(t *testing.T) {
	dlx := "hardening-g5-dlx"
	dlq := "hardening-g5-dlq"
	svc := "hardening-g5-svc"

	conn, ch := rawChannel(t)
	defer conn.Close()
	defer cleanupQueue(ch, dlq)
	defer cleanupExchange(ch, dlx)

	if err := ch.ExchangeDeclare(dlx, "fanout", true, false, false, false, nil); err != nil {
		t.Fatalf("failed to declare dlx: %v", err)
	}
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		t.Fatalf("failed to declare dlq: %v", err)
	}
	if err := ch.QueueBind(dlq, "", dlx, false, nil); err != nil {
		t.Fatalf("failed to bind dlq: %v", err)
	}

	b := buildHardeningBus(svc, func(builder gbus.Builder) gbus.Builder {
		return builder.
			WithDeadlettering(dlx).
			WithNoHandlerAction(gbus.NoHandlerReject)
	})
	proceed := make(chan bool, 1)
	if err := b.HandleMessage(Command1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}); err != nil {
		t.Fatalf("failed to register handler: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("failed to start bus: %v", err)
	}
	defer b.Shutdown()
	defer cleanupQueue(ch, svc)

	//start observing the dlq before sending the unhandled message
	dlqMsgs, err := ch.Consume(dlq, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume dlq: %v", err)
	}

	//Command2 has no registered handler on this bus
	if err := b.Send(noopTraceContext(), svc, gbus.NewBusMessage(Command2{Data: "nobody-home"})); err != nil {
		t.Fatalf("failed to send command: %v", err)
	}

	select {
	case d := <-dlqMsgs:
		if d.Headers["x-death"] == nil {
			t.Fatalf("expected x-death header on dead-lettered message, headers: %v", d.Headers)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("message with no handler was not rejected to the dead-letter queue")
	}
}

// G4: with WithMandatory, publishing to a routing key that matches no binding
// surfaces as an error instead of being silently dropped
func TestMandatoryPublishReturnsErrorForUnroutable(t *testing.T) {
	exchange := "hardening-g4-exch"
	svc := "hardening-g4-svc"

	conn, ch := rawChannel(t)
	defer conn.Close()
	defer cleanupExchange(ch, exchange)

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("failed to declare exchange: %v", err)
	}

	//no WithConfirms: mandatory must imply the confirm machinery on its own
	b := buildHardeningBus(svc, func(builder gbus.Builder) gbus.Builder {
		return builder.WithMandatory()
	})
	if err := b.Start(); err != nil {
		t.Fatalf("failed to start bus: %v", err)
	}
	defer b.Shutdown()
	defer cleanupQueue(ch, svc)

	err := b.Publish(noopTraceContext(), exchange, "no.such.binding", gbus.NewBusMessage(Event1{Data: "lost?"}))
	if err == nil {
		t.Fatal("expected an error for an unroutable mandatory publish, got nil")
	}
	t.Logf("got expected unroutable error: %v", err)
}

// Management API: QueueDepth/Purge round-trip plus Health/DeadletterCount against a unique queue
func TestManagementAPI(t *testing.T) {
	queue := "hardening-mgmt-q"
	svc := "hardening-mgmt-svc"

	conn, ch := rawChannel(t)
	defer conn.Close()
	defer cleanupQueue(ch, queue)

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}

	b := buildHardeningBus(svc, nil)
	if err := b.Start(); err != nil {
		t.Fatalf("failed to start bus: %v", err)
	}
	defer b.Shutdown()
	defer cleanupQueue(ch, svc)

	if !b.Health() {
		t.Fatal("expected bus to report healthy after start")
	}

	depth, err := b.QueueDepth(queue)
	if err != nil {
		t.Fatalf("QueueDepth failed: %v", err)
	}
	if depth != 0 {
		t.Fatalf("expected empty queue, got depth %d", depth)
	}

	for i := 0; i < 3; i++ {
		if err := ch.Publish("", queue, false, false, amqp.Publishing{Body: []byte("{}"), MessageId: "mgmt-" + string(rune('a'+i))}); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}

	waitFor(t, 5*time.Second, "queue depth to reach 3", func() bool {
		depth, err := b.QueueDepth(queue)
		return err == nil && depth == 3
	})

	purged, err := b.Purge(queue)
	if err != nil {
		t.Fatalf("Purge failed: %v", err)
	}
	if purged != 3 {
		t.Fatalf("expected 3 purged messages, got %d", purged)
	}

	depth, err = b.QueueDepth(queue)
	if err != nil {
		t.Fatalf("QueueDepth after purge failed: %v", err)
	}
	if depth != 0 {
		t.Fatalf("expected empty queue after purge, got depth %d", depth)
	}

	//no DLX configured on this bus
	dlCount, err := b.DeadletterCount()
	if err != nil {
		t.Fatalf("DeadletterCount failed: %v", err)
	}
	if dlCount != 0 {
		t.Fatalf("expected 0 deadletter count without a DLX, got %d", dlCount)
	}
}
