package gbus

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// mandatoryConfirmTimeout bounds how long Post waits for the broker to confirm
// or return a mandatory publish before failing the call
const mandatoryConfirmTimeout = 30 * time.Second

//AMQPOutbox sends messages to the amqp transport
type AMQPOutbox struct {
	channel      *amqp.Channel
	confirm      bool
	resendOnNack bool
	mandatory    bool
	sequence     uint64
	ack          chan uint64
	nack         chan uint64
	resends      chan pendingConfirmation
	returns      chan amqp.Return
	locker       *sync.Mutex
	pending      map[uint64]pendingConfirmation
	results      map[uint64]chan error
	stop         chan bool
	stopOnce     sync.Once
}

func (out *AMQPOutbox) init(amqpCh *amqp.Channel, confirm, resendOnNack bool, resendsBufferSize int) error {
	return out.initWithOptions(amqpCh, confirm, resendOnNack, resendsBufferSize, false)
}

func (out *AMQPOutbox) initWithOptions(amqpCh *amqp.Channel, confirm, resendOnNack bool, resendsBufferSize int, mandatory bool) error {
	//ponytail: stop is buffered so shutdown never blocks when the confirmation
	//loop was never started (confirms off) — an unbuffered send here hung Shutdown
	out.stop = make(chan bool, 1)
	out.stopOnce = sync.Once{}
	out.pending = make(map[uint64]pendingConfirmation)
	out.results = make(map[uint64]chan error)
	out.locker = &sync.Mutex{}
	out.channel = amqpCh
	out.confirm = confirm
	out.mandatory = mandatory
	if confirm || mandatory {
		if resendsBufferSize < 1 {
			resendsBufferSize = 100
		}
		out.ack = make(chan uint64, 10)
		out.nack = make(chan uint64, 10)
		out.resends = make(chan pendingConfirmation, resendsBufferSize)

		if err := out.channel.Confirm(false /*noWait*/); err != nil {
			return err
		}
		out.channel.NotifyConfirm(out.ack, out.nack)
		if mandatory {
			out.returns = make(chan amqp.Return, 32)
			out.channel.NotifyReturn(out.returns)
		}
		if resendOnNack || mandatory {
			out.resendOnNack = resendOnNack
			go out.confirmationLoop()
		}

	}

	return nil
}

func (out *AMQPOutbox) shutdown() {
	out.stopOnce.Do(func() {
		out.stop <- true
	})
}

//Post implements Outbox.Send
func (out *AMQPOutbox) Post(exchange, routingKey string, amqpMessage amqp.Publishing) (uint64, error) {

	out.locker.Lock()
	//generate the delivery tag for this message
	nextSequence := out.sequence + 1

	if out.confirm || out.mandatory {
		p := pendingConfirmation{
			exchange:    exchange,
			routingKey:  routingKey,
			amqpMessage: amqpMessage}
		out.pending[nextSequence] = p
	}

	var result chan error
	if out.mandatory {
		result = make(chan error, 1)
		out.results[nextSequence] = result
	}

	sendErr := out.sendToChannel(exchange, routingKey, amqpMessage)
	if sendErr != nil {
		//if an error was received then move the pending confirmation from the pending map
		delete(out.pending, nextSequence)
		delete(out.results, nextSequence)
		out.locker.Unlock()
		return 0, sendErr

	}
	// only update the global sequence if the send optation on the channel does not return an error
	// so that the global sequence and the channel delivery tag counter stay in sync
	out.sequence = nextSequence
	out.locker.Unlock()

	if result == nil {
		return nextSequence, nil
	}

	//mandatory publishing: surface unroutable messages to the caller as an error
	select {
	case err := <-result:
		return nextSequence, err
	case <-time.After(mandatoryConfirmTimeout):
		out.locker.Lock()
		delete(out.results, nextSequence)
		out.locker.Unlock()
		return nextSequence, fmt.Errorf("timed out after %v waiting for publish confirmation from the broker", mandatoryConfirmTimeout)
	case <-out.stop:
		return nextSequence, errors.New("outbox shut down while waiting for publish confirmation")
	}
}

func (out *AMQPOutbox) confirmationLoop() {

	for {
		select {
		case <-out.stop:
			return
		case ack := <-out.ack:
			if ack <= 0 {
				continue
			}
			out.locker.Lock()
			result := out.results[ack]
			delete(out.results, ack)
			pending := out.pending[ack]
			if pending.deliveryTag > 0 {
				log.Printf("ack received for a pending delivery with tag %v", ack)
			}
			delete(out.pending, ack)
			out.locker.Unlock()
			if result != nil {
				result <- nil
			}
		case nack := <-out.nack:
			if nack <= 0 {
				continue
			}
			log.Printf("nack received for delivery tag %v", nack)
			out.locker.Lock()
			pending := out.pending[nack]
			pending.deliveryTag = nack
			result := out.results[nack]
			delete(out.results, nack)
			delete(out.pending, nack)
			out.locker.Unlock()
			if result != nil {
				result <- fmt.Errorf("broker nacked the publish (delivery tag %v)", nack)
			}
			if out.resendOnNack {
				out.enqueueResend(pending)
			}
		case resend := <-out.resends:
			out.Post(resend.exchange, resend.routingKey, resend.amqpMessage)
		case ret := <-out.returns:
			out.handleReturn(ret)
		}
	}
}

// enqueueResend schedules a nacked message for republishing.
// ponytail: the enqueue is non-blocking — when the resends buffer is full the
// message is dropped with an error log instead of blocking. Blocking here
// deadlocks the whole publish path: the caller holds out.locker (or the
// confirmationLoop resend case calls Post) while Post needs the same lock.
// Ceiling: at most resendsBufferSize NACK retries in flight; raise
// WithResendsBufferSize to lift it.
func (out *AMQPOutbox) enqueueResend(pending pendingConfirmation) {
	select {
	case out.resends <- pending:
	default:
		log.Printf("rabbitbus: resends buffer full (capacity %d), dropping nacked message. exchange:%q routingKey:%q messageId:%q",
			cap(out.resends), pending.exchange, pending.routingKey, pending.amqpMessage.MessageId)
	}
}

// handleReturn matches a returned (unroutable) mandatory message to its pending
// publish and resolves the caller's confirmation as a failure
func (out *AMQPOutbox) handleReturn(ret amqp.Return) {
	out.locker.Lock()
	var tag uint64
	for seq, pending := range out.pending {
		if pending.amqpMessage.MessageId == ret.MessageId &&
			pending.exchange == ret.Exchange &&
			pending.routingKey == ret.RoutingKey {
			tag = seq
			break
		}
	}
	if tag == 0 {
		out.locker.Unlock()
		log.Printf("rabbitbus: return received for unknown message. exchange:%q routingKey:%q messageId:%q",
			ret.Exchange, ret.RoutingKey, ret.MessageId)
		return
	}
	result := out.results[tag]
	delete(out.results, tag)
	delete(out.pending, tag)
	out.locker.Unlock()

	returnErr := fmt.Errorf("message returned as unroutable by the broker. exchange:%q routingKey:%q replyCode:%d replyText:%q",
		ret.Exchange, ret.RoutingKey, ret.ReplyCode, ret.ReplyText)
	log.Printf("rabbitbus: %v", returnErr)
	if result != nil {
		result <- returnErr
	}
}

func (out *AMQPOutbox) sendToChannel(exchange, routingKey string, amqpMessage amqp.Publishing) error {

	if out.channel == nil {
		return errors.New("outbox channel is nil, message not published")
	}
	return out.channel.Publish(exchange, /*exchange*/
		routingKey,    /*key*/
		out.mandatory, /*mandatory*/
		false,         /*immediate*/
		amqpMessage /*msg*/)
}

//NotifyConfirm send an amqp notification
func (out *AMQPOutbox) NotifyConfirm(ack, nack chan uint64) {
	out.channel.NotifyConfirm(ack, nack)
}

type pendingConfirmation struct {
	deliveryTag uint64
	exchange    string
	routingKey  string
	amqpMessage amqp.Publishing
}
