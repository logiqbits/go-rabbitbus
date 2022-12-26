package gbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"runtime/debug"
	"sync"
	"time"

	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var _ SagaRegister = &DefaultBus{}

//DefaultBus implements the Bus interface
type DefaultBus struct {
	*Safety
	Outgoing       *AMQPOutbox
	Outbox         TxOutbox
	PrefetchCount  uint
	AmqpConnStr    string
	amqpConn       *amqp.Connection
	workers        []*worker
	AMQPChannel    *amqp.Channel
	outAMQPChannel *amqp.Channel
	serviceQueue   amqp.Queue
	rpcQueue       amqp.Queue
	SvcName        string
	amqpErrors     chan *amqp.Error
	amqpBlocks     chan amqp.Blocking
	Registrations  []*Registration

	RPCHandlers          map[string]MessageHandler
	deadletterHandler    func(tx *sql.Tx, poision amqp.Delivery) error
	msgs                 <-chan amqp.Delivery
	rpcMsgs              <-chan amqp.Delivery
	HandlersLock         *sync.Mutex
	RPCLock              *sync.Mutex
	SenderLock           *sync.Mutex
	ConsumerLock         *sync.Mutex
	RegisteredSchemas    map[string]bool
	DelayedSubscriptions [][]string
	PurgeOnStartup       bool
	started              bool
	Glue                 SagaRegister
	TxProvider           TxProvider
	IsTxnl               bool
	WorkerNum            uint
	Serializer           Serializer
	DLX                  string
	DefaultPolicies      []MessagePolicy
	Confirm              bool
	healthChan           chan error
	backpreasure         bool
	rabbitFailure        bool
	DbPingTimeout        time.Duration
}

var (
	//TODO: Replace constants with configuration

	//MaxRetryCount defines the max times a retry can run
	MaxRetryCount uint = 3
	rpcHeaderName      = "x-grabbit-msg-rpc-id"
)

func (b *DefaultBus) createRPCQueue() (amqp.Queue, error) {
	/*
		the RPC queue is a queue per service instance (as opposed to the service queue which
		is shared between service instances to allow for round-robin load balancing) in order to
		support synchronous RPC style calls.amqpit is not durable and is auto-deleted once the service
		instance process terminates
	*/
	uid := xid.New().String()
	qName := b.SvcName + "_rpc_" + uid
	q, e := b.AMQPChannel.QueueDeclare(qName,
		false, /*durable*/
		true,  /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		nil /*args*/)
	return q, e
}

func (b *DefaultBus) createServiceQueue() (amqp.Queue, error) {
	qName := b.SvcName
	var q amqp.Queue

	if b.PurgeOnStartup {
		msgsPurged, purgeError := b.AMQPChannel.QueueDelete(qName, false /*ifUnused*/, false /*ifEmpty*/, false /*noWait*/)
		if purgeError != nil {
			b.log("failed to purge queue: %v.\ndeleted number of messages:%v\nError:%v", b.SvcName, msgsPurged, purgeError)
			return q, purgeError
		}
	}

	args := amqp.Table{}
	if b.DLX != "" {
		args["x-dead-letter-exchange"] = b.DLX
	}
	q, e := b.AMQPChannel.QueueDeclare(qName,
		true,  /*durable*/
		false, /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		args /*args*/)
	if e != nil {
		b.log("failed to declare queue.\nerror:%v", e)
	}
	b.serviceQueue = q
	return q, e
}

func (b *DefaultBus) bindServiceQueue() {

	if b.deadletterHandler != nil && b.DLX != "" {
		b.AMQPChannel.ExchangeDeclare(b.DLX, /*name*/
			"fanout", /*kind*/
			true,     /*durable*/
			false,    /*autoDelete*/
			false,    /*internal*/
			false,    /*noWait*/
			nil /*args amqp.Table*/)
		b.bindQueue("", b.DLX)
	}
	for _, subscription := range b.DelayedSubscriptions {
		topic := subscription[0]
		exchange := subscription[1]
		e := b.AMQPChannel.ExchangeDeclare(exchange, /*name*/
			"topic", /*kind*/
			true,    /*durable*/
			false,   /*autoDelete*/
			false,   /*internal*/
			false,   /*noWait*/
			nil /*args amqp.Table*/)
		if e != nil {
			b.log("failed to declare exchange %v\n%v", exchange, e)
		} else {
			e = b.bindQueue(topic, exchange)
			if e != nil {
				b.log("failed to bind to the following\n topic:%v\n exchange:%v\n%v", topic, exchange, e)
			}
		}
	}
}

func (b *DefaultBus) createAMQPChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, e := conn.Channel()
	if e != nil {
		return nil, e
	}
	return channel, nil
}

//Start implements GBus.Start()
func (b *DefaultBus) Start() error {

	var e error
	//create amqo connection and channel
	if b.amqpConn, e = b.connect(int(MaxRetryCount)); e != nil {
		return e
	}

	if b.AMQPChannel, e = b.createAMQPChannel(b.amqpConn); e != nil {
		return e
	}
	if b.outAMQPChannel, e = b.createAMQPChannel(b.amqpConn); e != nil {
		return e
	}

	//register on failure notifications
	b.amqpErrors = make(chan *amqp.Error)
	b.amqpBlocks = make(chan amqp.Blocking)
	b.amqpConn.NotifyClose(b.amqpErrors)
	b.amqpConn.NotifyBlocked(b.amqpBlocks)
	b.outAMQPChannel.NotifyClose(b.amqpErrors)
	//TODO:Figure out what should be done

	//init the outbox that sends the messages to the amqp transport and handles publisher confirms
	if b.Outgoing.init(b.outAMQPChannel, b.Confirm, true); e != nil {
		return e
	}
	/*
		start the transactional outbox, make sure calling b.TxOutgoing.Start() is done only after b.Outgoing.init is called
		TODO://the design is crap and needs to be refactored
	*/
	if b.IsTxnl {

		var amqpChan *amqp.Channel
		if amqpChan, e = b.createAMQPChannel(b.amqpConn); e != nil {
			b.log("failed to create amqp channel for transactional relay\n%v", e)
			return e
		}
		amqpChan.NotifyClose(b.amqpErrors)
		amqpOutbox := &AMQPOutbox{}
		amqpOutbox.init(amqpChan, b.Confirm, false)
		if startErr := b.Outbox.Start(amqpOutbox); startErr != nil {
			b.log("failed to start transactional relay\n%v", startErr)
			return startErr
		}

	}

	//declare queue
	var q amqp.Queue
	if q, e = b.createServiceQueue(); e != nil {
		return e
	}
	b.serviceQueue = q

	//bind queue
	b.bindServiceQueue()

	//declare rpc queue

	if b.rpcQueue, e = b.createRPCQueue(); e != nil {
		return e
	}

	b.log("initiating %v workers", b.WorkerNum)
	workers, createWorkersErr := b.createBusWorkers(b.WorkerNum)
	if createWorkersErr != nil {

		b.log("error creating channel for worker\n%s", createWorkersErr)

		return createWorkersErr
	}
	b.workers = workers
	b.started = true
	//start monitoring on amqp related errors
	go b.monitorAMQPErrors()
	//start consuming messags from service queue

	return nil
}

func (b *DefaultBus) createBusWorkers(workerNum uint) ([]*worker, error) {
	workers := make([]*worker, 0)
	for i := uint(0); i < workerNum; i++ {
		//create a channel per worker as we can't share channels across go routines
		amqpChan, createChanErr := b.createAMQPChannel(b.amqpConn)
		if createChanErr != nil {
			return nil, createChanErr
		}

		qosErr := amqpChan.Qos(int(b.PrefetchCount), 0, false)
		if qosErr != nil {
			log.Printf("failed to set worker qos\n %v", qosErr)
		}

		tag := fmt.Sprintf("%s_worker_%d", b.SvcName, i)

		w := &worker{
			consumerTag:       tag,
			channel:           amqpChan,
			q:                 b.serviceQueue,
			rpcq:              b.rpcQueue,
			svcName:           b.SvcName,
			isTxnl:            b.IsTxnl,
			txProvider:        b.TxProvider,
			rpcLock:           b.RPCLock,
			rpcHandlers:       b.RPCHandlers,
			deadletterHandler: b.deadletterHandler,
			handlersLock:      b.HandlersLock,
			registrations:     b.Registrations,
			serializer:        b.Serializer,
			b:                 b,
			amqpErrors:        b.amqpErrors}
		go w.Start()

		workers = append(workers, w)
	}
	return workers, nil
}

//Shutdown implements GBus.Start()
func (b *DefaultBus) Shutdown() (shutdwonErr error) {

	b.log("Shuting down %v ", b.SvcName)
	defer func() {
		if p := recover(); p != nil {
			pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
			shutdwonErr = errors.New(pncMsg)
		}
	}()

	for _, worker := range b.workers {
		worker.Stop()
	}

	b.Outgoing.shutdown()
	b.started = false
	b.amqpConn.Close()
	if b.IsTxnl {
		b.TxProvider.Dispose()
		b.Outbox.Stop()
	}
	return nil
}

//NotifyHealth implements Health.NotifyHealth
func (b *DefaultBus) NotifyHealth(health chan error) {
	if health == nil {
		panic("can't pass nil as health channel")
	}
	b.healthChan = health
}

//GetHealth implements Health.GetHealth
func (b *DefaultBus) GetHealth() HealthCard {
	var dbConnected bool

	if b.IsTxnl {
		dbConnected = b.TxProvider.Ping(b.DbPingTimeout)
	}

	return HealthCard{
		DbConnected:        dbConnected,
		RabbitBackPressure: b.backpreasure,
		RabbitConnected:    !b.rabbitFailure,
	}
}

func (b *DefaultBus) withTx(action func(tx *sql.Tx) error, ambientTx *sql.Tx) error {
	var shouldCommitTx bool
	var activeTx *sql.Tx
	//create a new transaction only if there is no active one already passed in
	if b.IsTxnl && ambientTx == nil {

		/*
			if the passed in ambient transaction is not nil it means that some caller has created the transaction
			and knows when should this transaction bee committed or rolledback.
			In these cases we only invoke the passed in action with the passed in transaction
			and do not commit/rollback the transaction.action
			If no ambient transaction is passed in then we create a new transaction and commit or rollback after
			invoking the passed in action
		*/
		shouldCommitTx = true

		newTx, newTxErr := b.TxProvider.New()
		if newTxErr != nil {
			b.log("failed to create transaction when sending a transactional message\n%s", newTxErr)
			return newTxErr
		}
		activeTx = newTx

	} else {
		activeTx = ambientTx
	}
	retryAction := func() error {
		return action(activeTx)
	}
	actionErr := b.SafeWithRetries(retryAction, MaxRetryCount)

	/*
		if the bus is transactional and there is no ambient tranaction then create a new one else use the ambient tranaction.
		if the bus is not transactional a nil transaction reference  will be passed
	*/
	if b.IsTxnl && shouldCommitTx {
		if actionErr != nil {
			activeTx.Rollback()
		} else {
			commitErr := activeTx.Commit()
			if commitErr != nil {
				return commitErr
			}
		}
	}
	return actionErr
}

//Send implements  GBus.Send(destination string, message interface{})
func (b *DefaultBus) Send(ctx context.Context, toService string, message *BusMessage, policies ...MessagePolicy) error {
	return b.sendWithTx(ctx, nil, toService, message, policies...)
}

//RPC implements  GBus.RPC
func (b *DefaultBus) RPC(ctx context.Context, service string, request, reply *BusMessage, timeout time.Duration) (*BusMessage, error) {

	if !b.started {
		return nil, errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}

	b.RPCLock.Lock()
	rpcID := xid.New().String()
	request.RPCID = rpcID
	replyChan := make(chan *BusMessage)
	handler := func(invocation Invocation, message *BusMessage) error {
		replyChan <- message
		return nil
	}

	b.RPCHandlers[rpcID] = handler
	//we do not defer this as we do not want b.RPCHandlers to be locked until a reply returns
	b.RPCLock.Unlock()
	request.Semantics = "cmd"
	rpc := rpcPolicy{
		rpcID: rpcID}

	b.Serializer.Register(reply.Payload)
	b.sendImpl(ctx, nil, service, b.rpcQueue.Name, "", "", request, rpc)

	//wait for reply or timeout
	select {
	case reply := <-replyChan:

		b.RPCLock.Lock()
		delete(b.RPCHandlers, rpcID)
		b.RPCLock.Unlock()
		return reply, nil
	case <-time.After(timeout):
		b.RPCLock.Lock()
		delete(b.RPCHandlers, rpcID)
		b.RPCLock.Unlock()
		return nil, errors.New("rpc call timed out")
	}
}

func (b *DefaultBus) publishWithTx(ctx context.Context, ambientTx *sql.Tx, exchange, topic string, message *BusMessage, policies ...MessagePolicy) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	message.Semantics = "evt"
	publish := func(tx *sql.Tx) error {
		return b.sendImpl(ctx, tx, "", b.SvcName, exchange, topic, message, policies...)
	}
	return b.withTx(publish, ambientTx)
}

func (b *DefaultBus) sendWithTx(ctx context.Context, ambientTx *sql.Tx, toService string, message *BusMessage, policies ...MessagePolicy) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	message.Semantics = "cmd"
	send := func(tx *sql.Tx) error {
		return b.sendImpl(ctx, tx, toService, b.SvcName, "", "", message, policies...)
	}
	return b.withTx(send, ambientTx)
}

//Publish implements GBus.Publish(topic, message)
func (b *DefaultBus) Publish(ctx context.Context, exchange, topic string, message *BusMessage, policies ...MessagePolicy) error {
	return b.publishWithTx(ctx, nil, exchange, topic, message, policies...)
}

//HandleMessage implements GBus.HandleMessage
func (b *DefaultBus) HandleMessage(message Message, handler MessageHandler) error {

	return b.registerHandlerImpl("", b.SvcName, message, handler)
}

//HandleEvent implements GBus.HandleEvent
func (b *DefaultBus) HandleEvent(exchange, topic string, event Message, handler MessageHandler) error {

	/*
	 TODO: Need to remove the event instance from the signature. currently, it is used to map
	 an incoming message to a given handler according to the message type.
	 This is not needed when handling events as we can resolve handlers that need to be invoked
	 by the subscription topic that the message was published to (we can get that out of the amqp headers)
	 In addition, creating a mapping between messages and handlers according to message type prevents us
	 from easily creating polymorphic events handlers or "catch all" handlers that are needed for dead-lettering scenarios
	*/

	/*
		Since it is most likely that the registration for handling an event will be called
		prior to the call to GBus.Start() we will store the exchange and topic of the delayedSubscriptions
		and bind the queue after the bus has been started

	*/

	if !b.started {
		subscription := make([]string, 0)
		subscription = append(subscription, topic, exchange)
		b.DelayedSubscriptions = append(b.DelayedSubscriptions, subscription)
	} else {
		err := b.bindQueue(topic, exchange)

		if err != nil {
			return err
		}
	}
	return b.registerHandlerImpl(exchange, topic, event, handler)
}

//HandleDeadletter implements GBus.HandleDeadletter
func (b *DefaultBus) HandleDeadletter(handler func(tx *sql.Tx, poision amqp.Delivery) error) {
	b.deadletterHandler = handler
}

//RegisterSaga impements GBus.RegisterSaga
func (b *DefaultBus) RegisterSaga(saga Saga, conf ...SagaConfFn) error {
	if b.Glue == nil {
		return errors.New("must configure bus to work with Sagas")
	}
	return b.Glue.RegisterSaga(saga, conf...)

}

func (b *DefaultBus) connect(retryCount int) (*amqp.Connection, error) {

	connected := false
	attempts := uint(0)
	var lastErr error
	for !connected && attempts < MaxRetryCount {
		conn, e := amqp.Dial(b.AmqpConnStr)
		if e == nil {
			return conn, e
		}
		lastErr = e
		attempts++
	}
	return nil, lastErr
}

func (b *DefaultBus) log(format string, v ...interface{}) {
	log.WithField("Service", b.SvcName).Infof(format, v...)
}
func (b *DefaultBus) monitorAMQPErrors() {

	for b.started {
		select {
		case blocked := <-b.amqpBlocks:
			if blocked.Active {
				b.log("amqp connection blocked. reason:%v", blocked.Reason)
			} else {
				b.log("amqp connection unblocked, reason:%v", blocked.Reason)
			}
			b.backpreasure = blocked.Active
		case amqpErr := <-b.amqpErrors:
			b.rabbitFailure = true
			b.log("amqp error: %v", amqpErr)
			if b.healthChan != nil {
				b.healthChan <- amqpErr
			}
		}
	}
}

func (b *DefaultBus) sendImpl(ctx context.Context, tx *sql.Tx, toService, replyTo, exchange, topic string, message *BusMessage, policies ...MessagePolicy) (er error) {
	b.SenderLock.Lock()
	defer b.SenderLock.Unlock()
	//do not attempt to contact the borker if backpreasure is being applied
	if b.backpreasure {
		return errors.New("can't send message due to backpreasure from amqp broker")
	}
	defer func() {
		if err := recover(); err != nil {

			errMsg := fmt.Sprintf("panic recovered panicking err:\n%v\n%s", err, debug.Stack())
			er = errors.New(errMsg)
		}
	}()

	headers := message.GetAMQPHeaders()

	buffer, err := b.Serializer.Encode(message.Payload)
	if err != nil {
		b.log("failed to send message, encoding of message failed with the following error:\n%v\nmessage details:\n%v", err, message)
		return err
	}

	msg := amqp.Publishing{
		Body:            buffer,
		ReplyTo:         replyTo,
		MessageId:       message.ID,
		CorrelationId:   message.CorrelationID,
		ContentEncoding: b.Serializer.Name(),
		Headers:         headers,
	}

	/* TODO:FIX Opentracing context
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		defer sp.Finish()
	}
	// Inject the span context into the AMQP header.
	if err := amqptracer.Inject(sp, msg.Headers); err != nil {
		return err
	}
	*/
	for _, defaultPolicy := range b.DefaultPolicies {
		defaultPolicy.Apply(&msg)
	}

	for _, policy := range policies {
		policy.Apply(&msg)
	}

	key := ""

	if message.Semantics == "cmd" {
		key = toService
	} else {
		key = topic
	}

	publish := func() error {
		//send to the transactional outbox if the bus is transactional
		//otherwise send directly to amqp
		if b.IsTxnl && tx != nil {
			b.log("sending message %v to outbox", msg.MessageId)
			saveErr := b.Outbox.Save(tx, exchange, key, msg)
			if saveErr != nil {
				log.Printf("fialed to save to transactional outbox\n%v", saveErr)
			}
			return saveErr
		}
		_, outgoingErr := b.Outgoing.Post(exchange, key, msg)
		return outgoingErr
	}
	//currently only one thread can publish at a time
	//TODO:add a publishing workers

	err = b.SafeWithRetries(publish, MaxRetryCount)

	if err != nil {
		log.Printf("failed publishing message.\n error:%v", err)
		return err
	}
	return err
}

func (b *DefaultBus) registerHandlerImpl(exchange, routingKey string, msg Message, handler MessageHandler) error {

	b.HandlersLock.Lock()
	defer b.HandlersLock.Unlock()

	if msg != nil {
		b.Serializer.Register(msg)
	}

	registration := NewRegistration(exchange, routingKey, msg, handler)
	b.Registrations = append(b.Registrations, registration)
	return nil
}

func (b *DefaultBus) bindQueue(topic, exchange string) error {
	return b.AMQPChannel.QueueBind(b.serviceQueue.Name, topic, exchange, false /*noWait*/, nil /*args*/)
}

type rpcPolicy struct {
	rpcID string
}

func (p rpcPolicy) Apply(publishing *amqp.Publishing) {
	publishing.Headers[rpcHeaderName] = p.rpcID
}
