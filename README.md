# RabbitBus

This repository is a fork of [https://github.com/rhinof/grabbit](https://github.com/rhinof/grabbit). RabbitBus separated transactional database provider from source code. We'll manage a separate project to use database providers as driver to store transactional information.

RabbitMQ based service bus for GoLang



### How to use

Install library in your existing go project

```
go get github.com/logiqbits/go-rabbitbus
```



### Examples

Pattern wise messaging examples

* [Command-Reply](#command-reply)
* [Publish/Subscribe](#pub-sub)


#### Command-Reply

Service bus builder

```go
func createServiceBus(conn, serviceName string) gbus.Bus {
	return builder.
		New().
		Bus(conn).
		WithPolicies(&policy.Durable{}).
		WithConfirms().
		Build(serviceName)
}
```



Define command and reply structure, connections and service name strings

```go
type Command1 struct{}

func (Command1) SchemaName() string {
	return "cmd1"
}

type Reply1 struct{}

func (Reply1) SchemaName() string {
	return "reply1"
}

connection := "amqp://guest:guest@localhost"

commandServiceName := "svc.cmd"
replyServiceName := "svc.cmd.reply"
```

**Command service**

```go
commandService := createServiceBus(connection, commandServiceName)
	commandService.HandleMessage(Reply1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		log.Println("[Received Reply]")
		return nil
	})


commandService.Start()
defer commandService.Shutdown()

commandService.Send(context.Background(), replyServiceName, gbus.NewBusMessage(Command1{}))
```

**Reply service**

```go
replyService := createServiceBus(connection, replyServiceName)
	replyService.HandleMessage(Command1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		log.Println("[Received Command]")
		log.Println("[Dispatching Reply]")
		return invocation.Reply(context.Background(), gbus.NewBusMessage(Reply1{}))
	})

	replyService.Start()
	defer replyService.Shutdown()
```





#### Pub-Sub

Define event

```go
type Event1 struct {
	Data string
}

func (Event1) SchemaName() string {
	return "Event1"
}
```



**Subscriber**

```go
connection := "amqp://guest:guest@localhost"
const eventName = "service.event.customname"

bus :=  builder.
        New().
        Bus(connection).
        WithPolicies(&policy.Durable{}).
        WithConfirms().
        Build(eventName)

eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
  log.Println(message.Payload)
  return nil
}
bus.HandleEvent("test_exchange", "test_topic", Event1{}, eventHandler)

bus.Start()
defer bus.Shutdown()
```



**Publisher**

```go
connection := "amqp://guest:guest@localhost"
const eventName = "service.event.customname"

bus :=  builder.
        New().
        Bus(connection).
        WithPolicies(&policy.Durable{}).
        WithConfirms().
        Build(eventName)

err := b.Publish(context.Background(), "test_exchange", "test_topic", gbus.NewBusMessage(&Event1{Data: time.Now().String()}))
if err != nil {
  log.Fatal(err)
}

bus.Start()
defer bus.Shutdown()
```



#### RPC

Defining service names, connection string, service bus builder method

```go
const (
	serverServiceName  = "serverServiceName"
	invokerServiceName = "invokerServiceName"
)

type RpcRequest struct {
	Data string
}

func (RpcRequest) SchemaName() string {
	return "logiqbits.rpc.request"
}

type RpcResponse struct {
	Data string
}

func (RpcResponse) SchemaName() string {
	return "logiqbits.rpc.response"
}

func createRpcBus(conn, svcName string) gbus.Bus {
	return builder.
		New().
		Bus(conn).
		WithPolicies(&policy.Durable{}).
		WithConfirms().
		WithDeadlettering("dead-logiqbits-rabbitbus").
		PurgeOnStartUp().
		Build(svcName)
}
```



**Server**

```go
connection := "amqp://guest:guest@localhost"
handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
  req, ok := message.Payload.(*RpcRequest)
  if !ok {
    log.Fatalln("failed to parse request body")
  }
  return invocation.Reply(context.Background(), gbus.NewBusMessage(RpcResponse{
    Data: fmt.Sprintf("Hello %s", req.Data),
  }))
}

serverService := createRpcBus(connection, serverServiceName)
serverService.HandleMessage(RpcRequest{}, handler)
serverService.Start()
defer serverService.Shutdown()
```



**Invoker**

```go
invokerService := createRpcBus(connection, invokerServiceName)
invokerService.Start()
defer invokerService.Shutdown()

log.Println("Sending RPC")
reply, err := invokerService.RPC(
  context.Background(),
  serverServiceName,
  gbus.NewBusMessage(RpcRequest{Data: "Mr. Jack"}),
  gbus.NewBusMessage(RpcResponse{}),
  5*time.Second)
if err != nil {
  log.Fatal(err)
}
log.Println(reply.Payload) // should be 'Hello Mr. Jack'
```



