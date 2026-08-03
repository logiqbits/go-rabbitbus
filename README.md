# RabbitBus

This repository is a fork of [https://github.com/rhinof/grabbit](https://github.com/rhinof/grabbit). RabbitBus separated transactional database provider from source code. We'll manage a separate project to use database providers as driver to store transactional information.

RabbitMQ based service bus for GoLang

> A Python implementation is available at [python-rabbitbus](https://github.com/logiqbits/python-rabbitbus). It is wire-compatible with this library, so Go and Python services can exchange commands, events, and RPC calls over the same RabbitMQ broker. See [Cross-language messaging with Python](#cross-language-messaging-with-python) below.



### How to use

Install library in your existing go project

```
go get github.com/logiqbits/go-rabbitbus
```



### Examples

Pattern wise messaging examples

* [Command-Reply](#command-reply)
* [Publish/Subscribe](#pub-sub)
* [RPC](#rpc)


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

//need to start the bus before sending/publishing
bus.Start()

err := bus.Publish(context.Background(), "test_exchange", "test_topic", gbus.NewBusMessage(&Event1{Data: time.Now().String()}))
if err != nil {
  log.Fatal(err)
}


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



## Cross-language messaging with Python

The [python-rabbitbus](https://github.com/logiqbits/python-rabbitbus) library implements the same AMQP conventions and messaging patterns. To communicate with a Python service, configure the Go bus to use the JSON serializer and make sure message schema names and JSON field names match on both sides.

### Message schema naming

Use the same `SchemaName()` value and compatible JSON tags:

```go
// Go
type Command1 struct {
    Data string `json:"data"`
}

func (Command1) SchemaName() string { return "example.Command1" }
```

```python
# Python
class Command1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "example.Command1"
```

### Go service configured for Python interop

```go
import (
    "github.com/logiqbits/go-rabbitbus/gbus"
    "github.com/logiqbits/go-rabbitbus/gbus/builder"
    "github.com/logiqbits/go-rabbitbus/gbus/policy"
    "github.com/logiqbits/go-rabbitbus/gbus/serialization"
)

bus := builder.New().Bus("amqp://guest:guest@localhost").
    WithSerializer(serialization.NewJsonSerializer()).
    WithPolicies(&policy.Durable{}).
    Build("go.svc")
```

After that, `bus.Send(...)`, `bus.Publish(...)`, and `bus.RPC(...)` work transparently with Python peers.

### Supported patterns

- **Command-Reply** — Go sends a command to a Python service name; Python replies to the Go service queue.
- **Pub/Sub** — Go publishes to a topic exchange; Python subscribes with `handle_event(...)`, and vice versa.
- **RPC** — Go calls `bus.RPC(...)` targeting a Python service; Python handles the request and replies to the Go RPC queue.

See the `python-rabbitbus` README for a complete bidirectional example.
