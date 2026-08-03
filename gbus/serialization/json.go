package serialization

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/logiqbits/go-rabbitbus/gbus"
	"github.com/sirupsen/logrus"
)

type jsonEnvelope struct {
	SchemaName string          `json:"schema_name"`
	Payload    json.RawMessage `json:"payload"`
}

var _ gbus.Serializer = &Json{}

//Json a serializer for GBus uses JSON
type Json struct {
	lock              *sync.Mutex
	registeredSchemas map[string]reflect.Type
}

//NewJsonSerializer creates a new instance of Json and returns it
func NewJsonSerializer() gbus.Serializer {
	return &Json{
		registeredSchemas: make(map[string]reflect.Type),
		lock:              &sync.Mutex{},
	}
}

//Name implements Serializer.Name
func (js *Json) Name() string {
	return "json"
}

//Encode encodes an object into a byte array
func (js *Json) Encode(obj gbus.Message) (buffer []byte, err error) {
	js.Register(obj)

	payloadBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	env := jsonEnvelope{
		SchemaName: obj.SchemaName(),
		Payload:    payloadBytes,
	}

	return json.Marshal(env)
}

//Decode decodes a byte array into an object
func (js *Json) Decode(buffer []byte, schemaName string) (msg gbus.Message, err error) {
	js.lock.Lock()
	t, ok := js.registeredSchemas[schemaName]
	js.lock.Unlock()
	if !ok {
		return nil, fmt.Errorf("could not find the message type in json registry, type: %s", schemaName)
	}

	var env jsonEnvelope
	if err = json.Unmarshal(buffer, &env); err != nil {
		return nil, err
	}

	instance := reflect.New(t).Interface()
	if err = json.Unmarshal(env.Payload, instance); err != nil {
		return nil, err
	}

	msg, ok = instance.(gbus.Message)
	if !ok {
		return nil, fmt.Errorf("could not cast %v to gbus.Message", instance)
	}
	return msg, nil
}

//Register json messages
func (js *Json) Register(obj gbus.Message) {
	js.lock.Lock()
	defer js.lock.Unlock()
	if js.registeredSchemas[obj.SchemaName()] == nil {
		logrus.WithField("SchemaName", obj.SchemaName()).Debug("registering schema to json")
		t := reflect.TypeOf(obj)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		js.registeredSchemas[obj.SchemaName()] = t
	}
}
