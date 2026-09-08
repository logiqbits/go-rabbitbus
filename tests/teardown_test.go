package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/logiqbits/go-rabbitbus/gbus"
	"github.com/logiqbits/go-rabbitbus/gbus/builder"
	"github.com/logiqbits/go-rabbitbus/gbus/policy"
	"github.com/logiqbits/go-rabbitbus/gbus/serialization"
)

// Teardown stress tests guarding against the two production panics:
//  1. "close of closed channel" in gbus when graceful shutdown and the
//     connection-loss supervisor tear the bus down concurrently.
//  2. "send on closed channel" inside amqp091-go's own goroutines
//     (https://github.com/rabbitmq/amqp091-go/issues/360), triggered when a
//     NotifyClose listener buffer is full at connection shutdown.
//
// The tests use uniquely-prefixed queue names and only ever force-close
// connections that consume from those queues, so nothing else on the shared
// broker is disturbed.

var stressMgmt = os.Getenv("RABBITBUS_MGMT") // e.g. http://user:Password%402026@119.40.87.181:15672

func mgmt(t *testing.T, method, path string, wantStatus int) []byte {
	t.Helper()
	if stressMgmt == "" {
		t.Skip("RABBITBUS_MGMT not set; stress tests need the broker management API for connection kills and cleanup")
	}
	req, err := http.NewRequest(method, stressMgmt+path, bytes.NewBufferString("{}"))
	if err != nil {
		t.Fatalf("bad management request %s %s: %v", method, path, err)
	}
	if method == "PUT" || method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("management request %s %s failed: %v", method, path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("management request %s %s: got status %d, want %d: %s", method, path, resp.StatusCode, wantStatus, body)
	}
	return body
}

// connectionsConsuming returns the names of the broker connections that have
// a consumer on the given queue.
func connectionsConsuming(t *testing.T, queue string) []string {
	t.Helper()
	list := mgmt(t, "GET", "/api/consumers", 200)
	var consumers []struct {
		Queue struct {
			Name string `json:"name"`
		} `json:"queue"`
		ChannelDetails struct {
			ConnectionName string `json:"connection_name"`
		} `json:"channel_details"`
	}
	if err := json.Unmarshal(list, &consumers); err != nil {
		t.Fatalf("cannot parse consumers list: %v", err)
	}
	var names []string
	seen := map[string]bool{}
	for _, c := range consumers {
		if c.Queue.Name == queue && !seen[c.ChannelDetails.ConnectionName] {
			seen[c.ChannelDetails.ConnectionName] = true
			names = append(names, c.ChannelDetails.ConnectionName)
		}
	}
	return names
}

// killConnectionsConsuming force-closes every AMQP connection that has a
// consumer on the given queue — the production incident's connection-loss
// supervisor path — and returns once the broker reports them present.
func killConnectionsConsuming(t *testing.T, queue string) {
	t.Helper()
	waitFor(t, 15*time.Second, "consumers to register on the broker", func() bool {
		return len(connectionsConsuming(t, queue)) > 0
	})
	for _, name := range connectionsConsuming(t, queue) {
		mgmt(t, "DELETE", "/api/connections/"+url.PathEscape(name), 204)
	}
}

func cleanupStressQueue(t *testing.T, queue string) {
	mgmt(t, "DELETE", "/api/queues/%2f/"+queue, 204)
}

func buildStressBus(connStr, svcName string) gbus.Bus {
	return builder.New().
		Bus(connStr).
		WithSerializer(serialization.NewJsonSerializer()).
		WithPolicies(&policy.Durable{}).
		WithConfirms().
		WithResendsBufferSize(100).
		WorkerNum(2, 1).
		Build(svcName)
}

// 50 cycles of Start -> consume a few deliveries -> Shutdown, with Shutdown
// also called concurrently from a second goroutine on every cycle. A panic
// (double close in our teardown, or a racing send inside amqp091-go) crashes
// the test binary; survival plus a clean, consuming restart every cycle is
// the assertion.
func TestTeardownStressStartStopCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	svc := fmt.Sprintf("rbteardown-%d-svc", os.Getpid())
	defer cleanupStressQueue(t, svc)

	var consumed int32

	const cycles = 50
	for i := 0; i < cycles; i++ {
		b := buildStressBus(connStr, svc)
		if err := b.HandleMessage(Command1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
			atomic.AddInt32(&consumed, 1)
			return nil
		}); err != nil {
			t.Fatalf("cycle %d: failed to register handler: %v", i, err)
		}
		if err := b.Start(); err != nil {
			t.Fatalf("cycle %d: failed to start bus: %v", i, err)
		}

		for j := 0; j < 3; j++ {
			if err := b.Send(noopTraceContext(), svc, gbus.NewBusMessage(Command1{Data: fmt.Sprintf("cycle-%d-%d", i, j)})); err != nil {
				t.Fatalf("cycle %d: failed to send command: %v", i, err)
			}
		}
		waitFor(t, 15*time.Second, fmt.Sprintf("cycle %d deliveries to be consumed", i), func() bool {
			return atomic.LoadInt32(&consumed) == int32(3*(i+1))
		})

		//graceful shutdown racing a second graceful shutdown; both must return cleanly
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for k := 0; k < 2; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				errs[k] = b.Shutdown()
			}(k)
		}
		wg.Wait()
		for k, err := range errs {
			if err != nil {
				t.Fatalf("cycle %d: concurrent Shutdown %d returned error: %v", i, k, err)
			}
		}
	}
}

// The broker force-closes the bus connection (the connection-loss supervisor
// fires its Shutdown) while the test simultaneously issues a graceful
// Shutdown. This is the exact interleaving that produced the production
// "close of closed channel" panic.
func TestTeardownStressBrokerSideConnectionKill(t *testing.T) {
	svc := fmt.Sprintf("rbteardown-%d-kill-svc", os.Getpid())
	defer cleanupStressQueue(t, svc)

	b := buildStressBus(connStr, svc)
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

	if err := b.Send(noopTraceContext(), svc, gbus.NewBusMessage(Command1{Data: "ping"})); err != nil {
		t.Fatalf("failed to send command: %v", err)
	}
	select {
	case <-proceed:
	case <-time.After(15 * time.Second):
		t.Fatal("bus did not consume its message before the connection kill")
	}

	killConnectionsConsuming(t, svc)

	//race the connection-loss supervisor's Shutdown (spawned by the monitor)
	//with a graceful Shutdown from the owner
	done := make(chan error, 1)
	go func() { done <- b.Shutdown() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown returned error after connection kill: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("Shutdown hung after broker-side connection kill")
	}
	if b.Health() {
		t.Fatal("bus reported healthy after its connection was killed")
	}
}
