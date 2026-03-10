package bus_nats

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	base "github.com/infrago/base"
	"github.com/infrago/bus"
	"github.com/infrago/infra"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNatsBusCrossNodeRequest(t *testing.T) {
	addr := freeAddr(t)

	ns, err := server.NewServer(&server.Options{
		Host: "127.0.0.1",
		Port: addr.Port,
	})
	if err != nil {
		t.Fatalf("new nats server failed: %v", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatalf("nats server not ready")
	}
	defer ns.Shutdown()

	serviceName := fmt.Sprintf("bus.test.%d", time.Now().UnixNano())
	infra.Register(serviceName, infra.Service{
		Action: func(ctx *infra.Context) (base.Map, base.Res) {
			return base.Map{"ok": true, "name": serviceName}, infra.OK
		},
	})
	argv := os.Args
	os.Args = []string{argv[0]}
	defer func() { os.Args = argv }()
	infra.Prepare()

	url := fmt.Sprintf("nats://%s", addr.String())

	responder1, err := (&natsBusDriver{}).Connect(&bus.Instance{
		Name: "node1",
		Config: bus.Config{
			Setting: map[string]any{"url": url},
		},
	})
	if err != nil {
		t.Fatalf("connect responder1 failed: %v", err)
	}
	defer responder1.Close()
	if err := responder1.Open(); err != nil {
		t.Fatalf("open responder1 failed: %v", err)
	}
	if err := responder1.RegisterService(serviceName, nil); err != nil {
		t.Fatalf("register responder1 failed: %v", err)
	}
	if err := responder1.Start(); err != nil {
		t.Fatalf("start responder1 failed: %v", err)
	}
	defer responder1.Stop()

	responder2, err := (&natsBusDriver{}).Connect(&bus.Instance{
		Name: "node2",
		Config: bus.Config{
			Setting: map[string]any{"url": url},
		},
	})
	if err != nil {
		t.Fatalf("connect responder2 failed: %v", err)
	}
	defer responder2.Close()
	if err := responder2.Open(); err != nil {
		t.Fatalf("open responder2 failed: %v", err)
	}
	if err := responder2.RegisterService(serviceName, nil); err != nil {
		t.Fatalf("register responder2 failed: %v", err)
	}
	if err := responder2.Start(); err != nil {
		t.Fatalf("start responder2 failed: %v", err)
	}
	defer responder2.Stop()

	requester, err := (&natsBusDriver{}).Connect(&bus.Instance{
		Name: "client",
		Config: bus.Config{
			Setting: map[string]any{"url": url},
		},
	})
	if err != nil {
		t.Fatalf("connect requester failed: %v", err)
	}
	defer requester.Close()
	if err := requester.Open(); err != nil {
		t.Fatalf("open requester failed: %v", err)
	}

	reqBody := map[string]any{
		"Name":    serviceName,
		"Payload": map[string]any{"hello": "world"},
	}
	reqBytes, _ := msgpack.Marshal(reqBody)

	for i := 0; i < 3; i++ {
		respBytes, err := requester.Request("call."+serviceName, reqBytes, 2*time.Second)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		resp := map[string]any{}
		if err := msgpack.Unmarshal(respBytes, &resp); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}

		code, _ := resp["code"].(float64)
		if int(code) != 0 {
			t.Fatalf("unexpected response code: %v, body=%s", code, string(respBytes))
		}
	}
}

func freeAddr(t *testing.T) *net.TCPAddr {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr)
}
