package alerts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
)

func newServer(t *testing.T, status int, captured *atomic.Int32, body *[]byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Add(1)
		if body != nil {
			b, _ := io.ReadAll(r.Body)
			*body = b
		}
		w.WriteHeader(status)
	}))
}

func TestSlackNotifier_SendsWhenEnabled(t *testing.T) {
	var hits atomic.Int32
	var body []byte
	srv := newServer(t, 200, &hits, &body)
	defer srv.Close()

	n := NewSlackNotifier(config.SlackConfig{
		Enabled:                true,
		WebhookURL:             srv.URL,
		NotifyOnCircuitBreaker: true,
	}, zap.NewNop())

	if err := n.Notify(context.Background(), KindCircuitBreaker, "drawdown breached"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if hits.Load() != 1 {
		t.Errorf("expected 1 hit, got %d", hits.Load())
	}
	var payload map[string]string
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("body not JSON: %v body=%s", err, body)
	}
	if payload["text"] != "[circuit_breaker] drawdown breached" {
		t.Errorf("unexpected text: %q", payload["text"])
	}
}

func TestSlackNotifier_DropsWhenDisabled(t *testing.T) {
	var hits atomic.Int32
	srv := newServer(t, 200, &hits, nil)
	defer srv.Close()

	n := NewSlackNotifier(config.SlackConfig{Enabled: false, WebhookURL: srv.URL}, zap.NewNop())
	if err := n.Notify(context.Background(), KindHalt, "test"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if hits.Load() != 0 {
		t.Errorf("disabled notifier should not hit webhook, got %d hits", hits.Load())
	}
}

func TestSlackNotifier_GatesByKind(t *testing.T) {
	var hits atomic.Int32
	srv := newServer(t, 200, &hits, nil)
	defer srv.Close()

	n := NewSlackNotifier(config.SlackConfig{
		Enabled:                true,
		WebhookURL:             srv.URL,
		NotifyOnFill:           false,
		NotifyOnError:          true,
		NotifyOnCircuitBreaker: true,
	}, zap.NewNop())

	// Fill: blocked
	_ = n.Notify(context.Background(), KindFill, "x")
	// Error: allowed
	_ = n.Notify(context.Background(), KindError, "y")
	// Halt: allowed via circuit_breaker flag
	_ = n.Notify(context.Background(), KindHalt, "z")

	if hits.Load() != 2 {
		t.Errorf("expected 2 hits (error + halt), got %d", hits.Load())
	}
}

func TestSlackNotifier_SurfacesNon2xx(t *testing.T) {
	var hits atomic.Int32
	srv := newServer(t, 500, &hits, nil)
	defer srv.Close()

	n := NewSlackNotifier(config.SlackConfig{
		Enabled:       true,
		WebhookURL:    srv.URL,
		NotifyOnError: true,
	}, zap.NewNop())

	err := n.Notify(context.Background(), KindError, "boom")
	if err == nil {
		t.Errorf("expected error on 500 response, got nil")
	}
}

func TestNoopNotifier_NeverErrors(t *testing.T) {
	if err := (NoopNotifier{}).Notify(context.Background(), KindError, "x"); err != nil {
		t.Errorf("noop should never error, got %v", err)
	}
}
