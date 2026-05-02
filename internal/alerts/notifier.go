package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/jkhatri23/Market-Maker/internal/config"
)

// Kind identifies the kind of alert. Used both for log/metric labelling
// and to gate which kinds get delivered (per config.SlackConfig flags).
type Kind string

const (
	KindFill           Kind = "fill"
	KindError          Kind = "error"
	KindCircuitBreaker Kind = "circuit_breaker"
	KindHalt           Kind = "halt"
	KindStartup        Kind = "startup"
)

type Notifier interface {
	Notify(ctx context.Context, kind Kind, msg string) error
}

// NoopNotifier discards all alerts. Used when Slack is disabled.
type NoopNotifier struct{}

func (NoopNotifier) Notify(context.Context, Kind, string) error { return nil }

// SlackNotifier sends a JSON webhook. Each Kind is gated by its
// corresponding flag in SlackConfig; unconfigured kinds default to send
// (don't silently drop alerts we forgot to flag).
type SlackNotifier struct {
	cfg    config.SlackConfig
	client *http.Client
	logger *zap.Logger
}

func NewSlackNotifier(cfg config.SlackConfig, logger *zap.Logger) *SlackNotifier {
	return &SlackNotifier{
		cfg:    cfg,
		client: &http.Client{Timeout: 5 * time.Second},
		logger: logger,
	}
}

func (n *SlackNotifier) Notify(ctx context.Context, kind Kind, msg string) error {
	if !n.shouldSend(kind) {
		return nil
	}
	if n.cfg.WebhookURL == "" {
		return nil
	}
	body, err := json.Marshal(map[string]string{
		"text": fmt.Sprintf("[%s] %s", kind, msg),
	})
	if err != nil {
		return fmt.Errorf("marshal alert body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.cfg.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("post slack webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}
	n.logger.Debug("alert sent", zap.String("kind", string(kind)))
	return nil
}

func (n *SlackNotifier) shouldSend(kind Kind) bool {
	if !n.cfg.Enabled {
		return false
	}
	switch kind {
	case KindFill:
		return n.cfg.NotifyOnFill
	case KindError:
		return n.cfg.NotifyOnError
	case KindCircuitBreaker, KindHalt:
		return n.cfg.NotifyOnCircuitBreaker
	default:
		return true
	}
}
