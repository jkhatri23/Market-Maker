package pyth

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// ---------- Pure helpers ----------

func TestNew_RejectsEmptyConfig(t *testing.T) {
	if _, err := New("", map[string]string{"BTC": "abc"}, zap.NewNop()); err == nil {
		t.Errorf("expected error on empty wsURL")
	}
	if _, err := New("ws://x", map[string]string{}, zap.NewNop()); err == nil {
		t.Errorf("expected error on empty feedIDs")
	}
}

func TestNew_NormalizesIDs(t *testing.T) {
	c, err := New("ws://x", map[string]string{"BTC": "0xABCdef"}, zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.feedIDs["BTC"] != "abcdef" {
		t.Errorf("expected lowercased no-0x id, got %q", c.feedIDs["BTC"])
	}
	if c.idToAsset["abcdef"] != "BTC" {
		t.Errorf("reverse lookup wrong: %v", c.idToAsset)
	}
}

func TestHandleUpdate_AppliesExpo(t *testing.T) {
	c, _ := New("ws://x", map[string]string{"BTC": "abc"}, zap.NewNop())
	ch, _ := c.Subscribe("BTC")

	c.handleUpdate(wsFeed{
		ID: "0xABC",
		Price: wsPrice{
			Price:       "5000000000000", // 5e12
			Conf:        "100000000",     // 1e8
			Expo:        -8,
			PublishTime: time.Now().Unix(),
		},
	})

	select {
	case upd := <-ch:
		if math.Abs(upd.Price-50_000) > 0.01 {
			t.Errorf("price = %v, want 50000", upd.Price)
		}
		if math.Abs(upd.Confidence-1.0) > 1e-6 {
			t.Errorf("conf = %v, want 1.0", upd.Confidence)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive update")
	}

	price, age, ok := c.GetPrice("BTC")
	if !ok {
		t.Errorf("GetPrice ok=false after update")
	}
	if math.Abs(price-50_000) > 0.01 {
		t.Errorf("GetPrice = %v, want 50000", price)
	}
	if age > time.Second {
		t.Errorf("GetPrice age %v larger than expected", age)
	}
}

func TestHandleUpdate_IgnoresUnknownFeed(t *testing.T) {
	c, _ := New("ws://x", map[string]string{"BTC": "abc"}, zap.NewNop())
	c.handleUpdate(wsFeed{
		ID:    "deadbeef",
		Price: wsPrice{Price: "100", Conf: "1", Expo: 0, PublishTime: time.Now().Unix()},
	})
	if _, _, ok := c.GetPrice("BTC"); ok {
		t.Errorf("expected no price recorded for unknown id")
	}
}

func TestSubscribe_RejectsUnknownAsset(t *testing.T) {
	c, _ := New("ws://x", map[string]string{"BTC": "abc"}, zap.NewNop())
	if _, err := c.Subscribe("DOGE"); err == nil {
		t.Errorf("expected error subscribing unknown asset")
	}
}

func TestGetPrice_NotSeenYet(t *testing.T) {
	c, _ := New("ws://x", map[string]string{"BTC": "abc"}, zap.NewNop())
	if _, _, ok := c.GetPrice("BTC"); ok {
		t.Errorf("expected ok=false before any update")
	}
}

// ---------- Integration: real WebSocket round-trip ----------

func TestRun_EndToEnd(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade: %v", err)
			return
		}
		defer conn.Close()

		// Read the subscribe message so the test verifies we send one.
		var sub map[string]any
		if err := conn.ReadJSON(&sub); err != nil {
			t.Logf("server read: %v", err)
			return
		}
		if sub["type"] != "subscribe" {
			t.Logf("expected subscribe, got %v", sub["type"])
		}

		// Push one price_update for BTC.
		_ = conn.WriteJSON(wsMessage{
			Type: "price_update",
			PriceFeed: wsFeed{
				ID: "abc",
				Price: wsPrice{
					Price:       "5000000000000",
					Conf:        "100000000",
					Expo:        -8,
					PublishTime: time.Now().Unix(),
				},
			},
		})

		<-r.Context().Done()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := New(wsURL, map[string]string{"BTC": "abc"}, zap.NewNop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ch, _ := c.Subscribe("BTC")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	select {
	case upd := <-ch:
		if math.Abs(upd.Price-50_000) > 0.01 {
			t.Errorf("price = %v, want 50000", upd.Price)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never received price update through WS")
	}
}
