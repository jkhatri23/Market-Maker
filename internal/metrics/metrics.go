package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is a small, hand-rolled set of counters/gauges. A custom
// registry (rather than the global one) keeps the test surface clean and
// makes it trivial to spin up multiple bots in one process for sim work.
type Metrics struct {
	reg *prometheus.Registry

	Requotes        *prometheus.CounterVec // labels: asset, venue
	RequoteFailures *prometheus.CounterVec // labels: asset, venue
	OrdersPlaced    *prometheus.CounterVec // labels: asset, venue, side
	Fills           *prometheus.CounterVec // labels: asset, venue, side, maker
	Position        *prometheus.GaugeVec   // labels: asset
	NetPnL          prometheus.Gauge
	Halted          prometheus.Gauge
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		Requotes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "perpsmm_requotes_total", Help: "Total requote attempts",
		}, []string{"asset", "venue"}),
		RequoteFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "perpsmm_requote_failures_total", Help: "Requote attempts that returned an error",
		}, []string{"asset", "venue"}),
		OrdersPlaced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "perpsmm_orders_placed_total", Help: "Orders placed by the engine",
		}, []string{"asset", "venue", "side"}),
		Fills: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "perpsmm_fills_total", Help: "Fills observed",
		}, []string{"asset", "venue", "side", "maker"}),
		Position: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "perpsmm_position_size", Help: "Signed net position per asset",
		}, []string{"asset"}),
		NetPnL: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "perpsmm_net_pnl_usd", Help: "Net PnL across all assets (realized + unrealized)",
		}),
		Halted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "perpsmm_halted", Help: "1 if the risk manager is halted, 0 otherwise",
		}),
	}
	reg.MustRegister(
		m.Requotes, m.RequoteFailures, m.OrdersPlaced, m.Fills,
		m.Position, m.NetPnL, m.Halted,
	)
	return m
}

// Serve binds an HTTP server on addr exposing /metrics. Returns when ctx
// is canceled or the listener fails. Pass an empty addr to disable.
func (m *Metrics) Serve(ctx context.Context, addr string) error {
	if addr == "" {
		<-ctx.Done()
		return ctx.Err()
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
