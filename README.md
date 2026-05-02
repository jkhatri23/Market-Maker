# SOL Market Maker (Hyperliquid + Binance)

A high-performance, SOL-only market making bot designed to capture the spread on **Hyperliquid** (Maker Venue) and instantly flatten directional risk on **Binance Futures** (Hedge Venue).

## Overview

This bot operates on a "Maker-Hedge" principle:
1.  **Quoting**: It places two-sided limit orders (bids and asks) on Hyperliquid, where liquidity is thinner and maker rebates are available.
2.  **Execution**: When a limit order is filled on Hyperliquid, the bot receives a WebSocket notification.
3.  **Hedging**: It immediately sends a market order to Binance Futures for the same size but on the opposite side.
4.  **Result**: The net position across both exchanges remains near zero, while the bot captures the spread from Hyperliquid minus the slippage and fees on Binance.

## System Architecture

-   **`cmd/bot`**: The entry point. Handles configuration loading, logger initialization, and wiring the engine with selected venues.
-   **`internal/engine`**: 
    -   **Asset Workers**: Manage the requote loop for each asset. They monitor price moves and update quotes.
    -   **Reconciler**: Minimizes API calls by only canceling/placing orders when the desired state deviates significantly from the current state.
-   **`internal/hedger`**: Listens for fills on the maker venue and triggers immediate market orders on the hedge venue.
-   **`internal/exchanges`**:
    -   **Hyperliquid**: Implements EIP-712 signing for L1 actions and connects to the L1 WebSocket for real-time fills.
    -   **Binance**: Implements HMAC-SHA256 signing for USD-M Futures and uses the User Data Stream for fill notifications.
    -   **Paper**: A local simulator for safe testing without real capital.
-   **`internal/risk`**: Manages the "Risk Book," tracking net positions and PnL. It can "skew" or "widen" quotes based on inventory to encourage price moves that return the position to neutral.
-   **`internal/pricefeed`**: Connects to **Pyth Network** (Hermes) via WebSockets to get low-latency reference prices.

## Design Choices

### 1. SOL-Only Focus
By specializing in SOL, the bot's configuration and risk parameters are tuned for SOL's volatility and liquidity profiles. This reduces complexity and allows for more aggressive quoting.

### 2. Hyperliquid as Maker
Hyperliquid offers zero-fee (or rebate) maker orders and a high-performance L1. This makes it an ideal "Alpha" venue where we want our orders to be hit.

### 3. Async Hedging
Hedging is performed in a separate goroutine. This ensures that the ingestion of fills from the WebSocket is never blocked by the network latency of placing a hedge order on a different exchange.

### 4. Reconciler Pattern
Instead of "Cancel All -> Place New," the reconciler calculates the diff between the target state and the current state. This preserves "queue priority" for orders that don't need to move and stays well within exchange rate limits.

### 5. Type Safety & Interfaces
The `exchange.Exchange` interface allows the engine to treat Paper, Hyperliquid, and Binance identically. This enabled us to develop the entire strategy using the Paper venue before switching to live exchanges.

## Getting Started

1.  **Setup Config**: Copy `config.example.yaml` to `config.yaml`.
2.  **Paper Trade**: Run with the default "paper" maker venue to see how it quotes against live Pyth prices.
3.  **Live Trade**:
    -   Set `engine.maker_venue: hyperliquid`
    -   Set `engine.hedge_venue: binance`
    -   Provide API keys/Private keys in your environment or `config.yaml`.

```bash
go run ./cmd/bot --config config.yaml
```

## Monitoring

-   **Prometheus**: Metrics are exposed on `:9090/metrics` by default.
-   **Slack**: Real-time alerts for fills, circuit breaker trips, and system errors.
-   **Postgres**: (Optional) Every fill and PnL snapshot can be persisted for long-term analysis.
