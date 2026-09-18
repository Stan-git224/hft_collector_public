# HFT L2 Order Book Collector

A multi-symbol L2 order book collector and signal engine for Binance USDT-M futures,
written in Rust and Tokio. It rebuilds local books from the depth WebSocket stream in real
time, compensates for latency, recovers from sequence gaps on its own, and writes ticks and
snapshots to Parquet for offline research. The focus is the systems side: low-latency
plumbing, safe shared state across async tasks, and fault tolerance.

## What it does

- **Fixed-point prices.** `fast_parse_price` shifts the bytes of `"90042.1234"` straight
  into an `i64` with 8 implied decimals, skipping the standard library's float parse.
- **Self-healing sync.** The `U`/`u`/`pu` update ids catch dropped packets; the book flags
  itself `WaitingForSnapshot` and re-fetches a REST snapshot on a cooldown. Reconnects use
  capped exponential backoff.
- **Stale-data guard.** An adaptive clock offset corrects the receive timestamp, and any
  book past the latency threshold is marked `Stale` so the strategy won't trade on it.
- **Decoupled pipeline.** WSS receivers feed a bounded Tokio `mpsc` channel makes slow
  consumer to apply backpressure instead of blocking the socket.
- **Durable output.** Incremental ticks and on-the-hour snapshots are written as Parquet
  through Arrow.
- **Signals.** Example: Mid price and N-level depth imbalance.

## Architecture

The data path is one direction:

    Binance WSS depth stream
        -> per-symbol receiver tasks (reconnect, mark Stale on drop)
        -> mpsc channel (backpressure buffer)
        -> processor (fast parse, clock compensation, latency/stale check)
        -> BookManager: HashMap<Key, Arc<RwLock<OrderBook>>>, bids/asks in BTreeMap<i64, f64>
        -> strategy reader (mid / imbalance) and Parquet storage

REST snapshots feed the books at startup and again during gap recovery.

## Why RwLock

Each exchange/symbol book is one `Arc<RwLock<OrderBook>>`. The workload is read-heavy: one
writer (the WSS processor) updates a book while several readers (strategy, snapshot flush,
monitoring) look at it. `RwLock` matches that shape, allows concurrent reads, and makes the
"readers see a consistent book" intent explicit. Pushing tail latency lower with a
lock-free pointer swap or seqlock-style reads is future work; this version keeps the simpler
and correct design.

## Build and run

Requires a stable Rust toolchain ([rustup.rs](https://rustup.rs)).

    cargo build --release
    cargo run --release

On start it fetches exchange info, opens a WSS connection per symbol, warms up, loads the
REST snapshots, then prints performance reports and live signals.

Telegram and Slack alerts are optional and read from environment variables; an unset channel
is simply disabled:

    export TG_BOT_TOKEN=...
    export TG_CHAT_ID=...
    export SLACK_WEBHOOK_URL=...

## Configuration

All tunables live in `config.toml` (network floor and latency threshold, mpsc buffer and
warmup, strategy depth, symbol list), each with a short comment.

## Benchmarks

Criterion covers the hot paths (fixed-point parse, snapshot load, incremental update):

    cargo bench

HTML reports land in `target/criterion/`.

## Data

Ticks and hourly snapshots are stored as Parquet (`timestamp`, `price`, `qty`, `side`).
`pull_s3_data.sh` syncs historical data from an S3 bucket set via `S3_BUCKET`; `pycode/` has
small Python helpers for inspecting Parquet.

## Layout

    src/
      main.rs          pipeline entry: WSS tasks, processor, consumer, gap recovery
      book.rs          i64 fixed-point OrderBook, BookManager (the RwLock matrix), fast parser
      model.rs         MarketMessage, Binance payloads, AppConfig
      parser.rs        earlier borrow-based parser prototype, kept for reference
      init_market.rs   exchange-info fetch and config loading
      storage.rs       Parquet writer and snapshot scheduler
      alert.rs         async Telegram / Slack alerts
    benches/           criterion hot-path benchmarks
    pycode/            Python helpers
    config.toml        engine / network / strategy settings

## Disclaimer

For research and educational use only. Not investment advice, and no guarantee of
profitability or stability in a live environment.
