# HFT L2 Order Book Collector

A multi-symbol L2 order book collector and signal engine for Binance USDT-M futures,
written in Rust and Tokio. It rebuilds local order books from the depth WebSocket stream
in real time, compensates for latency, protects against stale data, recovers from sequence
gaps on its own, and writes ticks and periodic snapshots to Parquet for offline research.

The focus is on the systems side rather than trading alpha: low-latency data plumbing,
safe shared state across async tasks, and fault tolerance.

## What it does

- Hand-rolled fixed-point price parsing. `fast_parse_price` walks the bytes of `"90042.1234"`
  and shifts them into an `i64` with 8 implied decimals, skipping the standard library's
  float parse.
- One `Arc<RwLock<OrderBook>>` per exchange/symbol. The stream writes, the strategy and
  storage read. It is a read-heavy workload, so `RwLock` keeps the intent obvious and lets
  readers run concurrently.
- WSS receive and processing are decoupled by a Tokio `mpsc` channel with a configurable
  buffer, so a slow consumer applies backpressure instead of blocking the socket.
- Adaptive clock offset: a rolling minimum latency estimates local clock skew and corrects
  the receive timestamp before the latency check.
- Stale-data guard: when latency crosses the configured threshold the book is marked
  `Stale`, and the strategy refuses to emit signals on it.
- Sequence-gap recovery: the `U`/`u`/`pu` update ids catch dropped packets, flag the book
  `WaitingForSnapshot`, and re-fetch a REST snapshot on a cooldown to avoid rate limits.
- Reconnect with capped exponential backoff; a dropped socket marks its book `Stale`
  immediately.
- Parquet output through Arrow: batched incremental ticks plus on-the-hour snapshots.
- Signals: mid price and N-level depth imbalance.

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

The order book is read-heavy: one writer (the WSS processor) updates it while several
readers (strategy, snapshot flush, monitoring) look at it. `RwLock` matches that shape,
allows concurrent reads, and makes the "readers see a consistent book" intent explicit.
Pushing tail latency lower (a lock-free pointer swap, or seqlock-style snapshot reads) is
future work; this version keeps the simpler and correct `RwLock` design.

## Build and run

    cargo build --release
    cargo run --release

On start it fetches exchange info, opens a WSS connection per symbol, warms up the buffer,
loads the REST snapshots, then prints performance reports and live signals.

Telegram and Slack alerts are optional and read from environment variables. An unset
channel is simply disabled:

    export TG_BOT_TOKEN=...
    export TG_CHAT_ID=...
    export SLACK_WEBHOOK_URL=...

## Configuration

All tunables live in `config.toml`: the network floor and latency threshold, the mpsc
buffer size and warmup seconds, the strategy depth, and the symbol list. Each field has a
short comment in that file.

## Benchmarks

Criterion covers the hot paths (fixed-point parse, snapshot load, incremental update, and
the pure-numeric update):

    cargo bench

HTML reports are written to `target/criterion/`.

## Data

Ticks and hourly snapshots are stored as Parquet with columns `timestamp`, `price`, `qty`,
and `side`. `pull_s3_data.sh` syncs historical data from an S3 bucket you set through the
`S3_BUCKET` environment variable. `pycode/` has small Python helpers for inspecting Parquet
and plotting.

## Layout

    src/
      main.rs          pipeline entry: WSS tasks, processor, consumer, gap recovery
      lib.rs           library surface used by the benchmark
      book.rs          i64 fixed-point OrderBook, BookManager (the RwLock matrix), fast parser
      model.rs         MarketMessage, Binance payloads, AppConfig
      parser.rs        earlier borrow-based parser prototype, kept for reference
      init_market.rs   exchange-info fetch and config loading
      storage.rs       Parquet writer and snapshot scheduler
      alert.rs         async Telegram / Slack alerts
    benches/
      orderbook_bench.rs   criterion hot-path benchmarks
    pycode/            Python viewer and plotting helpers
    config.toml        engine / network / strategy settings
    pull_s3_data.sh    S3 historical-data sync

## Disclaimer

For research and educational use only. This is not investment advice, and there is no
guarantee of profitability or stability in a live environment.
