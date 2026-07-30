use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc; // shared handle for the order-book matrix
use tokio::sync::mpsc; // async channels, only used for the pipeline
use futures_util::StreamExt; // websocket next()
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use chrono::Utc;

mod model;
mod parser;
mod book;
mod init_market;
mod storage;
mod alert;

use crate::book::{BookManager, BookStatus, OrderBook};
use crate::init_market::{load_config, sync_exchange_info};
use crate::model::{BinanceDepthUpdate, BinanceSnapshot, MarketMessage}; // init pipeline
use crate::storage::{spawn_incremental_storage_engine, write_batch_to_parquet, TickData, SnapshotScheduler};
use crate::alert::{AlertEngine, AlertLevel};

async fn fetch_snapshot(symbol: &str) -> Result<BinanceSnapshot, Box<dyn std::error::Error + Send + Sync>> {
    // Binance Futures REST API
    let limit = 100;
    let max_retries = 5;
    let url = format!("https://fapi.binance.com/fapi/v1/depth?symbol={}&limit={limit}", symbol);
    let mut delay = tokio::time::Duration::from_secs(1);


    for attempt in 1..=max_retries {
        match reqwest::get(&url).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let snapshot = resp.json::<BinanceSnapshot>().await?;
                    return Ok(snapshot);
                }
                eprintln!("[Snapshots] {} Status code error: {}, trying {}/{} times.", symbol, resp.status(), attempt, max_retries);
            }
                Err(e) => {
                    eprintln!("[Snapshots] {} snapshots request failed: {:?}, trying {}/{} times.", symbol, e, attempt, max_retries);
                }
            }
            if attempt < max_retries {
                tokio::time::sleep(delay).await;
                delay *= 2; // back off between REST retries
            }
        }
        Err(format!("{} trying {} times but still failed to get snapshots.", symbol, max_retries).into())
    }

// helper: local time in ms since Unix epoch
fn get_local_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis() as i64
}

#[tokio::main]
async fn main() {
    // 1. load config.toml
    let config = load_config();
    // 2. fetch exchange info over REST
    let market_filters_map = sync_exchange_info(&config).await;
    let ex = config.tradingpairs.exchange.clone();


    // init alert: credentials come from env vars, never hard-coded.
    // an unset channel gets None and is silently disabled.
    // usage: export TG_BOT_TOKEN=... TG_CHAT_ID=... SLACK_WEBHOOK_URL=...
    let tg_bot_token = std::env::var("YOUR_TG_BOT_TOKEN").ok();
    let tg_chat_id = std::env::var("YOUR_TG_CHAT_ID").ok();
    let slack_webhook_url = std::env::var("YOUR_SLACK_WEBHOOK_URL").ok();
    let alerts = Arc::new(AlertEngine::new(
        tg_bot_token.as_deref(),
        tg_chat_id.as_deref(),
        slack_webhook_url.as_deref(),
    ));

    // init async storage channel
    let (storage_tx, storage_rx) = mpsc::channel::<TickData>(100000);

    // path switch: "./data" locally, "/home/ubuntu/data" on AWS
    let base_data_path = if std::env::var("USER").unwrap_or_default() == "ubuntu" {
        "/home/ubuntu/data"
    } else {
        "./data"
    };

    // spawn the background parquet storage engine
    spawn_incremental_storage_engine(storage_rx, base_data_path);
    alerts.trigger(AlertLevel::INFO, "SYSTEM", "AWS HFT market and storage self-healing engine online");

    // 3. init book manager
    let mut manager_init = BookManager::new();
    // 4. register every symbol from config
        for symbol in &config.tradingpairs.symbols {
            if let Some(filters) = market_filters_map.get(&symbol.to_uppercase()) {
                manager_init.register_pair(ex.as_str(), symbol, filters.clone());
            } else {
                panic!("Fatal Error: symbol {} not found in Binance Futures, please check config.toml or exchange info", symbol);
            }
        }

        // wrap manager in Arc and only ever read it afterwards. we never take an outer lock on it,
        // so there is no std/tokio lock interleaving to deadlock on inside any tokio::spawn.
        let manager = Arc::new(manager_init);

    // 5. MPSC channel, buffer size from config
    let (tx, mut rx) = mpsc::channel::<MarketMessage>(config.engine.channel_buffer_size);

    // [TASK 1]: dynamicaly init several WSS by symbols, auto reconnect if connection lost
    let symbols = config.tradingpairs.symbols.clone();
    let ex_string = config.tradingpairs.exchange.clone();
    let ex_static: &'static str = Box::leak(ex_string.into_boxed_str());
    let wss_limit = 100;
    let max_retries = 9;

    // clone symbols so the original stays available; otherwise it would be moved into the spawn
    // [TASK1] WSS stream receiver + snapshot sync task
    for symbol in symbols.clone() {
        let tx_clone = tx.clone();
        // clone the handle at the top of the task; each loop uses its own
        let manager_clone = Arc::clone(&manager); // shared Arc handle
        let alerts_clone = Arc::clone(&alerts);

        tokio::spawn(async move {
            let mut retry_count = 0;

            // retry logic
            loop {
                // clone symbol per iteration for this connection and its receive loop
                let symbol_run = symbol.clone();
                println!("[WSS] establishing binance.{} real-time connection, trying {}/{} times.", symbol_run, retry_count, max_retries );
                let wss_url = format!("wss://fstream.binance.com/ws/{}@depth@{}ms", symbol_run.to_lowercase(), wss_limit);
                // 1. retry connection to WebSocket
                match connect_async(&wss_url).await {
                    Ok((ws_stream, _)) => {
                        // connect successfully, reset retry count
                        retry_count = 0;
                        println!("[WSS succsss] binance.{} connection successful! starting background fast data transfer task.", symbol_run);
                        let (_, mut mut_read) = ws_stream.split();

                        // on each successful connect, clone a manager handle for the snapshot/reconnect subtasks
                        let manager_for_snap = Arc::clone(&manager_clone);
                        // spawn a parallel task to re-fetch the snapshot after 2s; clone first to avoid closure capture
                        let symbol_for_snap = symbol_run.clone();

                        tokio::spawn(async move {
                            println!("[WSS sync!] waiting 2 seconds for new data to enter the pipeline buffer for binance.{}...", symbol_for_snap);
                            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                            println!("[WSS sync!] buffer ready, starting to download snapshot for binance.{} for offline reconnection.", symbol_for_snap);
                            match fetch_snapshot(&symbol_for_snap).await {
                                Ok(snapshot) => {
                                    // get this book's tokio lock via the manager, no outer std lock
                                    if let Some(book_lock) = manager_for_snap.get_book(ex_static, &symbol_for_snap) {
                                        let mut b = book_lock.write().await;
                                        // convert the REST Vec<Vec<String>> into the &[(&str, &str)] load_snapshot expects
                                        let bids_borrowed: Vec<(&str, &str)> = snapshot.bids.iter()
                                        .filter(|level| level.len() == 2)
                                        .map(|level| (level[0].as_str(), level[1].as_str()))
                                        .collect();

                                        let asks_borrowed: Vec<(&str, &str)> = snapshot.asks.iter()
                                        .filter(|level| level.len() == 2)
                                        .map(|level| (level[0].as_str(), level[1].as_str()))
                                        .collect();

                                        // zero-alloc snapshot load
                                        b.load_snapshot(snapshot.last_update_id as i64, &bids_borrowed, &asks_borrowed);
                                        println!("[SYNC] {} book loaded snapshot.", symbol_for_snap);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to fetch snapshot for {}: {:?}", symbol_for_snap, e);
                                }
                            }
                        });
                        // main receive loop
                        while let Some(msg) = mut_read.next().await {
                            if let Ok(Message::Text(text)) = msg {
                                let market_msg = MarketMessage {
                                    exchange: ex_static,  // &'static str, avoids per-message allocation
                                    symbol: symbol_run.clone(),
                                    raw_json: text
                                };
                                if tx_clone.send(market_msg).await.is_err() { break;} // pipeline closed, shut down
                            }
                        }
                        // WSS disconnect alert
                        alerts_clone.trigger(AlertLevel::CRITICAL, "WSS", &format!(" {} WSS network disconnected, triggering automatic reconnection.", symbol_run));
                        println!("[WSS Error] binance.{}. Automatically reconnecting...", symbol_run);
                        // on disconnect, mark the book Stale immediately so strategy won't read stale data and emit bad signals
                        if let Some(book_lock) = manager_clone.get_book(ex_static, &symbol_run) {
                            let mut b = book_lock.write().await;
                            b.status = BookStatus::Stale;
                            println!("[WSS disconnected] binance.{}. book: Stale.", symbol_run);
                        }
                    }
                    Err(e) => {
                        retry_count += 1;
                        // 3. connection failed: exponential backoff reconnect
                        if retry_count > max_retries {
                            eprintln!("[Fatal Error] binance.{} connection failed {} times, reconnection failed, task ended.", symbol_run, retry_count);
                            panic!("WSS connection lost permanently for {}", symbol_run);
                        }
                        // 2^retry_count
                        let backoff_secs = 2u64.pow(retry_count as u32);
                        eprintln!("[WSS retry] connection to binance failed: {:?}. will retry in {} seconds, {}/{} times.", e, backoff_secs, retry_count, max_retries);
                    }
                }
            }
        });
    }

    println!("Waiting for WSS queue to accumulate safe buffer data...");
    tokio::time::sleep(tokio::time::Duration::from_secs(config.engine.warmup_secs)).await;

    // ------------------------------------------------
    // [TASK 2] central high-throughput routing/processing engine
    // ------------------------------------------------
    let manager_for_processor = Arc::clone(&manager);
    let net_config = config.network.clone();
    let storage_tx_clone = storage_tx.clone();

    tokio::spawn(async move {
        // let mut msg_count = 0;
        // let mut last_report_time = get_local_millis();
        let mut latency_buffer: Vec<u128> = Vec::with_capacity(10000);

        //  --- adaptive clock state ---
        let mut dynamic_clock_offset = 0i64;
        let mut min_raw_latency = i64::MAX;
        let mut pkt_loop_count = 0;
        // physical network floor to Binance's Tokyo futures DC, ~25-30ms
        let base_network_floor = net_config.base_network_floor;
        let latency_threshold = net_config.latency_threshold;
        let pkt_loop_threshold = net_config.pkt_loop_threshold;
        println!("[Processor] multi-threaded router: receiving market information.");

        while let Some(msg) = rx.recv().await {
            let raw_local_recv_time = get_local_millis();
            // msg_count += 1;
            pkt_loop_count += 1;

            // take this book's tokio lock directly, no std lock
            if let Some(book_lock) = manager_for_processor.get_book(msg.exchange, &msg.symbol) {
                if let Ok(update) = serde_json::from_str::<BinanceDepthUpdate>(&msg.raw_json) {
                    let speed_start = std::time::Instant::now();
                    let raw_latency = raw_local_recv_time - update.transaction_time;
                    if raw_latency < min_raw_latency { min_raw_latency = raw_latency;}
                    if pkt_loop_count >= pkt_loop_threshold {
                        dynamic_clock_offset = base_network_floor - min_raw_latency;
                        min_raw_latency = i64::MAX;
                        pkt_loop_count = 0;
                     }

                     let corrected_local_recv_time = raw_local_recv_time + dynamic_clock_offset;
                     let net_latency = corrected_local_recv_time - update.transaction_time;
                     let mut b = book_lock.write().await;

                     if net_latency > latency_threshold { b.status = BookStatus::Stale;}
                     else {b.status = BookStatus::Synced;}

                     if b.handle_event(update.first_update_id, update.final_update_id, update.prev_final_update_id) {
                        // map each [p, q] into a temporary (&str, &str) tuple
                        let bids_borrowed: Vec<(&str, &str)> = update.bids.iter()
                            .map(|k|(k[0].as_str(), k[1].as_str()))
                            .collect();

                        let asks_borrowed: Vec<(&str, &str)> = update.asks.iter()
                            .map(|k|(k[0].as_str(), k[1].as_str()))
                            .collect();

                        b.update_levels(&bids_borrowed, &asks_borrowed);
                        // let elapsed_time = speed_start.elapsed().as_nanos();

                        // push into the storage pipeline: a few tens of ns, never blocks the processor
                        for lv in &update.bids {
                            if let Err(_) = storage_tx_clone.try_send(TickData {
                                timestamp: raw_local_recv_time,
                                symbol: msg.symbol.clone(),
                                price: OrderBook::fast_parse_price(&lv[0]),
                                qty: lv[1].parse::<f64>().unwrap_or(0.0),
                                is_bid: true,
                            }) {
                                eprintln!("[Storage Drop] BIDS: storage channel is full; dropped the oldestincremental tick data..");
                            }
                        }
                        for lv in &update.asks {
                            if let Err(_) = storage_tx_clone.try_send(TickData {
                                timestamp: raw_local_recv_time,
                                symbol: msg.symbol.clone(),
                                price: OrderBook::fast_parse_price(&lv[0]),
                                qty: lv[1].parse::<f64>().unwrap_or(0.0),
                                is_bid: false,
                            }) {
                                eprintln!("[Storage Drop] ASKS: storage channel is full; dropped the oldest incremental tick data..");
                            }
                        }
                        let elapsed_time = speed_start.elapsed().as_nanos();
                        latency_buffer.push(elapsed_time);
                        let avg_count = 1000;
                        if latency_buffer.len() >= avg_count {
                            let sum: u128 = latency_buffer.iter().sum();
                            let avg = sum / avg_count as u128;
                            let max = latency_buffer.iter().max().unwrap_or(&0);
                            let min = latency_buffer.iter().min().unwrap_or(&0);
                            println!("[Speed test] {} avg latency over 1000 ticks: {} ns, max: {} ns, min: {} ns", msg.symbol, avg, max, min);
                            latency_buffer.clear();
                        }

                     }
                }
            }
        // aggregate throughput report
            // let now = get_local_millis();
            // if now - last_report_time >= 1000 {
            //     println!(" --- [HFT Engine Performance Report] --- ");
            //     println!(" [Speed test] parallel processing speed: {} msg/s", msg_count);
            //     println!(" [Clock dynamic offset] current system bias correction: {} ms", dynamic_clock_offset);

            //     for (key, lock) in &manager_for_processor.books {
            //         // inner tokio lock, await
            //         let b = lock.read().await;
            //         println!("    -> pairs: {}.{} | status: {:?} ", key.exchange, key.symbol, b.status);
            //     }
            //     println!("--------------------------------");
            //     msg_count = 0;
            //     last_report_time = now;
            // }
        }
    });
    // ------------------------------------------------
    // [task 3]: strategy monitoring (consumer)
    // ------------------------------------------------
    let strat_config = config.strategy.clone();
    let manager_for_consumer = Arc::clone(&manager);
    // copy the static exchange ptr and the symbol list
    let ex_static_consumer = ex_static;
    let target_symbols = config.tradingpairs.symbols.clone();
    let depth = strat_config.signal_depth as usize;
    // let depth = config.strategy.clone().signal_depth as usize;

    // snapshot scheduler: default 1h buckets
    let mut scheduler = SnapshotScheduler::new("1h");
    let consumer = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.engine.warmup_secs)).await;

        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(50));
        loop {
            interval.tick().await;
            let current_time = get_local_millis();

            // let the scheduler check whether we crossed a bucket boundary
            let trigger_snapshot = scheduler.should_trigger(Utc::now());

            for symbol in &target_symbols {
                if let Some(book_lock) = manager_for_consumer.get_book(ex_static, symbol) {
                    // take a write lock (we may overwrite last_snapshot_request_time / status), hence mut b
                    let mut b = book_lock.write().await;

                    // on-the-hour snapshot backup: call write_batch_to_parquet
                    if trigger_snapshot && b.status == BookStatus::Synced {
                        // keys are i64, deref them directly
                        // let bids_snap: Vec<(i64, f64)> = b.bids.iter().map(|(p, q)| (*p, *q)).collect();
                        let bids_snap: Vec<(i64, f64)> = b.bids.iter().map(|(&p, &q)| (p, q)).collect();
                        let asks_snap: Vec<(i64, f64)> = b.asks.iter().map(|(&p, &q)| (p, q)).collect();

                        // unified parquet writer (is_snapshot = true)
                        write_batch_to_parquet(symbol, current_time, bids_snap, asks_snap, base_data_path, true);

                    }
                    match b.status {
                        BookStatus::Synced => {
                            if let Some(mid) = b.get_mid_price() {
                                let imbalance = b.get_deep_imbalance(depth).unwrap_or(0.0);
                                // println!("[Live] {} |Current mid: {} | {depth} layers imbalance: {:.4}", symbol, mid, imbalance);
                            }
                        }
                        BookStatus::Stale => {
                            // eprintln!("[TOXIC FLOW] {} | book status: Stale, stopping signal generating.", symbol);
                            continue;
                        }
                        BookStatus::WaitingForSnapshot => {
                            // cooldown: only re-request a snapshot every ~5s to avoid REST rate limits
                            if current_time - b.last_snapshot_request_time > 5000 {
                                println!("[Self-healing] detected {} book in gap, starting background asynchronous snapshot.", symbol);

                                b.last_snapshot_request_time = current_time;
                                let manager_heal = Arc::clone(&manager_for_consumer);
                                let symbol_heal = symbol.clone();

                                // run async so the loop isn't blocked
                                tokio::spawn(async move {
                                    match fetch_snapshot(&symbol_heal).await {
                                        Ok(snapshot) => {
                                            if let Some(inner_book_lock) = manager_heal.get_book(ex_static_consumer, &symbol_heal) {
                                                let mut inner_b = inner_book_lock.write().await;

                                                let bids_borrowed: Vec<(&str, &str)> = snapshot.bids.iter()
                                                    .filter(|k|k.len() == 2)
                                                    .map(|k| (k[0].as_str(), k[1].as_str()))
                                                    .collect();

                                                let asks_borrowed: Vec<(&str, &str)> = snapshot.asks.iter()
                                                .filter(|k|k.len() == 2)
                                                .map(|k| (k[0].as_str(), k[1].as_str()))
                                                .collect();

                                                inner_b.load_snapshot(snapshot.last_update_id, &bids_borrowed, &asks_borrowed);
                                                println!("[Self-healing] {} book gap repaired, book status: Synced!", symbol_heal);
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("[Self-healing] failed to fetch snapshot for {}: {:?}", symbol_heal, e);
                                        }
                                    }
                                });
                            } else {
                                println!("[Waiting] {} | book not ready yet.", symbol);
                            }
                        } // end WaitingForSnapshot arm
                    }
                }
            }
        }
    });
    let _ = consumer.await;
}
