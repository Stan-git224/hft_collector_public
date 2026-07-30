use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize,Deserialize)]
pub struct BinanceDepthUpdate {
    #[serde(rename = "e")]
    pub event_type: String,  // event type
    #[serde(rename = "E")]
    pub event_time: i64, // event time: when the exchange generated the event (ms)
    #[serde(rename = "T")]
    pub transaction_time: i64, // transaction time: when the matching engine actually executed the fill / book change (ms)
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub final_update_id: i64,
    #[serde(rename = "pu")]
    pub prev_final_update_id: i64, // perps-only: the previous packet's end u
    #[serde(rename = "b")]
    pub bids: Vec<Vec<String>>, // [["price", "qty"], ...]
    #[serde(rename = "a")]
    pub asks: Vec<Vec<String>>, // [["price", "qty"], ...]
}


#[derive(Serialize, Deserialize)]
pub struct BinanceSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    pub bids: Vec<Vec<String>>, // [["price", "qty"], ...]
    pub asks: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct MarketMessage {
    pub exchange: &'static str, // &'static str, avoids per-message allocation ("binance", "bybit", "okx")
    pub symbol: String,  // "BTCUSDT", "ETHUSDT"
    pub raw_json: String, // raw JSON string
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub engine: EngineConfig,
    pub network: NetworkConfig,
    pub strategy: StrategyConfig,
    pub tradingpairs: TradingPairsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EngineConfig {
    pub channel_buffer_size: usize,
    pub warmup_secs: u64,
    // kept to mirror config.toml, read by the tuning module
    #[allow(dead_code)]
    pub clock_tuning_threshold_vol: i64,
    #[allow(dead_code)]
    pub report_interval_ms: u64
}

#[derive(Debug, Deserialize, Clone)]
pub struct NetworkConfig {
    #[serde(rename = "latency_threshold_ms")]
    pub latency_threshold: i64,
    #[serde(rename = "base_network_floor_ms")]
    pub base_network_floor: i64,
    pub pkt_loop_threshold: i64
}

#[derive(Debug, Deserialize, Clone)]
pub struct StrategyConfig {
    #[allow(dead_code)]
    pub monitor_interval_ms: u64,
    pub signal_depth: u64
}

#[derive(Debug, Deserialize, Clone)]
pub struct TradingPairsConfig {
    pub exchange: String,
    pub symbols: Vec<String>,
}
