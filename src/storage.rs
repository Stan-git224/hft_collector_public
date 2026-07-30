use std::fs::{create_dir_all, OpenOptions};
use std::path::Path;
use std::sync::Arc;
use chrono::{Utc, Timelike, DateTime};
use tokio::sync::mpsc;
use arrow::array::{Int64Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

// =========================================
// 1. data carrier for high-frequency storage and alerts
// =========================================

#[derive(Debug, Clone)]
pub struct TickData {
    // local receive timestamp; kept on the row when flushed, for debugging / replay alignment
    #[allow(dead_code)]
    pub timestamp: i64,
    pub symbol: String,
    pub price: i64,
    pub qty: f64,
    pub is_bid: bool,
}


pub fn write_batch_to_parquet(
    symbol: &str,
    timestamp: i64,
    bids: Vec<(i64, f64)>,
    asks: Vec<(i64, f64)>,
    base_path: &str,
    is_snapshot: bool,
) {
    // if the data is empty, reject to build parquet file to avoid Arrow crash.
    if bids.is_empty() && asks.is_empty() {
        return;
    }

    let path_str = base_path.to_string();
    let symbol_str = symbol.to_string().to_uppercase();

    tokio::spawn(async move {
        let current_time = Utc::now().format("%Y%m%d-%H%M%S").to_string();
        let (dir_type, flie_prefix) = if is_snapshot {
            ("snapshots", "book_snap")
        } else {
            ("incremental", "tick_diff")
        };

        let target_dir = format!("{}/{}/{}", path_str, dir_type, symbol_str);

        // ensure the folder exists.
        if let Err(e) = std::fs::create_dir_all(&target_dir) {
            eprintln!("[Storage Error] 無法建立幣別資料夾及檔案: {}: {:?}", target_dir, e);
            return;
        }
        let file_path = format!("{}/{}_{}.parquet", target_dir, flie_prefix, current_time);


        let total_len = bids.len() + asks.len();
        let mut ts_vec = Vec::with_capacity(total_len);
        let mut price_vec = Vec::with_capacity(total_len);
        let mut qty_vec = Vec::with_capacity(total_len);
        let mut side_vec = Vec::with_capacity(total_len);

        for (p, q) in bids {
            ts_vec.push(timestamp);
            price_vec.push(p);
            qty_vec.push(q);
            side_vec.push("BID");
        }
        for (p, q) in asks {
            ts_vec.push(timestamp);
            price_vec.push(p);
            qty_vec.push(q);
            side_vec.push("ASK");
        }

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("qty", arrow::datatypes::DataType::Float64, false),
        arrow::datatypes::Field::new("side", arrow::datatypes::DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
                    Arc::new(Int64Array::from(ts_vec)),
                    Arc::new(Int64Array::from(price_vec)),
                    Arc::new(Float64Array::from(qty_vec)),
                    Arc::new(StringArray::from(side_vec)),
        ],
    ).unwrap();

    let file = OpenOptions::new().create(true).write(true).append(!is_snapshot).truncate(is_snapshot).open(Path::new(&file_path)).unwrap();
    let props = WriterProperties::builder().set_compression(parquet::basic::Compression::SNAPPY).build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // println!("[Storage Log] successfully wrote data block asynchronously: {} -> total {} rows.", file_path, total_len);
    });
}

// incremental pipeline: flush every 10000 ticks or every 10 seconds
pub fn spawn_incremental_storage_engine(mut rx: mpsc::Receiver<TickData>, base_path: &str) {
    let path_str = base_path.to_string();
    let buffer_size = 10000;
    let flush_interval = 10;
    tokio::spawn(async move {
        create_dir_all(format!("{}/incremental", path_str)).unwrap();
        create_dir_all(format!("{}/snapshots", path_str)).unwrap();

        let mut buffer: Vec<TickData> = Vec::with_capacity(buffer_size); // in-memory batch buffer to avoid hitting disk too often, 10000
        let mut last_flush_time = Utc::now().timestamp();

        while let Some(tick) = rx.recv().await {
            buffer.push(tick);
            let now = Utc::now().timestamp();

            if buffer.len() >= buffer_size || (now - last_flush_time >= flush_interval && !buffer.is_empty()) {
                let bids: Vec<(i64, f64)> = buffer.iter().filter(|d| d.is_bid).map(|d| (d.price, d.qty)).collect();
                let asks: Vec<(i64, f64)> = buffer.iter().filter(|d| !d.is_bid).map(|d| (d.price, d.qty)).collect();

                if let Some(first) = buffer.first() {
                    write_batch_to_parquet(&first.symbol, now*1000, bids, asks, &path_str, false);
                    buffer.clear();
                    last_flush_time = now;
                }

            }
        }
    });
}

pub struct SnapshotScheduler {
    interval_str: String,
    last_triggered_bucket: i64
}

impl SnapshotScheduler {
    pub fn new(interval: &str) -> Self {
        Self {
            interval_str: interval.to_string(),
            last_triggered_bucket: -1
        }
    }

    // pass the current Utc; aligns to 00:00 automatically
    pub fn should_trigger(&mut self, now: DateTime<Utc>) -> bool {
        let total_minutes_since_midnight = (now.hour() * 60 + now.minute()) as i64;

        let interval_minutes = match self.interval_str.as_str() {
            "15m" => 15,
            "30m" => 30,
            "1h" => 60,
            "2h" => 120,
            "4h" => 240,
            "12h" => 720,
            _ => 60, // default 1 hour
        };
        // which time bucket we currently fall into
        let current_bucket = total_minutes_since_midnight / interval_minutes;
        let remainder = total_minutes_since_midnight % interval_minutes;

        // only when we just entered a new bucket, the minute aligns exactly (remainder == 0), and this bucket hasn't fired yet
        if remainder == 0 && current_bucket != self.last_triggered_bucket {
            self.last_triggered_bucket = current_bucket;
            return true;
        }
        false
    }
}