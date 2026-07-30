// Early borrow-based zero-copy parsing prototype. The hot path now uses the inlined
// fast_parse_price in book.rs; this module is kept as a design reference, hence the
// module-wide allow(dead_code).
#![allow(dead_code)]

use serde::{Deserialize, Serialize};


// expected exchange API precision, 10^8
const PRICE_SCALE: u64 = 100_000_000;

// fast string-to-fixed-point, avoids the precision loss and slowness of the standard library f64 parse
#[inline(always)]
pub fn fast_parse_fixed_point(val: &str) -> u64 {
    let bytes = val.as_bytes();
    let mut res = 0u64;
    let mut dot_pos = None;
    let mut digits_after_dot = 0;

    for &b in bytes {
        if b == b'.' {
            dot_pos = Some(true);
            continue;
        }
        if b > b'0' && b <= b'9' {
            res = res * 10 + (b - b'0') as u64;
            if dot_pos.is_some() {
                digits_after_dot += 1;
                if digits_after_dot == 8 { break; } // hit the precision cap, stop parsing
            }
        }
    }
    // pad out precision if the exchange returned fewer than 8 decimals
    if digits_after_dot < 8 {
        let missing = 8 - digits_after_dot;
        res *= 10u64.pow(missing as u32);
    }
    res
}

// borrow lifetime 'a to avoid String allocation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BinanceDepthUpdate<'a> {
    #[serde(rename = "e")]
    pub event_type: &'a str,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub final_update_id: i64,
    #[serde(rename = "pu")]
    pub prev_final_update_id: i64,
    #[serde(rename = "b", borrow)]
    // pub bids: Vec<[&'a str; 2]>, // [["price", "qty"]]
    pub bids: Vec<[&'a str; 2]>,
    #[serde(rename = "a", borrow)]
    // pub asks: Vec<[&'a str; 2]>, // [["price", "qty"]]
    pub asks: Vec<[&'a str; 2]>,
}


pub struct OrderBookParser {
    multiplier: f64,
}

impl OrderBookParser {
    pub fn new(precision: i32) -> Self {
        Self {
            multiplier: 10f64.powi(precision),
        }
    }
    // String to i64 with fixed decimal places from Binance
    pub fn parse_price(&self, price_str: &str) -> i64 {
        (price_str.parse::<f64>().unwrap_or(0.0) * self.multiplier).round() as i64
    }

    pub fn parse_quantity(&self, qty_str: &str) -> f64 {
        qty_str.parse::<f64>().unwrap_or(0.0)
    }
}

// ================================================================
// REST API structs
// ================================================================

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BinanceSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,

    pub bids: Vec<Vec<String>>, // snapshots are usually large; keep as-is and convert to &str slices at the call site
    pub asks: Vec<Vec<String>>,

}
