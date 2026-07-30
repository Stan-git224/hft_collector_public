use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::init_market::MarketFilters;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BookStatus {
    WaitingForSnapshot, // init, waiting for the first snapshot or reconnection needed , can't process any data
    Synced, // synced with snapshot, can process data
    Stale, // latency too high, data is stale (toxic, unsafe)
}

pub struct OrderBook {
    pub bids: BTreeMap<i64, f64>, // price from high to low (BTreeMap: low to high in default)
    pub asks: BTreeMap<i64, f64>,
    pub last_update_id: i64,
    pub inv_multiplier: f64,
    pub status: BookStatus, // track the book's status
    pub last_snapshot_request_time: i64, // throttle requests to avoid rate limits
}

impl OrderBook {
    pub fn new(_precision: i32) -> Self {
        // CORE decision: whatever precision the exchange reports, force everything to 8 decimals in memory (1.0 / 10^8)
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            inv_multiplier: 0.00000001f64, // 1.0 / 10f64.powi(8),
            status: BookStatus::WaitingForSnapshot,
            last_snapshot_request_time: 0,
        }
    }
    pub fn load_snapshot(
        &mut self,
        last_update_id: i64,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
    ){
        // 1. clean old data
        self.bids.clear();
        self.asks.clear();

        // 2. apply snapshot data
        self.update_levels(bids, asks);

        // 3. save the snapshot's id
        self.last_update_id = last_update_id;
        self.status = BookStatus::Synced;
        println!("Snapshot loaded ID: {}, Status: {:?}", last_update_id, self.status);
    }

    #[inline(always)]
    pub fn apply_update(&mut self, price: i64, quantity: f64, is_bid: bool) {
        let target_map = if is_bid { &mut self.bids} else { &mut self.asks};

        if quantity == 0.0 {
            target_map.remove(&price);
        } else {
            target_map.insert(price, quantity);
        }
    }
    // get best bid and ask quickly?
    #[inline(always)]
    pub fn get_best_bid(&self) -> Option<(&i64, &f64)> {
        self.bids.iter().next_back() // BTreeMap: last element is the highest price
    }
    #[inline(always)]
    pub fn get_best_ask(&self) -> Option<(&i64, &f64)> {
        self.asks.iter().next()
    }
    pub fn handle_event(&mut self, first_update_id: i64, final_update_id: i64, prev_final_update_id: i64) -> bool {
        // first_update_id: this packet's start U; final_update_id: its end u; prev_final_update_id: the previous packet's end pu
        // 1. Lock: if no snapshot loaded, reject to update
        if self.status == BookStatus::WaitingForSnapshot {
            println!("Stop update. Waiting for new snapshot.");
            return false;
        }
        // 2. standard futures filter: if this packet ends older than our book, drop it, e.g. last = 100, u = 96
        if final_update_id <= self.last_update_id {
            return false;
        }
        // pu > last_update_id means we missed something in between
        if prev_final_update_id > self.last_update_id {
            println!("[Gap detected] current account ID: {}, pkg pu: {}, range: {} - {}", self.last_update_id, prev_final_update_id, first_update_id, final_update_id);
            self.status = BookStatus::WaitingForSnapshot;
            return false;
        }
        self.last_update_id = final_update_id;

        // advanced successfully and not flagged Stale externally -> stay Synced; if Stale, keep it and let main.rs decide by latency
        if self.status == BookStatus::Stale {} else {self.status = BookStatus::Synced;}
        true
    }

    // parse and apply levels using our own fast parser, dropping the dependency on the external parser
    pub fn update_levels(&mut self, bids: &[(&str, &str)], asks: &[(&str, &str)]) {
        // update bids
        for &(p_str, q_str) in bids {
            let price = Self::fast_parse_price(p_str);
            let qty = Self::fast_parse_qty(q_str);
            self.apply_update(price, qty, true);
        }
        for &(p_str, q_str) in asks {
            let price = Self::fast_parse_price(p_str);
            let qty = Self::fast_parse_qty(q_str);
            self.apply_update(price, qty, false);
        }
    }
    pub fn to_f64(&self, price: i64) -> f64 {
        price as f64 * self.inv_multiplier
    }

    pub fn get_mid_price(&self) -> Option<f64> {
        let bid = self.get_best_bid()?.0;
        let ask = self.get_best_ask()?.0;
        // (best bid + best ask) /2
        Some((self.to_f64(*bid) + self.to_f64(*ask)) / 2.0)
    }
    pub fn get_deep_imbalance(&self, depth: usize) -> Option<f64> {
        // 1. quantity sum of the Nth bid (BTreeMap: from small to large, so we need to reverse)
        let bid_vol: f64 = self.bids.values().rev().take(depth).sum();
        // 2. quantity sum of the Nth ask
        let ask_vol: f64 = self.asks.values().take(depth).sum();
        if bid_vol + ask_vol == 0.0 {return None;}
        Some((bid_vol - ask_vol) / (bid_vol + ask_vol))
    }
    // pure-numeric hot path, used by the criterion benchmark (the binary never calls it directly)
    #[allow(dead_code)]
    pub fn update_pure_numbers(&mut self, bids: &[(i64, f64)], asks: &[(i64, f64)]) {
        // handle bids diff
        for &(price, qty) in bids {
            if qty == 0.0 {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, qty);
            }
        }
        for &(price, qty) in asks {
            if qty == 0.0 {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, qty);
            }
        }
    }

    

// =======================================================================
// custom fast parser: hand-rolled to match C++-level speed and skip the standard library's heavy float parsing
// =======================================================================

    #[inline(always)]
    pub fn fast_parse_price(s: &str) -> i64 {
        let bytes = s.as_bytes(); // e.g. "0.0988" -> [48, 46, 48, 57, 56, 56]
        let mut res = 0i64;
        // let mut dot_pos = None;
        let mut has_dot = false;
        let mut digits_after_dot = 0;

        for &b in bytes {
            if b == b'.' { // hit the decimal point (46)
                has_dot = true;
                continue; // skip the dot and keep going
            }
            if b >= b'0' && b <= b'9' { // make sure it's a digit 0-9
                if has_dot {
                    if digits_after_dot < 8 {
                        res = res * 10 + (b - b'0') as i64; // b - b'0' is the trick: b'9' (57) - 48 = 9, ASCII byte to digit
                        digits_after_dot += 1;
                    }
                    // truncate anything past 8 decimals
                } else {
                    res = res * 10 + (b - b'0') as i64;
                }
            }
        }
        if !has_dot {
            res *= 100_000_000;
        } else if digits_after_dot < 8 {
            res *= 10i64.pow(8 - digits_after_dot);
        }
        res
    }

    #[inline(always)]
    pub fn fast_parse_qty(s: &str) -> f64 {
        // qty feeds depth math, so just parse straight to f64
        s.parse::<f64>().unwrap_or(0.0)
    }
}

// =======================================================================
// BookManager: allocation-free composite key for faster hashmap lookups; BookKey and BookManager live in their own scope
// =======================================================================

// allocation-free composite hashmap key (avoids a format!("{}.{}", ex, sym) on every lookup)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BookKey {
    pub exchange: String,
    pub symbol: String,
}

pub struct BookManager {
    pub books: HashMap<BookKey, Arc<RwLock<OrderBook>>>,
    pub filters_map: HashMap<BookKey, MarketFilters>,
}

impl BookManager {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
            filters_map: HashMap::new(),
        }
    }
    pub fn register_pair(&mut self, exchange: &str, symbol: &str, filters: MarketFilters) {
        let key = BookKey {
            exchange: exchange.to_lowercase(),
            symbol: symbol.to_uppercase(),
        };
        let book = RwLock::new(OrderBook::new(filters.price_precision as i32));
        self.books.insert(key.clone(), Arc::new(book));
        self.filters_map.insert(key, filters);
    }

    // temporary allocation-free view for the HashMap match, avoids a String copy
    pub fn get_book(&self, exchange: &str, symbol: &str) -> Option<Arc<RwLock<OrderBook>>> {
        // match on fields of the same shape via lifetimes
        // note: to fully avoid the temporary String, BookKey could be split into a &str Borrow form
        // this is the standard, fast-enough lookup:
        let target_ex = exchange.to_lowercase();
        let target_sym = symbol.to_uppercase();

        let query_key = BookKey { exchange: target_ex, symbol: target_sym};
        self.books.get(&query_key).cloned()
    }
}
