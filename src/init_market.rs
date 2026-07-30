use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use crate::model::AppConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFilters {
    pub symbol: String,
    pub price_precision: usize,
    pub qty_precision: usize, 
    pub tick_size: f64,  // minimum price tick
    pub step_size: f64,  // minimum quantity step
    pub min_qty: f64 
}

// load config.toml
pub fn load_config() -> AppConfig {
    let mut file = File::open("config.toml").expect("Failed to find config.toml");
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    toml::from_str(&contents).expect("config.toml 解析失敗")
}

// core function: call Binance rest API
pub async fn sync_exchange_info(config: &AppConfig) -> HashMap<String, MarketFilters> {
    println!("[Bootstrapping] get exchange info from Binance Futures...");

    let url = format!("https://fapi.binance.com/fapi/v1/exchangeInfo");
    let response: serde_json::Value = reqwest::get(url)
    .await
    .expect("Failed to fetch exchange info")
    .json()
    .await
    .expect("Exchange Info JSON parse failed.");

    let mut filtered_market_data = HashMap::new();
    // HashSet for quick membership checks against the symbols in config
    let target_symbols: HashSet<String> = config.tradingpairs.symbols
    .iter().map(|s| s.to_uppercase()).collect();

    if let Some(symbols_array) = response["symbols"].as_array() {
        for s in symbols_array {
            let symbol_str = s["symbol"].as_str().unwrap_or("").to_string();
            
            // if this symbol is listed in config.toml, clean it up and store it
            if target_symbols.contains(&symbol_str) {
                let price_precision = s["pricePrecision"].as_u64().unwrap_or(2) as usize;
                let qty_precision = s["quantityPrecision"].as_u64().unwrap_or(2) as usize;
                
                let mut tick_size = 0.0;
                let mut step_size = 0.0;
                let mut min_qty = 0.0;

                // parse Binance's filters array
                if let Some(filters) = s["filters"].as_array() {
                    for f in filters {
                        match f["filterType"].as_str() {
                            Some("PRICE_FILTER") => {
                                tick_size = f["tickSize"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                            }
                            Some("LOT_SIZE") => {
                                step_size = f["stepSize"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                                min_qty = f["minQty"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                            }
                            _ => {}
                        }
                    }
                }
                let filters = MarketFilters {
                    symbol: symbol_str.clone(),
                    price_precision,
                    qty_precision,
                    tick_size,
                    step_size,
                    min_qty,
                };
                filtered_market_data.insert(symbol_str, filters);
            }
        }
    }

    // write the cleaned, slimmed-down format to src/exchange_info.json
    let json_string = serde_json::to_string_pretty(&filtered_market_data).unwrap();
    let mut file = File::create("src/binance_exchange_info.json").expect("Failed to create exchange_info.json");
    file.write_all(json_string.as_bytes()).unwrap();

    println!("[ExchangeInfo] Synced exchange info to src/binance_exchange_info.json, total {} symbols", filtered_market_data.len());
    filtered_market_data
}
