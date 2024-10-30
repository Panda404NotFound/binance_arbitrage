use log::info;
use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow};
use hashbrown::HashMap;
use serde_json::Value;
use tokio::time::sleep;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub lot_size_step_size: String,
    pub lot_size_min_qty: String,
    pub lot_size_max_qty: String,
    pub market_lot_size_max_qty: String,
    pub price_filter_min_price: String,
    pub price_filter_max_price: String,
    pub price_filter_tick_size: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ExchangeInfoResponse {
    pub timezone: String,
    pub serverTime: u64,
    pub rateLimits: Vec<RateLimit>,
    pub exchangeFilters: Vec<serde_json::Value>,
    pub symbols: Vec<Symbol>,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RateLimit {
    pub rateLimitType: String,
    pub interval: String,
    pub intervalNum: u32,
    pub limit: u32,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Symbol {
    pub symbol: String,
    pub status: String,
    pub baseAsset: String,
    pub quoteAsset: String,
    pub filters: Vec<Filter>,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "filterType")]
pub enum Filter {
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter {
        minPrice: String,
        maxPrice: String,
        tickSize: String,
    },
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        minQty: String,
        maxQty: String,
        stepSize: String,
    },
    #[serde(rename = "MARKET_LOT_SIZE")]
    MarketLotSize {
        minQty: String,
        maxQty: String,
        stepSize: String,
    },
    #[serde(other)]
    Unknown,
}

pub struct StepSizeStore {
    pub symbols: HashMap<String, SymbolInfo>,
}

impl StepSizeStore {
    pub fn new() -> Self {
        StepSizeStore {
            symbols: HashMap::new(),
        }
    }
}

#[allow(unused_variables)]
pub async fn get_step_sizes(symbols: Vec<String>) -> Result<StepSizeStore, Box<dyn std::error::Error + Send + Sync>> {
    let mut all_step_sizes = StepSizeStore::new();
    let chunk_size = 200;

    for (i, chunk) in symbols.chunks(chunk_size).enumerate() {
        //info!("Обработка чанка {} (символы {} - {})", i + 1, i * chunk_size, i * chunk_size + chunk.len());
        let chunk_symbols = chunk.to_vec();
        let chunk_step_sizes = get_chunk_step_sizes(chunk_symbols).await?;
        all_step_sizes.symbols.extend(chunk_step_sizes.symbols);

        //info!("Чанк {} обработан. Ожидание 250 мс", i + 1);
        sleep(Duration::from_millis(250)).await;
    }

    //info!("Все чанки обработаны. Всего символов: {}", all_step_sizes.symbols.len());
    Ok(all_step_sizes)
}

async fn get_chunk_step_sizes(symbols: Vec<String>) -> Result<StepSizeStore, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let symbols_param = symbols.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",");
    let url = format!("https://api.binance.com/api/v3/exchangeInfo?symbols=[{}]", symbols_param);

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        info!("Ошибка при запросе: статус {}", response.status());
        return Err(anyhow!("Ошибка при запросе: статус {}", response.status()).into());
    }

    let response_text = response.text().await?;
    let data: Value = serde_json::from_str(&response_text)?;
    let mut step_sizes = StepSizeStore::new();

    if let Some(symbols_data) = data["symbols"].as_array() {
        for symbol in symbols_data {
            if let (Some(symbol_name), Some(filters)) = (symbol["symbol"].as_str(), symbol["filters"].as_array()) {
                let mut info = SymbolInfo {
                    symbol: symbol_name.to_string(),
                    lot_size_step_size: String::new(),
                    lot_size_min_qty: String::new(),
                    lot_size_max_qty: String::new(),
                    market_lot_size_max_qty: String::new(),
                    price_filter_min_price: String::new(),
                    price_filter_max_price: String::new(),
                    price_filter_tick_size: String::new(),
                };

                for filter in filters {
                    match filter["filterType"].as_str() {
                        Some("LOT_SIZE") => {
                            info.lot_size_step_size = filter["stepSize"].as_str().unwrap_or("").to_string();
                            info.lot_size_min_qty = filter["minQty"].as_str().unwrap_or("").to_string();
                            info.lot_size_max_qty = filter["maxQty"].as_str().unwrap_or("").to_string();
                        },
                        Some("MARKET_LOT_SIZE") => {
                            info.market_lot_size_max_qty = filter["maxQty"].as_str().unwrap_or("").to_string();
                        },
                        Some("PRICE_FILTER") => {
                            info.price_filter_min_price = filter["minPrice"].as_str().unwrap_or("").to_string();
                            info.price_filter_max_price = filter["maxPrice"].as_str().unwrap_or("").to_string();
                            info.price_filter_tick_size = filter["tickSize"].as_str().unwrap_or("").to_string();
                        },
                        _ => {}
                    }
                }

                step_sizes.symbols.insert(symbol_name.to_string(), info);
            }
        }
    }

    //info!("Обработано {} символов из чанка", step_sizes.symbols.len());
    Ok(step_sizes)
}