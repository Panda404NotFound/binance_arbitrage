use std::sync::Arc;
use tokio::time::Instant;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use log::info;
//use prometheus::{Registry, Gauge, IntCounter};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderBookEntry {
    pub price: f64,
    pub volume: f64,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookUpdate {
    pub symbol: String,
    pub asks_updates: Vec<OrderBookEntry>,
    pub bids_updates: Vec<OrderBookEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenDataUpdate {
    pub symbol: String,
    pub order_book_update: OrderBookUpdate,
}

#[derive(Clone)]
pub struct DataEntry {
    pub version: u64,
    pub data: TokenDataUpdate,
}

pub struct LockFreeTokenDataStore {
    pub data: DashMap<String, DataEntry>,
    pub update_queue: SegQueue<(String, DataEntry)>,
}

#[allow(dead_code)]
impl LockFreeTokenDataStore {
    pub fn new() -> Self {
        LockFreeTokenDataStore {
            data: DashMap::new(),
            update_queue: SegQueue::new(),
        }
    }

    pub fn update_entry(&self, symbol: String, update: TokenDataUpdate) {
        let version = self.get_next_version(&symbol);
        let entry = DataEntry { version, data: update };
        self.update_queue.push((symbol.clone(), entry.clone()));
        self.data.insert(symbol, entry);
    }

    fn get_next_version(&self, symbol: &str) -> u64 {
        self.data.get(symbol)
            .map(|entry| entry.version + 1)
            .unwrap_or(1)
    }

    pub fn remove_token_data(&self, symbol: &str) {
        self.data.remove(symbol);
        let new_queue = SegQueue::new();
        while let Some((s, entry)) = self.update_queue.pop() {
            if s != symbol {
                new_queue.push((s, entry));
            }
        }
        while let Some((s, entry)) = new_queue.pop() {
            self.update_queue.push((s, entry));
        }
    }

    pub fn get_store_size(&self) -> usize {
        self.data.len()
    }
    
    #[allow(unused_variables)]
    pub fn update_order_book(&self, symbol: &str, updates: OrderBookUpdate) {
        if let Some(mut entry) = self.data.get_mut(symbol) {
            let order_book = &mut entry.data.order_book_update;
            
            // Метрики для asks
            let asks_start = Instant::now();
            let mut asks_count = 0;
            
            // Обновляем asks
            for ask in updates.asks_updates {
                asks_count += 1;
                if ask.volume == 0.0 {
                    order_book.asks_updates.retain(|a| a.price != ask.price);
                } else {
                    if let Some(existing_ask) = order_book.asks_updates.iter_mut().find(|a| a.price == ask.price) {
                        existing_ask.volume = ask.volume;
                    } else {
                        order_book.asks_updates.push(ask);
                    }
                }
            }
            order_book.asks_updates.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
            order_book.asks_updates.truncate(10);
            
            let asks_duration = asks_start.elapsed();
            
            // Метрики для bids
            let bids_start = Instant::now();
            let mut bids_count = 0;
            
            // Обновляем bids
            for bid in updates.bids_updates {
                bids_count += 1;
                if bid.volume == 0.0 {
                    order_book.bids_updates.retain(|b| b.price != bid.price);
                } else {
                    if let Some(existing_bid) = order_book.bids_updates.iter_mut().find(|b| b.price == bid.price) {
                        existing_bid.volume = bid.volume;
                    } else {
                        order_book.bids_updates.push(bid);
                    }
                }
            }
            order_book.bids_updates.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
            order_book.bids_updates.truncate(10);
            
            // Проверка и коррекция пересечения ask и bid
            if let (Some(lowest_ask), Some(highest_bid)) = (order_book.asks_updates.first(), order_book.bids_updates.first()) {
                if highest_bid.price >= lowest_ask.price {
                    let highest_bid_price = highest_bid.price;
                    let lowest_ask_price = lowest_ask.price;
                    // Удаляем пересекающиеся ордера
                    order_book.asks_updates.retain(|ask| ask.price > highest_bid_price);
                    order_book.bids_updates.retain(|bid| bid.price < lowest_ask_price);
                }
            }

            let bids_duration = bids_start.elapsed();
            
            // Логирование метрик
            #[cfg(debug_assertions)]
            info!("Order book update for {}: Asks (count: {}, time: {:?}), Bids (count: {}, time: {:?})",
            symbol, asks_count, asks_duration, bids_count, bids_duration);
            
            // Вызов новой функции для вывода стакана
            #[cfg(debug_assertions)]
            self.log_order_book(symbol, order_book);
        }
    }

    fn log_order_book(&self, symbol: &str, order_book: &OrderBookUpdate) {
        info!("Order book for {}", symbol);
        info!("Asks (top 5):");
        for (i, ask) in order_book.asks_updates.iter().take(5).enumerate() {
            info!("  {}: Price: {}, Volume: {}", i + 1, ask.price, ask.volume);
        }
        info!("Bids (top 5):");
        for (i, bid) in order_book.bids_updates.iter().take(5).enumerate() {
            info!("  {}: Price: {}, Volume: {}", i + 1, bid.price, bid.volume);
        }
    }
}

pub type TokenDataStore = Arc<LockFreeTokenDataStore>;

pub fn update_token_data(store: &TokenDataStore, update: TokenDataUpdate) {
    store.update_entry(update.symbol.clone(), update);
}

#[allow(dead_code)]
pub fn format_price(price: f64) -> String {
    let mut formatted = format!("{:.10}", price);
    
    if let Some(decimal_pos) = formatted.find('.') {
        let significant_part = &formatted[decimal_pos + 1..];
        
        if significant_part.chars().all(|c| c == '0') {
            return format!("{}.0", &formatted[..decimal_pos]);
        }
        
        let mut end = significant_part.len();
        while end > 0 && significant_part.chars().nth(end - 1) == Some('0') {
            end -= 1;
        }
        
        if end == 0 {
            return format!("{}.0", &formatted[..decimal_pos]);
        }
        
        if end > 10 {
            end = 10;
            while end < significant_part.len() && significant_part.chars().nth(end - 1) == Some('0') {
                end += 1;
            }
        }
        
        formatted = format!("{}.{}", &formatted[..decimal_pos], &significant_part[..end]);
    }
    
    formatted
}