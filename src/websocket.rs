use crate::tokens::{TokenLiquidity, get_mandatory_tokens};
use crate::data::{LockFreeTokenDataStore, TokenDataUpdate, OrderBookUpdate, OrderBookEntry, update_token_data};
use crate::config::Config;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;

use log::info;

use url::Url;
use reqwest::Client;
use serde_json::{json, Value};
use anyhow::{Result, anyhow};

use futures::future;
use futures_util::SinkExt;
use async_channel::{Sender, unbounded};
use futures::stream::StreamExt;
use tokio::time::{sleep, Duration, Instant};
use tokio::sync::mpsc;
use std::sync::atomic::{AtomicBool, Ordering};
//use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use hashbrown::HashSet;

use tokio::net::TcpStream;
use prometheus::{IntCounter, IntGauge};

#[allow(dead_code)]
pub struct WebSocketManager {
    token_data_store: Arc<LockFreeTokenDataStore>,
    subscription_receiver: mpsc::Receiver<Vec<TokenLiquidity>>,
    current_subscriptions: HashSet<String>,
    http_client: Client,
    updating_subscriptions: AtomicBool,
    unused_pairs_receiver: mpsc::Receiver<Vec<String>>,
    token_update_sender: mpsc::Sender<Vec<String>>,
    metrics: WebSocketMetrics,
    write: Option<futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    config: Arc<Config>,
}

#[allow(dead_code)]
struct WebSocketMetrics {
    messages_processed: IntCounter,
    active_connections: IntGauge,
}

impl WebSocketMetrics {
    fn new() -> Self {
        WebSocketMetrics {
            messages_processed: IntCounter::new("websocket_messages_processed", "Total number of WebSocket messages processed").unwrap(),
            active_connections: IntGauge::new("websocket_active_connections", "Number of active WebSocket connections").unwrap(),
        }
    }
}

impl WebSocketManager {
    pub fn new(
        token_data_store: Arc<LockFreeTokenDataStore>,
        subscription_receiver: mpsc::Receiver<Vec<TokenLiquidity>>,
        unused_pairs_receiver: mpsc::Receiver<Vec<String>>,
        token_update_sender: mpsc::Sender<Vec<String>>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            token_data_store,
            subscription_receiver,
            current_subscriptions: HashSet::new(),
            http_client: Client::new(),
            updating_subscriptions: AtomicBool::new(false),
            unused_pairs_receiver,
            token_update_sender,
            metrics: WebSocketMetrics::new(),
            write: None,
            config,
        }
    }

    pub async fn run(&mut self, initial_tokens: Vec<TokenLiquidity>) -> Result<()> {
        let url = Url::parse("wss://stream.binance.com:9443/ws")?;
        self.current_subscriptions = initial_tokens.iter().map(|t| t.symbol.clone()).collect();

        // Получаем обязательные токены
        let mandatory_tokens = get_mandatory_tokens(&self.config).await;
        
        // Объединяем начальные и обязательные токены
        let mut all_tokens = [initial_tokens, mandatory_tokens].concat();

        let initial_order_books = self.get_initial_order_books(&mut all_tokens).await?;
    
        self.current_subscriptions = all_tokens.iter().map(|t| t.symbol.clone()).collect();
        
        info!("Received initial order books for {} tokens", initial_order_books.len());

        for (_symbol, token_data_update) in initial_order_books {
            update_token_data(&self.token_data_store, token_data_update);
        }

        info!("Initial TokenDataStore update completed");

        let (tx, _rx) = unbounded::<(String, OrderBookUpdate)>();

        loop {
            match self.connect_and_handle(&url, tx.clone()).await {
                Ok(_) => info!("WebSocket connection closed normally. Reconnecting..."),
                Err(e) => info!("WebSocket error: {:?}. Attempting to reconnect...", e),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub fn remove_token_from_structures(
        symbol: &str, 
        token_data_store: &Arc<LockFreeTokenDataStore>,
        token_liquidity: &mut Vec<TokenLiquidity>
    ) {
        // Удаление данных из LockFreeTokenDataStore
        token_data_store.remove_token_data(symbol);
        
        // Удаление токена из вектора TokenLiquidity
        token_liquidity.retain(|token| token.symbol != symbol);
    
        info!("Removed data for token: {} from token_data_store and TokenLiquidity", symbol);
    }
    
    async fn get_initial_order_books(&mut self, tokens: &mut Vec<TokenLiquidity>) -> Result<Vec<(String, TokenDataUpdate)>> {
        info!("Starting to fetch initial order books for {} tokens", tokens.len());
        
        let chunk_size = 50;
        let delay = Duration::from_millis(500);
        
        let mut all_order_books = Vec::new();
        let mut tokens_to_remove = Vec::new();

        for chunk in tokens.chunks(chunk_size) {
            let chunk_results = future::join_all(chunk.iter().map(|token| async {
                let result = self.get_initial_order_book(&token.symbol).await;
                (token.symbol.clone(), result)
            })).await;

            for (symbol, result) in chunk_results {
                match result {
                    Ok(order_book) => {
                        // Проверяем, не пуст ли ордербук
                        if !order_book.order_book_update.bids_updates.is_empty() && !order_book.order_book_update.asks_updates.is_empty() {
                            all_order_books.push((symbol.clone(), order_book));
                            info!("Added initial order book for {}", symbol);
                        } else {
                            info!("Skipped empty order book for {}", symbol);
                            tokens_to_remove.push(symbol);
                        }
                    },
                    Err(e) => {
                        info!("Error fetching initial order book for {}: {:?}", symbol, e);
                        tokens_to_remove.push(symbol);
                    }
                }
            }

            // Задержка перед следующим чанком
            sleep(delay).await;
        }

        // Удаляем токены с пустыми ордербуками или ошибками из всех структур
        for symbol in &tokens_to_remove {
            Self::remove_token_from_structures(symbol, &self.token_data_store, tokens);
        }

        info!("Completed fetching initial order books. Total tokens: {}, Valid order books: {}, Removed tokens: {}", 
              tokens.len(), all_order_books.len(), tokens_to_remove.len());

        if !tokens_to_remove.is_empty() {
            info!("Tokens removed due to empty order books or errors: {:?}", tokens_to_remove);
        }

        Ok(all_order_books)
    }

    async fn get_initial_order_book(&self, symbol: &str) -> Result<TokenDataUpdate> {
        let symbol = symbol.to_uppercase();

        let url = format!("https://api.binance.com/api/v3/depth?symbol={}&limit=5", symbol);
        
        let response = self.http_client.get(&url).send().await?;
        let data: Value = response.json().await?;
        
        let order_book = self.parse_order_book(&data, &symbol)?;
        
        //info!("Received initial order book for {}: {} bids, {} asks", 
        //    symbol, order_book.order_book_update.bids_updates.len(), order_book.order_book_update.asks_updates.len());
        
        Ok(order_book)
    }

    async fn send_subscriptions_in_chunks(&self, write: &mut futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>, tokens: Vec<String>) -> Result<()> {
        let chunk_size = 100;
        for chunk in tokens.chunks(chunk_size) {
            let subscribe_message = self.create_subscribe_message(chunk);
            write.send(Message::Text(subscribe_message.to_string())).await?;
            info!("Отправлено сообщение подписки для чанка: {}", subscribe_message);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(())
    }

    #[allow(unused_variables)]
    async fn connect_and_handle(&mut self, url: &Url, tx: Sender<(String, OrderBookUpdate)>) -> Result<()> {
        info!("Connecting to WebSocket...");
        let (ws_stream, _) = connect_async(url).await?;
        info!("WebSocket connected successfully.");
        let (mut write, mut read) = ws_stream.split();

        self.metrics.active_connections.inc();

        let tokens_to_subscribe: Vec<String> = self.current_subscriptions.iter().cloned().collect();
        self.send_subscriptions_in_chunks(&mut write, tokens_to_subscribe).await?;
        info!("Отправлены все начальные сообщения подписки");

        let last_data_time = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

        /* 

        let message_count = Arc::new(AtomicUsize::new(0));
        let message_count_clone = Arc::clone(&message_count);

        let monitoring_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                info!("WebSocket статистика: обработано сообщений: {}", message_count_clone.load(Ordering::Relaxed));
            }
        });

        tokio::spawn(async move {
            if let Err(e) = monitoring_task.await {
                eprintln!("Ошибка в задаче мониторинга: {:?}", e);
            }
        });

        */

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if last_data_time.elapsed() > Duration::from_secs(60 * 60 * 60) {
                        info!("No data received for 60 hours. Reconnecting...");
                        self.metrics.active_connections.dec();
                        return Ok(());
                    }
                }
                _ = ping_interval.tick() => {
                    write.send(Message::Ping(vec![])).await?;
                    info!("Ping sent");
                }
                Some(message) = read.next() => {
                    match message {
                        Ok(Message::Text(text)) => {
                            let start = Instant::now();
                            //message_count.fetch_add(1, Ordering::Relaxed);
                            
                            let v: Value = serde_json::from_str(&text)?;
                            self.process_message(&v, &tx).await?;
                            
                            info!("Время обработки сообщения WebSocket: {:?}", start.elapsed());
                        }
                        Ok(Message::Pong(_)) => {
                            info!("Pong received");
                        }
                        Ok(Message::Close(frame)) => {
                            info!("WebSocket closed: {:?}", frame);
                            self.metrics.active_connections.dec();
                            return Ok(());
                        }
                        Err(e) => {
                            info!("WebSocket error: {:?}", e);
                            self.metrics.active_connections.dec();
                            return Err(anyhow!("WebSocket error: {:?}", e));
                        }
                        _ => {}
                    }
                }
                result = self.subscription_receiver.recv() => {
                    if let Some(new_tokens) = result {
                        self.update_subscriptions(&mut write, new_tokens, Vec::new()).await?;
                    } else {
                        self.metrics.active_connections.dec();
                        return Ok(());
                    }
                }
                
                // Добавляем новую ветку для обработки неиспользуемых пар
                Some(unused_pairs) = self.unused_pairs_receiver.recv() => {
                    info!("Received unused pairs: {:?}", unused_pairs);
                    self.update_subscriptions(&mut write, Vec::new(), unused_pairs).await?;
                }
            }
        }
    }

    async fn update_subscriptions(
        &mut self,
        write: &mut futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut new_tokens: Vec<TokenLiquidity>,
        tokens_to_unsubscribe: Vec<String>
    ) -> Result<()> {
        
        if self.updating_subscriptions.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            info!("Subscription update already in progress, skipping...");
            return Ok(());
        }
    
        let new_subscriptions: HashSet<String> = new_tokens.iter().map(|t| t.symbol.clone()).collect();
        
        info!("Updating subscriptions. Current: {:?}, New: {:?}", self.current_subscriptions, new_subscriptions);

        let to_subscribe: Vec<String> = new_subscriptions.difference(&self.current_subscriptions).cloned().collect();
    
        let to_unsubscribe = tokens_to_unsubscribe
        .into_iter()
        .filter(|pair| self.current_subscriptions.contains(pair))
        .collect::<Vec<String>>();
    
        info!("Tokens to unsubscribe: {:?}", to_unsubscribe);
        info!("Tokens to subscribe: {:?}", to_subscribe);
    
        if !to_unsubscribe.is_empty() {
            let unsubscribe_message = self.create_unsubscribe_message(&to_unsubscribe);
            write.send(Message::Text(unsubscribe_message.to_string())).await?;
            info!("Successfully unsubscribed from: {:?}", to_unsubscribe);
            for symbol in &to_unsubscribe {
                self.current_subscriptions.remove(symbol);
                Self::remove_token_from_structures(symbol, &self.token_data_store, &mut new_tokens);
            }
        
            let updated_tokens: Vec<String> = self.current_subscriptions.iter().cloned().collect();
            if let Err(e) = self.token_update_sender.send(updated_tokens).await {
                info!("Failed to send token updates to Graph: {:?}", e);
            }
        }

        if !to_unsubscribe.is_empty() {
            let updated_tokens: Vec<String> = self.current_subscriptions.iter().cloned().collect();
            if let Err(e) = self.token_update_sender.send(updated_tokens).await {
                info!("Failed to send token updates to Graph: {:?}", e);
            }
        }
    
        if !to_subscribe.is_empty() {
            let mut tokens_to_fetch: Vec<TokenLiquidity> = new_tokens.iter()
                .filter(|t| to_subscribe.contains(&t.symbol))
                .cloned()
                .collect();
            let new_order_books = self.get_initial_order_books(&mut tokens_to_fetch).await?;
            for (_symbol, token_data_update) in new_order_books {
                update_token_data(&self.token_data_store, token_data_update);
            }
    
            let subscribe_message = self.create_subscribe_message(&to_subscribe);
            write.send(Message::Text(subscribe_message.to_string())).await?;
            info!("Successfully subscribed to: {:?}", to_subscribe);
            for symbol in &to_subscribe {
                self.current_subscriptions.insert(symbol.clone());
            }
        }
    
        if !to_subscribe.is_empty() || !to_unsubscribe.is_empty() {
            let updated_tokens: Vec<String> = self.current_subscriptions.iter().cloned().collect();
            if let Err(e) = self.token_update_sender.send(updated_tokens).await {
                info!("Failed to send token updates to Graph: {:?}", e);
            }
        }
    
        info!("Subscription update complete. Current subscriptions: {:?}", self.current_subscriptions);
        
        self.updating_subscriptions.store(false, Ordering::Release);
        
        Ok(())
    }

    fn create_subscribe_message(&self, symbols: &[String]) -> Value {
        info!("Creating subscribe message for {:?}", symbols);
        let streams: Vec<String> = symbols.iter()
            .map(|s| format!("{}@depth@100ms", s.to_lowercase()))
            .collect();

        json!({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        })
    }

    fn create_unsubscribe_message(&self, symbols: &[String]) -> Value {
        info!("Creating unsubscribe message for {:?}", symbols);
        let streams: Vec<String> = symbols.iter()
            .map(|s| format!("{}@depth@100ms", s.to_lowercase()))
            .collect();

        json!({
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": 1
        })
    }

    async fn process_message(&self, message: &Value, tx: &Sender<(String, OrderBookUpdate)>) -> Result<()> {
        let _start_time = Instant::now();
        //info!("Обрабатываем сообщение: {}", message);

        if let Some(stream_type) = message.get("e") {
            if stream_type == "depthUpdate" {
                if let Some(symbol) = message.get("s").and_then(Value::as_str) {
                    if !self.current_subscriptions.contains(symbol) {
                        return Ok(());
                    }

                    let order_book_update = self.parse_order_book_update(message, symbol)?;

                    // Обновляем ордербук в token_data_store
                    self.token_data_store.update_order_book(symbol, order_book_update.clone());
    
                    // Отправляем обновление через канал
                    tx.send((symbol.to_string(), order_book_update)).await?;

                    #[cfg(debug_assertions)]
                    info!("Обработано обновление для символа: {}", symbol);
                }
            }
        }

        #[cfg(debug_assertions)]
        info!("Время обработки сообщения: {:?}", _start_time.elapsed());
        
        Ok(())
    }
    
    fn parse_order_book_update(&self, data: &Value, symbol: &str) -> Result<OrderBookUpdate> {
        let parse_entries = |entries: &Value| -> Result<Vec<OrderBookEntry>> {
            entries.as_array()
                .ok_or_else(|| anyhow!("Invalid order book data"))?
                .iter()
                .map(|entry| {
                    let price = entry[0].as_str().ok_or_else(|| anyhow!("Invalid price"))?.parse()?;
                    let volume = entry[1].as_str().ok_or_else(|| anyhow!("Invalid volume"))?.parse()?;
                    Ok(OrderBookEntry { price, volume, version: 0 })
                })
                .collect()
        };

        let bids = parse_entries(data.get("b").unwrap_or(&Value::Null))?;
        let asks = parse_entries(data.get("a").unwrap_or(&Value::Null))?;
        
        Ok(OrderBookUpdate {
            symbol: symbol.to_string(),
            bids_updates: bids,
            asks_updates: asks,
        })
    }

    fn parse_order_book(&self, data: &Value, symbol: &str) -> Result<TokenDataUpdate> {
        let parse_entries = |entries: &Value| -> Result<Vec<OrderBookEntry>> {
            entries.as_array()
                .ok_or_else(|| anyhow!("Invalid order book data"))?
                .iter()
                .map(|entry| {
                    let price = entry[0].as_str().ok_or_else(|| anyhow!("Invalid price"))?.parse()?;
                    let volume = entry[1].as_str().ok_or_else(|| anyhow!("Invalid volume"))?.parse()?;
                    Ok(OrderBookEntry { price, volume, version: 0 })
                })
                .collect()
        };

        let bids = parse_entries(data.get("bids").unwrap_or(&Value::Null))?;
        let asks = parse_entries(data.get("asks").unwrap_or(&Value::Null))?;
        
        info!("Parsed order book: {} bids, {} asks", bids.len(), asks.len());
        
        Ok(TokenDataUpdate {
            symbol: symbol.to_string(),
            order_book_update: OrderBookUpdate {
                symbol: symbol.to_string(),
                bids_updates: bids,
                asks_updates: asks,
            },
        })
    }
}
