// Статусы кодов: 50, 52, 53, 54
// 50 не критический 
// 52, 53, 54 критические 
// Реализовать валидную обработку каждого кода 
// И дополнительную логику 400/401 ошибок с проверкой баланса 

// Можно и нужно добавить RateLimits по операциям
// создавая новую структуру данных для лимитов 
// которая будет парсится вместе с handle_message 
// 
// Относительно лимитов корректировать логику действий бота  

use log::{info, error};
use url::Url;
use serde_json::{json, Value};

use dashmap::DashMap;
use hashbrown::HashMap;
use once_cell::sync::Lazy;

use sha2::Sha256;
use hmac::{Hmac, Mac, NewMac};

use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};

use std::sync::Arc;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering, AtomicBool};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream, MaybeTlsStream};

use crate::config::Config;
use crate::data::LockFreeTokenDataStore;
use crate::tokenfilter::{get_step_sizes, StepSizeStore};
use crate::math_graph::{MathGraph, ChainResult, OperationInfo, Operation};


static ORDER_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct WebSocketMessage {
    message: String,
    response_sender: mpsc::Sender<Result<(u64, f64, String), Box<dyn std::error::Error + Send + Sync>>>,
}

#[derive(Clone, Debug)]
struct ChainExecutionInfo {
    chain: Vec<String>,
    profit_percentage: f64,
    count: f64,
    timestamp: Instant,
    is_ready: bool,
    chain_result: Option<ChainResult>,
}

impl ChainExecutionInfo {
    fn new(chain: Vec<String>, profit_percentage: f64) -> Self {
        ChainExecutionInfo {
            chain,
            profit_percentage,
            count: 0.0,
            timestamp: Instant::now(),
            is_ready: false,
            chain_result: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BotErrors {
    pub order_execution_error: Option<String>,
    pub websocket_error: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum BotAction {
    CycleStart,
    ChainSelected { chain: Vec<String>, expected_profit: f64 },
    OperationExecuted { 
        from_token: String, 
        to_token: String, 
        symbol: String,
        operation: String,
        amount: f64,
        previous_balance: f64,
        new_balance: f64
    },
    CycleCompleted { 
        initial_balance: f64, 
        final_balance: f64, 
        profit_percentage: f64,
        intermediate_results: Vec<BotAction>,
    },
}

pub struct ProfitableChainStore {
    chains: DashMap<Vec<String>, ChainExecutionInfo>,
    config: Arc<Config>,
    math_graph: Arc<MathGraph>,
}

impl ProfitableChainStore {
    pub fn new(config: Arc<Config>, math_graph: Arc<MathGraph>) -> Self {
        ProfitableChainStore {
            chains: DashMap::new(),
            config,
            math_graph,
        }
    }

    fn verify_chain_exists(&self, chain: &[String]) -> Option<ChainResult> {
        let chain_key = chain.join("->");
        let result = self.math_graph.profitable_chains
            .get(&chain_key)
            .map(|entry| entry.value().clone());

        // Логирование для проверки корректности функции
        if result.is_some() {
            info!("Цепочка найдена: ключ = {}", chain_key);
        } else {
            info!("Цепочка не найдена: ключ = {}", chain_key);
        }

        result
    }

    fn add_or_update_chain(&self, chain_result: ChainResult, basic_threshold: f64) {
        let chain = chain_result.chain.clone();
        let profit_percentage = chain_result.profit_percentage;
        
        match self.verify_chain_exists(&chain) {
            Some(current_chain_result) => {
                if profit_percentage >= basic_threshold {
                    // Клонируем current_chain_result перед использованием в замыканиях
                    let chain_result_for_modify = current_chain_result.clone();
                    let chain_result_for_insert = current_chain_result.clone();
                    
                    self.chains.entry(chain.clone())
                        .and_modify(|info| {
                            let significant_change = (info.profit_percentage - profit_percentage).abs() > 0.01;
                            
                            info!("Обновление цепочки {:?}: Старая прибыль: {:.4}%, Новая прибыль: {:.4}%", 
                                chain, info.profit_percentage * 100.0, profit_percentage * 100.0);
                            
                            info.profit_percentage = profit_percentage;
                            info.chain_result = Some(chain_result_for_modify);
                            
                            if significant_change {
                                info.is_ready = false;
                                // info.timestamp = Instant::now();
                                info!("Существенное изменение прибыли, сброс готовности цепочки {:?}", chain);
                            }
                        })
                        .or_insert_with(|| {
                            info!("Добавление новой цепочки {:?} с прибылью: {:.4}%", 
                                chain, profit_percentage * 100.0);
                            let mut info = ChainExecutionInfo::new(chain, profit_percentage);
                            info.chain_result = Some(chain_result_for_insert);
                            info
                        });
                } else {
                    if let Some(_removed) = self.chains.remove(&chain) {
                        info!("Удалена цепочка {:?} из-за низкой прибыли: {:.4}%", 
                            chain, profit_percentage * 100.0);
                    }
                }
            },
            None => {
                if let Some(_removed) = self.chains.remove(&chain) {
                    info!("Удалена цепочка {:?}, так как она больше не существует в MathGraph", chain);
                }
            }
        }
    }

    fn get_valid_chains(&self, basic_threshold: f64, profit_threshold: f64) -> Vec<ChainExecutionInfo> {
        self.chains
            .iter()
            .filter(|entry| {
                if let Some(_current_chain) = self.verify_chain_exists(entry.key()) {
                    let info = entry.value();
                    let meets_basic = info.profit_percentage >= basic_threshold;
                    let meets_profit = info.profit_percentage >= profit_threshold;
                    let is_ready = info.is_ready;

                    info!("Проверка цепочки {:?}: meets_basic = {}, meets_profit = {}, is_ready = {}", 
                        entry.key(), meets_basic, meets_profit, is_ready);

                    meets_basic && meets_profit && is_ready
                } else {
                    false
                }
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn update_chain_validity(&self, basic_threshold: f64) {
        let now = Instant::now();
        let mut keys_to_remove = Vec::new();
    
        for mut entry in self.chains.iter_mut() {
            // Проверяем существование в MathGraph
            match self.verify_chain_exists(entry.key()) {
                Some(_current_chain) => {
                    let info = entry.value_mut();
                    let elapsed = now.duration_since(info.timestamp);
                    
                    if info.profit_percentage < basic_threshold {
                        info!("Цепочка {:?} будет удалена из-за низкой прибыльности: {:.4}%", 
                            info.chain, info.profit_percentage * 100.0);
                        keys_to_remove.push(entry.key().clone());
                    } 
                    else if info.profit_percentage >= self.config.profit_threshold + 0.04 {
                        if elapsed >= Duration::from_secs_f64(2.4) && !info.is_ready {
                            info.is_ready = true;
                            info!("Цепочка {:?} теперь готова к использованию после превышения profit_threshold + 4% (прошло {:.2} сек), прибыльность: {:.4}%", 
                                info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                        } else {
                            info!("Цепочка {:?} еще не готова после превышения profit_threshold + 4% (прошло {:.2} сек), прибыльность: {:.4}%", 
                                info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                        }
                    }
                    else if elapsed >= Duration::from_secs_f64(1.4) && !info.is_ready {
                        info.is_ready = true;
                        info!("Цепочка {:?} теперь готова к использованию (прошло {:.2} сек), прибыльность: {:.4}%", 
                            info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                    } 
                    else {
                        info!("Цепочка {:?} еще не готова (прошло {:.2} сек), прибыльность: {:.4}%", 
                            info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                    }
                },
                None => {
                    info!("Цепочка {:?} будет удалена, так как больше не существует в MathGraph", 
                        entry.key());
                    keys_to_remove.push(entry.key().clone());
                }
            }
        }
    
        // Удаляем помеченные цепочки
        for key in keys_to_remove {
            self.chains.remove(&key);
            info!("Удалена цепочка {:?}", key);
        }
    }
}

pub struct Bot {
    config: Arc<Config>,
    current_balance: f64,
    current_token: String,
    time_offset: i64,
    math_graph: Arc<MathGraph>,
    actions: Arc<RwLock<Vec<BotAction>>>,
    current_operation_index: usize,
    intermediate_results: Vec<BotAction>,
    current_chain: Option<ChainResult>,
    last_executed_chains: VecDeque<ChainExecutionInfo>,
    chain_execution_info: HashMap<Vec<String>, ChainExecutionInfo>,
    last_clean_time: Instant,
    write: Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    message_sender: mpsc::Sender<WebSocketMessage>,
    message_receiver: mpsc::Receiver<WebSocketMessage>,
    token_data_store: Arc<LockFreeTokenDataStore>,
    step_size_store: Arc<RwLock<StepSizeStore>>,
    pub errors: Arc<RwLock<BotErrors>>,
    profitable_chain_store: Arc<ProfitableChainStore>,
    bot_action_sender: broadcast::Sender<BotAction>,
}

impl Clone for Bot {
    fn clone(&self) -> Self {
        let (_new_sender, new_receiver) = mpsc::channel(100);
        Bot {
            config: Arc::clone(&self.config),
            current_balance: self.current_balance,
            current_token: self.current_token.clone(),
            time_offset: self.time_offset,
            math_graph: Arc::clone(&self.math_graph),
            actions: Arc::clone(&self.actions),
            current_operation_index: self.current_operation_index,
            intermediate_results: self.intermediate_results.clone(),
            current_chain: self.current_chain.clone(),
            last_executed_chains: self.last_executed_chains.clone(),
            chain_execution_info: self.chain_execution_info.clone(),
            last_clean_time: self.last_clean_time,
            write: None,
            message_sender: self.message_sender.clone(),
            message_receiver: new_receiver,
            token_data_store: Arc::clone(&self.token_data_store),
            step_size_store: Arc::clone(&self.step_size_store),
            errors: Arc::clone(&self.errors),
            profitable_chain_store: Arc::new(ProfitableChainStore::new(
                Arc::clone(&self.config),
                Arc::clone(&self.math_graph)
            )),            
            bot_action_sender: self.bot_action_sender.clone(),
        }
    }
}

impl Bot {
    pub fn new(
        config: Arc<Config>,
        math_graph: Arc<MathGraph>,
        token_data_store: Arc<LockFreeTokenDataStore>,
        bot_action_sender: broadcast::Sender<BotAction>,
    ) -> Self {
        let (message_sender, message_receiver) = mpsc::channel(100);

        // Клонируем Arc перед использованием
        let config_clone = Arc::clone(&config);
        let math_graph_clone = Arc::clone(&math_graph);

        Bot {
            current_balance: config.wallet_balance,
            current_token: config.start_end_token.to_string(),
            config,
            time_offset: 0,
            math_graph,
            actions: Arc::new(RwLock::new(Vec::new())),
            current_operation_index: 0,
            intermediate_results: Vec::new(),
            current_chain: None,
            last_executed_chains: VecDeque::with_capacity(10),
            chain_execution_info: HashMap::new(),
            last_clean_time: Instant::now(),
            write: None,
            message_sender,
            message_receiver,
            token_data_store,
            step_size_store: Arc::new(RwLock::new(StepSizeStore::new())),
            errors: Arc::new(RwLock::new(BotErrors::default())),
            profitable_chain_store: Arc::new(ProfitableChainStore::new(
                config_clone,
                math_graph_clone
            )),
            bot_action_sender,
        }
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.engine().await
    }

    pub async fn engine(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting Brain Bot");
        info!("Ждем 120 секунд в brain_bot.rs");

        tokio::time::sleep(Duration::from_secs(120)).await;

        // Получаем все символы из token_data_store
        let symbols: Vec<String> = self.token_data_store.data.iter().map(|entry| entry.key().clone()).collect();

        // Инициализируем step_size_store
        let step_sizes = get_step_sizes(symbols).await?;
        let store = Arc::clone(&self.step_size_store);
        tokio::spawn(async move {
            let mut guard = store.write().await;
            *guard = step_sizes;
            info!("Step_size_store успешно обновлен");
        });

        tokio::time::sleep(Duration::from_secs(5)).await;

        let server_time = self.get_server_time().await?;
        let local_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        self.time_offset = server_time - local_time;

        let url = Url::parse("wss://ws-api.binance.com:443/ws-api/v3")?;

        loop {
            match self.connect_and_handle(&url).await {
                Ok(_) => info!("WebSocket connection closed normally. Reconnecting..."),
                Err(e) => {
                    info!("WebSocket error: {:?}. Closed connection", e);
                    if e.to_string().contains("Error 52") || e.to_string().contains("Error 53") || e.to_string().contains("Error 54") {

                        return Err(e);
                    } else {

                    }
                }
            }
            info!("Вызвано ожидание 1 секунд в цикле run");
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn get_server_time(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let resp: Value = client.get("https://api.binance.com/api/v3/time")
            .send()
            .await?
            .json()
            .await?;
        
        Ok(resp["serverTime"].as_i64().unwrap())
    }

    async fn send_message(&mut self, message: String) -> Result<(u64, f64, String), Box<dyn std::error::Error + Send + Sync>> {
        let (response_sender, mut response_receiver) = mpsc::channel(100);
        
        let websocket_message = WebSocketMessage {
            message: message.clone(),
            response_sender,
        };
        
        self.message_sender.send(websocket_message).await?;
        info!("Отправленное сообщение из send_message {:.?}", message);

        info!("Waiting for response with 3 seconds timeout...");
        tokio::time::timeout(Duration::from_secs(3), response_receiver.recv()).await?
        .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "Error 54: Timed out waiting for response")) as Box<dyn std::error::Error + Send + Sync>)?
    }

    fn parse_symbol(&self, symbol: &str, is_buy: bool) -> String {
        info!("parse_symbol вызвана с параметрами: symbol = {}, is_buy = {}", symbol, is_buy);
    
        // Проверяем, состоит ли пара из двух валют из quote_currencies
        let is_double_quote = self.config.quote_currencies.iter().filter(|&currency| 
            symbol.starts_with(currency) || symbol.ends_with(currency)
        ).count() == 2;
    
        info!("Пара состоит из двух quote_currencies: {}", is_double_quote);
    
        if is_double_quote {
            // Если пара состоит из двух quote_currencies
            for &quote_currency in &self.config.quote_currencies {
                if symbol.starts_with(quote_currency) {
                    let base_currency = &symbol[quote_currency.len()..];
                    let result = if is_buy { quote_currency.to_string() } else { base_currency.to_string() };
                    info!("Обработка двойной quote пары. Результат: {}", result);
                    return result;
                }
                if symbol.ends_with(quote_currency) {
                    let base_currency = &symbol[..symbol.len() - quote_currency.len()];
                    let result = if is_buy { base_currency.to_string() } else { quote_currency.to_string() };
                    info!("Обработка двойной quote пары. Результат: {}", result);
                    return result;
                }
            }
        } else {
            // Стандартная логика для обычных пар
            for &quote_currency in &self.config.quote_currencies {
                if symbol.ends_with(quote_currency) {
                    let base_currency = &symbol[..symbol.len() - quote_currency.len()];
                    let result = if is_buy { base_currency.to_string() } else { quote_currency.to_string() };
                    info!("Обработка стандартной пары. Результат: {}", result);
                    return result;
                }
                if symbol.starts_with(quote_currency) {
                    let base_currency = &symbol[quote_currency.len()..];
                    let result = if is_buy { base_currency.to_string() } else { quote_currency.to_string() };
                    info!("Обработка стандартной пары. Результат: {}", result);
                    return result;
                }
            }
        }
        
        info!("Не удалось разобрать символ. Возвращаем исходный символ: {}", symbol);
        symbol.to_string()
    }

    fn check_clean_struct(&mut self, force_clean: bool) {
        static IS_FIRST_CLEAN_CHECK: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));
        
        let now = Instant::now();
        
        if IS_FIRST_CLEAN_CHECK.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            info!("First call to check_clean_struct. Initializing timer.");
            self.last_clean_time = now;
            return;
        }
        
        let elapsed = now.duration_since(self.last_clean_time);
        
        info!("check_clean_struct called. Force clean: {}", force_clean);
        info!("Time since last clean: {:?}", elapsed);

        if force_clean && elapsed >= Duration::from_millis(1400) {
            info!("Cleaning chain_execution_info structure");
            self.chain_execution_info.clear();
            self.last_clean_time = now;
            info!("Structure cleaned. New last_clean_time set");
        } else if force_clean {
            info!("Force clean requested, but time haven't passed yet");
        }
    }

    // Логика форматирования чисел по step_size 
    // Логика предназначена для документации binance 
    // Другие биржи могут отличаться и не требовать это
    fn format_float_truncate(value: f64, decimal_places: usize) -> f64 {
        if decimal_places == 0 {
            value.trunc()
        } else {
            let multiplier = 10f64.powi(decimal_places as i32);
            (value * multiplier).trunc() / multiplier
        }
    }

    async fn connect_and_handle(&mut self, url: &Url) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to WebSocket...");
        let (ws_stream, _) = connect_async(url).await?;
        info!("WebSocket connected successfully.");
        let (write, mut read) = ws_stream.split();
        self.write = Some(write);
    
        let mut ping_interval = interval(Duration::from_secs(30));
        let duration = Duration::from_secs(81420); // 22 часа 37 минут
    
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let timer_flag = Arc::new(AtomicBool::new(false));
    
        // Клонируем переменные для использования в задачах
        let shutdown_flag_clone1 = Arc::clone(&shutdown_flag);
        let shutdown_flag_clone2 = Arc::clone(&shutdown_flag);
        let timer_flag_clone1 = Arc::clone(&timer_flag);
        let timer_flag_clone2 = Arc::clone(&timer_flag);

        // Запускаем задачу для обработки Ctrl+C
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
            info!("Received Ctrl+C signal in Brain Bot. Preparing to stop bot.");
            shutdown_flag_clone1.store(true, Ordering::SeqCst);
        });
    
        // Запускаем задачу для отслеживания времени работы бота
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            info!("Bot runtime exceeded. Preparing to stop bot.");
            timer_flag_clone1.store(true, Ordering::SeqCst);
        });

        // Клонируем self для использования в trade_handle
        let mut self_clone = self.clone();
    
        // Запускаем задачу для выполнения торговых циклов
        let mut trade_handle = tokio::spawn(async move {
            loop {
                let cycle_start = Instant::now();

                if shutdown_flag_clone2.load(Ordering::SeqCst) {
                    info!("Shutdown signal received. Shutting down...");
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 54: Shutdown signal received")));
                }

                if timer_flag_clone2.load(Ordering::SeqCst) {
                    info!("Bot runtime exceeded. Shutting down...");
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 55: Bot runtime exceeded or shutdown signal received")));
                }
        
                match self_clone.execute_trade_cycle().await {
                    Ok((action, is_completed)) => {
                        match action {
                            BotAction::OperationExecuted { .. } => {
                                // Обработка уже происходит внутри execute_trade_cycle
                            },
                            BotAction::CycleCompleted { .. } => {
                                info!("Trade cycle completed");
                                if is_completed {
                                    info!("All operations in the chain completed");
                                    info!("Основной цикл операций бота завершен за {:?}", cycle_start.elapsed());
                                    // Начинаем новый цикл
                                    //info!("Начинаем новый цикл");
                                    //tokio::time::sleep(Duration::from_secs(10)).await;
                                    //continue;
                                }
                            },
                            _ => {}
                        }
                    },
                    Err(e) => {
                        if e.to_string() == "Error Timestamp" {
                            let server_time = match self_clone.get_server_time().await {
                                Ok(time) => time,
                                Err(err) => {
                                    info!("Не удалось получить время сервера: {:?}", err);
                                    continue;
                                }
                            };
                            let local_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as i64;
                            self_clone.time_offset = server_time - local_time;
                            info!("Обновили смещение времени из-за ошибки таймстампа. Ошибка: {:?}", e);
                            continue;
                        } else if e.to_string() == "Restart cycle" {
                            info!("Перезапуск цикла из-за убыточной операции с начальным токеном USDT");
                            continue;
                        } else if e.to_string().contains("No profitable chains found") {
                            info!("Error: No profitable chains found: {:?}", e);
                            continue;
                        } else {
                            info!("Error in trade cycle: {:?}", e);
                            if e.to_string().contains("Error 52")
                                || e.to_string().contains("Error 53")
                                || e.to_string().contains("Error 54")
                            {
                                info!("Critical error occurred. Stopping trade cycles.");
                                return Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    e.to_string(),
                                )));
                            }
                        }
                    }                    
                }
                // В этой части происходит конец цикла
                // Для релиза убрать задержку
                //info!("Для loop ожидаем время следующего цикла");
                //tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    
        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Some(write) = &mut self.write {
                        write.send(Message::Ping(vec![])).await?;
                        info!("Ping sent");
                    }
                }
                Some(message) = read.next() => {
                    match message {
                        Ok(Message::Text(text)) => {
                            // info!("Received message: {}", text);
                            // Обрабатываем только общие сообщения, не связанные с конкретными ордерами
                            // Здесь нужна отличная от "handle_text_message" функция
                            if !text.contains("\"method\":\"order.place\"") {
                                self.handle_text_message(&text).await?;
                            }
                        }
                        Ok(Message::Pong(_)) => {
                            info!("Pong received");
                        }
                        Ok(Message::Close(frame)) => {
                            info!("WebSocket closed: {:?}", frame);
                            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: WebSocket connection closed")));
                        }
                        Err(e) => {
                            error!("WebSocket error: {:?}", e);
                            return Err(Box::new(e));
                        }
                        _ => {}
                    }
                }
                Some(websocket_message) = self.message_receiver.recv() => {
                    let cycle_start = Instant::now();
                    if let Some(write) = &mut self.write {
                        match write.send(Message::Text(websocket_message.message.clone())).await {
                            Ok(_) => {
                                info!("Order message sent successfully");
                                let mut response_handled = false;
                                while !response_handled {
                                    // Ожидаем ответ от сервера
                                    if let Some(response) = read.next().await {
                                        match response {
                                            Ok(Message::Text(text)) => {
                                                info!("Received message: {}", text);
                                                match self.handle_text_message(&text).await {
                                                    Ok((id, new_balance, received_token)) => {
                                                        info!("Order executed: ID: {}, New balance: {} {}", id, new_balance, received_token);
                                                        let _ = websocket_message.response_sender.send(Ok((id, new_balance, received_token))).await;
                                                        response_handled = true;
                                                        info!("Вызов и обработка websocket сигнала: {:?}", cycle_start.elapsed());
                                                    },
                                                    Err(e) => {
                                                        info!("Error 52: Error processing order: {:?}", e);
                                                        
                                                        let mut errors = self.errors.write().await;
                                                        errors.order_execution_error = Some(format!("Error processing order: {:?}", e));

                                                        let _ = websocket_message.response_sender.send(Err(e)).await;
                                                        response_handled = true;
                                                    }
                                                }
                                            },
                                            Ok(Message::Pong(_)) => {
                                                info!("Pong received");
                                            },
                                            Ok(Message::Close(frame)) => {
                                                info!("WebSocket closed: {:?}", frame);
                                                let _ = websocket_message.response_sender.send(Err(Box::new(
                                                    std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Error 52: WebSocket connection closed")
                                                ))).await;
                                                response_handled = true;
                                            },
                                            Err(e) => {
                                                info!("Error receiving message: {:?}", e);
                                                let _ = websocket_message.response_sender.send(Err(Box::new(e))).await;
                                                response_handled = true;
                                            },
                                            _ => info!("Error 50: Unexpected response type: {:?}", response),
                                        }
                                    } else {
                                        info!("No response received from server");
                                        let _ = websocket_message.response_sender.send(Err(Box::new(
                                            std::io::Error::new(std::io::ErrorKind::Other, "Error 54: No response received from server")
                                        ))).await;
                                        response_handled = true;
                                    }
                                }
                            },
                            Err(e) => {
                                info!("Error 54: Failed to send order message: {:?}", e);
                                let mut errors = self.errors.write().await;
                                errors.websocket_error = Some(format!("Failed to send order message: {:?}", e));
                            },
                        }
                    } else {
                        info!("Error 54: WebSocket connection not established");
                    }
                }
                result = &mut trade_handle => {
                    match result {
                        Ok(Ok(())) => {
                            info!("Trade handle completed successfully");
                            return Ok(());
                        }
                        Ok(Err(e)) => {
                            info!("Trade handle completed with error: {:?}", e);
                            return Err(e);
                        }
                        Err(e) => {
                            info!("Trade handle panicked: {:?}", e);
                            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Error 54: Trade handle panicked: {:?}", e))));
                        }
                    }
                }
            }
        }
    }

    async fn handle_text_message(&mut self, text: &str) -> Result<(u64, f64, String), Box<dyn std::error::Error + Send + Sync>> {
        let websocket_message = Instant::now();
        let response: Value = serde_json::from_str(text)?;
        if let Some(id) = response["id"].as_u64() {
            if let Some(status) = response["status"].as_u64() {
                match status {
                    200 => {
                        if let Some(result) = response.get("result") {
                            let executed_qty: f64 = result["executedQty"].as_str().unwrap().parse()?;
                            let cummulative_quote_qty: f64 = result["cummulativeQuoteQty"].as_str().unwrap().parse()?;
                            
                            if executed_qty == 0.0 || cummulative_quote_qty == 0.0 {
                                info!("Warning: Order executed with zero quantity or quote quantity. Possible liquidity issues.");
                            }
                            
                            let side = result["side"].as_str().unwrap_or("UNKNOWN");
                            let symbol = result["symbol"].as_str().unwrap_or("UNKNOWN");

                            let mut total_commission_in_received_asset: f64 = 0.0;
                            let mut commission_count = 0;
                            let mut multiple_commission_assets = false;
                            let mut unique_commission_assets = std::collections::HashSet::new();
    
                            let (mut new_balance, received_token) = if side == "BUY" {
                                (executed_qty, self.parse_symbol(symbol, true))
                            } else {
                                (cummulative_quote_qty, self.parse_symbol(symbol, false))
                            };
                            
                            info!("Initial balance: {}", new_balance);

                            if let Some(fills) = result.get("fills").and_then(|f| f.as_array()) {
                                for fill in fills {
                                    if let (Some(commission), Some(asset)) = (fill["commission"].as_str(), fill["commissionAsset"].as_str()) {
                                        if let Ok(comm) = commission.parse::<f64>() {
                                            if asset == received_token {
                                                total_commission_in_received_asset += comm;
                                            }
                                            if !unique_commission_assets.insert(asset.to_string()) {
                                                multiple_commission_assets = true;
                                            }
                                            commission_count += 1;
                                        }
                                    }
                                }
                            }
    
                            info!("Commission info: {} commission{} processed.", commission_count, if commission_count == 1 { "" } else { "s" });
                            info!("Total commission in received asset ({}): {}", received_token, total_commission_in_received_asset);
                            
                            if multiple_commission_assets {
                                info!("Warning: Multiple commission assets detected. Only commissions in {} are subtracted.", received_token);
                                // Можно добавить дополнительную обработку или логирование здесь
                            }
                            
                            if total_commission_in_received_asset > 0.0 {
                                new_balance -= total_commission_in_received_asset;
                                info!("Total commission paid: {} {}", total_commission_in_received_asset, received_token);
                                info!("Balance after commission: {}", new_balance);
                            }
    
                            info!("Order executed successfully. New balance: {} {}", new_balance, received_token);
                            info!("Вызов и обработка websocket сигнала внутри handle_text_message: {:?}", websocket_message.elapsed());
                            return Ok((id, new_balance, received_token));
                        }
                    },
                    400 | 401 => {
                        let error_msg = response["error"]["msg"].as_str().unwrap_or("Unknown error");
                        info!("Error {}: Bad Request. Order not placed. Details: {}", status, error_msg);
                        if error_msg.starts_with("Timestamp for this request") {
                            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error Timestamp")));
                        } else {
                            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Error 52: Order execution failed. Status: {}", status))));
                        }
                    },
                    429 => {
                        info!("Error 429: Too Many Requests. Please slow down.");
                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 50: Too many requests")));
                    },
                    500 => {
                        info!("Error 500: Internal Server Error. Order status unknown.");
                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 50: Internal server error")));
                    },
                    _ => {
                        info!("Unexpected status code: {}. Response: {}", status, text);
                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Error 50: Unexpected order execution error. Status: {}", status))));
                    },
                }
            }
        }
        Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Invalid response format")))
    }

    async fn execute_trade_cycle(&mut self) -> Result<(BotAction, bool), Box<dyn std::error::Error + Send + Sync>> {
        let cycle_start = Instant::now();
    
        //info!("Starting new trade cycle");

        let mut check_and_switch_counter = 0;
    
        // Обновляем валидность цепочек перед выбором
        //self.profitable_chain_store.update_chain_validity(self.config.basic_threshold);

        match self.get_most_profitable_chain().await {
            Ok(chain) => {
                info!("Successfully retrieved a profitable chain");
                
                let chain_key = chain.chain.clone();
                if let Some(mut chain_info) = self.profitable_chain_store.chains.get_mut(&chain_key) {
                    if chain_info.count < 2.0 {
                        // Существующий код для случая count < 2.0
                        chain_info.count += 1.0;
                        self.current_chain = Some(chain.clone());
                        
                        info!("Selected chain: {:?}", chain_key);
                        info!("Expected profit: {:.2}%", chain.profit_percentage * 100.0);
                        info!("Chain execution count: {}", chain_info.count);
                        
                        info!("Adding CycleStart action");
                        self.add_action(BotAction::CycleStart).await;
                        info!("Adding ChainSelected action");
                        self.add_action(BotAction::ChainSelected { 
                            chain: chain_key.clone(), 
                            expected_profit: chain.profit_percentage 
                        }).await;
                    } else {                        
                        info!("Цепочка уже выполнена дважды, поиск новой цепочки");
                        chain_info.count = 0.0;                        
                        
                        // Освобождаем мьютекс перед сканированием всех цепочек
                        drop(chain_info);
                        
                        // Создаем копию всех цепочек для безопасной работы
                        let chains_snapshot: Vec<(Vec<String>, ChainExecutionInfo)> = self.profitable_chain_store.chains
                            .iter()
                            .map(|entry| (entry.key().clone(), entry.value().clone()))
                            .collect();
                
                        // Фильтруем цепочки без блокировки DashMap
                        let filtered_chains: Vec<_> = chains_snapshot.into_iter()
                            .filter(|(key, info)| {
                                let meets_criteria = key != &chain_key && 
                                                   info.is_ready && 
                                                   info.count < 2.0 &&
                                                   info.profit_percentage >= self.config.profit_threshold;
                                
                                info!("Проверка цепочки для фильтрации {:?}: ready={}, count={}, profit={:.4}%, meets_criteria={}", 
                                    key, info.is_ready, info.count, info.profit_percentage * 100.0, meets_criteria);
                                
                                meets_criteria
                            })
                            .collect();
                
                        info!("Найдено {} подходящих цепочек", filtered_chains.len());
                
                        if filtered_chains.is_empty() {
                            info!("Нет доступных альтернативных цепочек");
                            return Err("No profitable chains found".into());
                        }
                
                        // Находим самую прибыльную цепочку
                        let best_chain = filtered_chains.into_iter()
                            .max_by(|(_, a), (_, b)| {
                                a.profit_percentage.partial_cmp(&b.profit_percentage).unwrap()
                            })
                            .unwrap();
                
                        info!(
                            "Выбрана новая цепочка: {:?} с прибыльностью {:.4}%", 
                            best_chain.0, 
                            best_chain.1.profit_percentage * 100.0
                        );
                
                        // Теперь безопасно обновляем выбранную цепочку
                        if let Some(mut new_chain_entry) = self.profitable_chain_store.chains.get_mut(&best_chain.0) {
                            new_chain_entry.count += 1.0;
                            if let Some(chain_result) = &new_chain_entry.chain_result {
                                self.current_chain = Some(chain_result.clone());
                                
                                self.add_action(BotAction::ChainSelected { 
                                    chain: best_chain.0.clone(), 
                                    expected_profit: best_chain.1.profit_percentage 
                                }).await;
                
                                return Ok((BotAction::CycleStart, false));
                            }
                        }
                
                        info!("Не удалось получить доступ к выбранной цепочке");
                        return Err("No profitable chains found".into());
                    }
                } else {
                    info!("Chain not found in profitable_chain_store");
                    return Err("No profitable chains found".into());
                }
            },
            Err(e) if e.to_string() == "Restart cycle" => {
                info!("Перезапуск цикла из-за убыточной операции с начальным токеном USDT");
                return Ok((BotAction::CycleStart, false));
            },
            Err(e) => {
                info!("Error: No profitable chains found: {:?}", e);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No profitable chains found")));
            }
        }

        let mut chain_result = self.current_chain.as_ref().unwrap().clone();
        let mut index = 0;
    
        while index < chain_result.operations.len() {
            let operation_info = &chain_result.operations[index];
            let operation_start = Instant::now();
            self.current_operation_index = index;
            
            // Пересчитываем прибыльность цепочки с текущего токена
            let updated_chain = self.calculate_chain(&self.current_token.clone(), self.current_balance, &chain_result.chain).await?;
            info!("Updated chain profitability: {:.4}%", updated_chain.profit_percentage);
    
            // Проверяем прибыльность с учетом 15% погрешности
            if updated_chain.profit_percentage * 100.0 < self.config.profit_threshold * 85.0 {                
                info!("Chain profitability dropped below threshold. Current: {:.4}%, Threshold: {:.4}%", 
                    updated_chain.profit_percentage, self.config.profit_threshold);
                
                    if check_and_switch_counter < 2 {
                        match self.check_and_switch_chain(self.current_token.clone(), self.current_balance, updated_chain.profit_percentage).await {
                            Ok(new_chain) => {
                                // Проверяем, отличается ли новая цепочка от текущей
                                if new_chain.chain != chain_result.chain {
                                    if new_chain.profit_percentage * 100.0 > updated_chain.profit_percentage * 110.0 {
                                        info!("Switching to a more profitable chain. New profit: {:.4}%, previous profit: {:.4}%", new_chain.profit_percentage * 100.0, updated_chain.profit_percentage);
        
                                        // Обновляем текущую цепочку
                                        self.current_chain = Some(new_chain.clone());
        
                                        // Обновляем chain_result
                                        chain_result = new_chain.clone();
        
                                        // Добавляем соответствующие действия
                                        info!("Adding ChainSelected action for new chain");
                                        self.add_action(BotAction::ChainSelected { 
                                            chain: new_chain.chain.clone(), 
                                            expected_profit: new_chain.profit_percentage 
                                        }).await;
        
                                        // Сбрасываем индекс, чтобы начать с новой цепочки
                                        index = 0;
        
                                        // Продолжаем выполнение с новой цепочкой
                                        continue;
                                    } else {
                                        info!("Found a new chain, but its profitability ({:.4}%) is not better than current ({:.4}%). Not switching.", new_chain.profit_percentage, updated_chain.profit_percentage);
                                    }
                                } else {
                                    info!("New chain is identical to the current chain. Continuing with the current chain.");
                                }
                            },
                            Err(e) => {
                                info!("Error while checking for alternative chains: {:?}. Continuing with the current chain.", e);
                            }
                        }
                        check_and_switch_counter += 1;
                    } else {
                        info!("Limit of check_and_switch_chain calls reached. Continuing with the current chain.");
                    }
                }
    
            info!("Executing trade cycle operation {}", index + 1);
            info!("  From: {} -> To: {}", operation_info.from_token, operation_info.to_token);
            info!("  Symbol: {}", operation_info.symbol);
            info!("  Operation: {:?}", operation_info.operation);
            
            let (symbol, side, amount, is_quote_order) = self.prepare_order_params(operation_info, &self.current_balance.to_string(), self.current_balance);
    
            info!("Prepared order: Symbol: {}, Side: {}, Amount: {}, Is Quote Order: {}", symbol, side, amount, is_quote_order);
            
            let previous_balance = self.current_balance;
            let previous_token = self.current_token.clone();
    
            info!("  Previous balance: {} {}", previous_balance, operation_info.from_token);
            info!("  Previous token: {} {}", previous_token, operation_info.from_token);
    
            let order_start = Instant::now();
    
            let (new_balance, received_token) = self.place_market_order(&symbol, &side, amount, is_quote_order).await?;
    
            self.current_balance = new_balance;
            self.current_token = received_token.clone();
    
            info!("Одна операция ордера {:?}", order_start.elapsed());
    
            info!("Operation completed. New balance: {} {}", self.current_balance, received_token);
            
            let action = BotAction::OperationExecuted {
                from_token: operation_info.from_token.clone(),
                to_token: operation_info.to_token.clone(),
                symbol: symbol.clone(),
                operation: format!("{:?}", operation_info.operation),
                amount,
                previous_balance,
                new_balance: self.current_balance,
            };
            
            self.intermediate_results.push(action.clone());
            self.add_action(action).await;
    
            info!("Одна операция сделки {:?}", operation_start.elapsed());
    
            if received_token == self.config.start_end_token && index > 0 {
                info!("Цикл внутри execute_trade_cycle завершен за {:?}", cycle_start.elapsed());
                break;
            }
    
            index += 1;
        }
    
        let is_completed = self.current_operation_index >= chain_result.operations.len() - 1 || 
            self.current_balance.to_string() == self.config.start_end_token;
    
        if is_completed {
            info!("Chain execution completed");
            self.current_operation_index = 0;
            let final_balance = self.current_balance;
            let initial_balance = self.config.wallet_balance;
            let profit_percentage = (final_balance / initial_balance - 1.0) * 100.0;
            
            let completed_action = BotAction::CycleCompleted {
                initial_balance,
                final_balance,
                profit_percentage,
                intermediate_results: self.intermediate_results.clone(),
            };

            self.add_action(completed_action.clone()).await;

            info!("Trade cycle completed.");
            info!("Initial balance: {} {}", initial_balance, self.config.start_end_token);
            info!("Final balance: {} {}", final_balance, self.config.start_end_token);
            info!("Profit: {:.2}%", profit_percentage);
            info!("Executed operations:");
            for (i, op) in chain_result.operations.iter().enumerate() {
                info!("  {}. {} -> {}: {}", i+1, op.from_token, op.to_token, op.symbol);
            }

            self.intermediate_results.clear();
            self.current_chain = None;
            self.check_clean_struct(true);

            self.current_balance = self.config.wallet_balance;
            self.current_token = self.config.start_end_token.to_string();

            return Ok((completed_action, true));
        }
        
        Ok((BotAction::CycleStart, false))
    }

    async fn calculate_chain(&mut self, start_token: &str, start_balance: f64, full_chain: &[String]) -> Result<ChainResult, Box<dyn std::error::Error + Send + Sync>> {
        let cycle_start = Instant::now();
    
        info!("Начало расчета прибыльности цепочки");
        info!("Начальный токен: {}", start_token);
        info!("Начальный баланс: {}", start_balance);
        info!("Полная цепочка: {:?}", full_chain);
    
        // Находим индекс start_token в полной цепочке
        let start_index = full_chain.iter().position(|token| token == start_token)
            .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Начальный токен не найден в цепочке")))?;
    
        info!("Индекс начального токена в цепочке: {}", start_index);

        // Проверяем, не является ли текущий токен предпоследним в цепочке,
        // а следующий за ним - конечным токеном USDT
        if start_index == full_chain.len() - 2 && full_chain.last() == Some(&"USDT".to_string()) {
            info!("Обнаружена конечная токен пара - пропускаем операцию");
            return Ok(ChainResult {
                chain: full_chain.to_vec(),
                profit_percentage: self.config.profit_threshold,
                final_amount: start_balance,
                duration: cycle_start.elapsed(),
                operation_time: Duration::new(0, 0),
                operations: vec![]
            });
        }
    
        // Обрезаем цепочку, начиная с найденного индекса
        let remaining_chain = &full_chain[start_index..];
    
        info!("Расчет прибыльности цепочки от {} с балансом {}", start_token, start_balance);
        info!("Оставшаяся цепочка: {:?}", remaining_chain);
    
        let calc_start = Instant::now();
        let mut result = self.math_graph.calculate_chain_profit(remaining_chain.to_vec(), start_balance).await?;
        info!("Время расчета прибыльности: {:?}", calc_start.elapsed());
    
        info!("Результат расчета прибыльности:");
        info!("  Конечная сумма: {}", result.final_amount);
        info!("  Длительность: {:?}", result.duration);
        info!("  Время операции: {:?}", result.operation_time);

        // Добавляем логирование данных о цепочке
        #[cfg(debug_assertions)]
        for (i, operation) in result.operations.iter().enumerate() {
            info!("Операция {}: {} -> {}", i + 1, operation.from_token, operation.to_token);
            info!("  Символ: {}", operation.symbol);
            info!("  Операция: {:?}", operation.operation);
            for (j, entry) in operation.entries.iter().enumerate().take(5) {
                info!("    Уровень {}: Цена = {:.8}, Объем = {:.8}", j + 1, entry.price, entry.volume);
            }
        }
    
        // Проверка на недостаточную ликвидность и повторный расчет
        if result.final_amount == 0.0 {
            info!("Обнаружена нулевая конечная сумма. Выполняем повторный расчет.");
            result = self.math_graph.calculate_chain_profit(remaining_chain.to_vec(), start_balance).await?;
            
            tokio::time::sleep(Duration::from_millis(350)).await;

            if result.final_amount == 0.0 {
                info!("ВНИМАНИЕ: Недостаточно ликвидности для выполнения цепочки");
                result.profit_percentage = -100.0;
            }
        }
    
        // Корректируем результат с учетом начального баланса конфигурации
        let adjusted_profit_percentage = result.final_amount / self.config.wallet_balance - 1.0;

        // Добавляем проверку для начального токена USDT и убыточной прибыльности
        if start_index == 0 && start_token == "USDT" {
            if adjusted_profit_percentage * 100.0 < self.config.profit_threshold * 100.0 {
                info!("Обнаружен начальный токен USDT с убыточной прибыльностью");
                // Помечаем цепочку как выполненную дважды
                let chain_key = full_chain.to_vec();
                let chain_info = self.chain_execution_info
                    .entry(chain_key.clone())
                    .or_insert(ChainExecutionInfo {
                        chain: chain_key.clone(),
                        profit_percentage: adjusted_profit_percentage * 100.0,
                        count: 0.0,
                        timestamp: Instant::now(),
                        chain_result: None,
                        is_ready: false,
                    });
                chain_info.count = 2.0;

                // Добавляем цепочку в count 2.0 чтобы сразу запустить таймер
                // и регулировать случай изменения этой цепочки через время
                self.check_clean_struct(false);

                return Err("Restart cycle".into());
            }
        }
    
        info!("Корректировка результата:");
        info!("  Начальный баланс конфигурации: {}", self.config.wallet_balance);
        info!("  Скорректированный процент прибыли: {:.4}%", adjusted_profit_percentage * 100.0);
    
        let adjusted_result = ChainResult {
            chain: remaining_chain.to_vec(),
            profit_percentage: adjusted_profit_percentage * 100.0,
            final_amount: result.final_amount,
            duration: result.duration,
            operation_time: result.operation_time,
            operations: result.operations,
        };
        
        info!("Вычисление calculate_chain завершено за: {:?}", cycle_start.elapsed());
    
        Ok(adjusted_result)
    }

    async fn check_and_switch_chain(&self, current_token: String, current_balance: f64, updated_profit_percentage: f64) -> Result<ChainResult, Box<dyn std::error::Error + Send + Sync>> {
        let cycle_start = Instant::now();

        info!("Starting check_and_switch_chain for token: {}", current_token);
        
        let alternative_chains = self.math_graph.find_chains_with_token(&current_token).await;
        
        info!("Found {} alternative chains", alternative_chains.len());
    
        let chunk_size = 1000;
        let mut all_chain_results: Vec<ChainResult> = Vec::new();

        info!("Initial data:");
        info!("Current chain profitability: {:.4}%", updated_profit_percentage);
    
        for (_chunk_index, chunk) in alternative_chains.chunks(chunk_size).enumerate() {
            info!("Количество цепочек в чанке: {}", chunk.len());
        
            let results = futures::stream::iter(chunk.to_vec())
                .map(|chain| {
                    let math_graph = self.math_graph.clone();
                    let start_token_balance = current_balance; // self.current_balance;
                    async move {
                        let result = math_graph.calculate_chain_profit(chain.clone(), start_token_balance).await;
                        result
                    }
                })
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await;
        
            for result in results {
                match result {
                    Ok(chain_result) => {
                        // Расчет скорректированного profit_percentage
                        let adjusted_profit_percentage = chain_result.final_amount / self.config.wallet_balance - 1.0;
        
                        // Создание нового ChainResult с скорректированным profit_percentage
                        let adjusted_chain_result = ChainResult {
                            profit_percentage: adjusted_profit_percentage,
                            ..chain_result.clone()
                        };
        
                        // Добавление скорректированного результата в all_chain_results
                        all_chain_results.push(adjusted_chain_result.clone());
                    },
                    Err(e) => {
                        info!("Ошибка при расчете прибыли для цепочки: {:?}", e);
                    }
                }
            }
        }

        info!("Вычисление check_and_switch_chain завершено за: {:?}", cycle_start.elapsed());

        let mut profit_percentages: Vec<f64> = all_chain_results.iter().map(|chain| chain.profit_percentage).collect();
        profit_percentages.sort_by(|a, b| b.partial_cmp(a).unwrap());

        // Удаляем цепочки с некорректной прибыльностью
        all_chain_results.retain(|chain| chain.profit_percentage.is_finite());
        
        if all_chain_results.is_empty() {
            info!("No valid chain results after filtering. Exiting check_and_switch_chain.");
            return Err("No alternative chains found".into());
        }

        // Сортируем цепочки по прибыльности (по убыванию)
        let mut chains_by_profit = all_chain_results.clone();
        chains_by_profit.sort_by(|a, b| b.profit_percentage.partial_cmp(&a.profit_percentage).unwrap());

        // Сортируем цепочки по длине (по возрастанию), затем по прибыльности (по убыванию)
        let mut chains_by_length = all_chain_results.clone();
        chains_by_length.sort_by(|a, b| {
            let len_cmp = a.operations.len().cmp(&b.operations.len());
            if len_cmp == std::cmp::Ordering::Equal {
                b.profit_percentage.partial_cmp(&a.profit_percentage).unwrap()
            } else {
                len_cmp
            }
        });

        let top_profitable_chain = chains_by_profit[0].clone();
        let top_shortest_chain = chains_by_length[0].clone();

        info!("Top profitable chain selected: {:?}", top_profitable_chain.chain);
        info!("Top shortest chain selected: {:?}", top_shortest_chain.chain);

        // Вычисляем, все ли цепочки убыточные
        let all_chains_unprofitable = all_chain_results.iter().all(|chain| chain.profit_percentage < 0.0);

        let selected_chain = if all_chains_unprofitable {
            info!("Все цепочки убыточные. Выполняем проверку относительно порогов -0.1% и -1.0%.");

            if top_shortest_chain.profit_percentage >= -0.1 {
                info!(
                    "Самая короткая цепочка прибыльна или близка к нулю ({}%). Выбираем самую короткую цепочку.",
                    top_shortest_chain.profit_percentage
                );
                top_shortest_chain.clone()
            } else if top_shortest_chain.profit_percentage > -1.0 {
                info!(
                    "Самая короткая цепочка убыточна, но превышает -1.0% ({}%). Выбираем самую прибыльную цепочку.",
                    top_shortest_chain.profit_percentage
                );
                top_profitable_chain.clone()
            } else {
                info!(
                    "Самая короткая цепочка менее -1.0% ({}%). Выбираем самую короткую цепочку.",
                    top_shortest_chain.profit_percentage
                );
                top_shortest_chain.clone()
            }
        } else {
            // Определяем количество прибыльных цепочек
            let profitable_chains_count = all_chain_results.iter().filter(|chain| chain.profit_percentage > 0.0).count();

            if profitable_chains_count == 1 {
                // Выбираем единственную прибыльную цепочку
                let profitable_chain = all_chain_results.iter().find(|chain| chain.profit_percentage > 0.0).unwrap().clone();
                info!(
                    "Найдена одна прибыльная цепочка: {:?}. Выбираем её.",
                    profitable_chain.chain
                );
                profitable_chain
            } else {
                // Есть две или более прибыльных цепочек
                let shortest_profitable_chain = all_chain_results.iter()
                    .filter(|chain| chain.profit_percentage > 0.0)
                    .min_by_key(|chain| chain.operations.len())
                    .unwrap()
                    .clone();

                let most_profitable_chain = all_chain_results.iter()
                    .filter(|chain| chain.profit_percentage > 0.0)
                    .max_by(|a, b| a.profit_percentage.partial_cmp(&b.profit_percentage).unwrap())
                    .unwrap()
                    .clone();

                info!(
                    "Найдены две или более прибыльных цепочек:\n\
                    - Самая короткая прибыльная цепочка: {:?} с прибылью {:.4}%\n\
                    - Самая прибыльная цепочка: {:?} с прибылью {:.4}%",
                    shortest_profitable_chain.chain,
                    shortest_profitable_chain.profit_percentage,
                    most_profitable_chain.chain,
                    most_profitable_chain.profit_percentage
                );

                if shortest_profitable_chain.profit_percentage > 1.0 {
                    if most_profitable_chain.profit_percentage > 1.5 {
                        info!(
                            "Самая короткая цепочка прибыльна более чем на 1.0% ({:.2}%), \
                            а самая прибыльная цепочка превышает 1.5% ({:.2}%). \
                            Выбираем самую прибыльную цепочку.",
                            shortest_profitable_chain.profit_percentage,
                            most_profitable_chain.profit_percentage
                        );
                        most_profitable_chain
                    } else {
                        info!(
                            "Самая короткая цепочка прибыльна более чем на 1.0% ({:.2}%), \
                            а самая прибыльная цепочка НЕ превышает 1.5% ({:.2}%). \
                            Выбираем самую короткую цепочку.",
                            shortest_profitable_chain.profit_percentage,
                            most_profitable_chain.profit_percentage
                        );
                        shortest_profitable_chain
                    }
                } else if shortest_profitable_chain.profit_percentage <= 1.0 && most_profitable_chain.profit_percentage > 1.5 {
                    info!(
                        "Самая короткая цепочка НЕ превышает 1.0% ({:.2}%), \
                        а самая прибыльная цепочка превышает 1.5% ({:.2}%). \
                        Выбираем самую прибыльную цепочку.",
                        shortest_profitable_chain.profit_percentage,
                        most_profitable_chain.profit_percentage
                    );
                    most_profitable_chain
                } else {
                    info!(
                        "В остальных случаях выбираем самую короткую цепочку: {:?} с прибылью {:.4}%",
                        shortest_profitable_chain.chain,
                        shortest_profitable_chain.profit_percentage
                    );
                    shortest_profitable_chain
                }
            }
        };

        info!("Используем цепочку: {:?}", selected_chain.chain);
        
        Ok(selected_chain)
    }

    /*
    async fn get_most_profitable_chain(&mut self) -> Result<ChainResult, Box<dyn std::error::Error + Send + Sync>> {
        info!("Начало поиска наиболее прибыльной цепочки");
        let profitable_chains = self.math_graph.profitable_chains.clone();
        
        // Очистить текущий profitable_chain_store перед обновлением
        self.profitable_chain_store.chains.clear();
    
        info!("Обновление profitable_chain_store с {} цепочками", profitable_chains.len());
        for chain_result in profitable_chains.iter() {
            self.profitable_chain_store.add_or_update_chain(
                chain_result.clone(),
                self.config.basic_threshold,
            );
        }
    
        // Вывод всех цепочек в profitable_chain_store
        info!("Текущие цепочки в profitable_chain_store:");
        for entry in self.profitable_chain_store.chains.iter() {
            info!("Цепочка: {:?}, Прибыль: {:.4}%", entry.key(), entry.value().profit_percentage * 100.0);
        }
    
        info!("Обновление валидности цепочек");
        self.profitable_chain_store.update_chain_validity(self.config.basic_threshold);
        
        info!("Получение валидных цепочек");
        let valid_chains = self.profitable_chain_store.get_valid_chains(
            self.config.basic_threshold,
            self.config.profit_threshold,
        );
        
        if valid_chains.is_empty() {
            info!("Не найдено валидных прибыльных цепочек");
            return Err("No profitable chains found".into());
        }
        
        info!("Найдено {} валидных цепочек", valid_chains.len());
        let best_chain_info = valid_chains
            .into_iter()
            .max_by(|a, b| a.profit_percentage.partial_cmp(&b.profit_percentage).unwrap())
            .unwrap();
    
        info!("Best chain info: {:?}", best_chain_info.chain);
        
        if let Some(chain_result) = &best_chain_info.chain_result {
            info!("Выбрана цепочка: {:?}", chain_result.chain);
            info!("Ожидаемая прибыль: {:.2}%", chain_result.profit_percentage * 100.0);
            info!("Количество выполнений цепочки: {}", best_chain_info.count);
            
            Ok(chain_result.clone())
        } else {
            // Пересчитываем ChainResult, если он не сохранен
            let result = self.math_graph.calculate_chain_profit(best_chain_info.chain.clone(), self.config.wallet_balance).await?;
            info!("ChainResult пересчитан для цепочки: {:?}", result.chain);
            Ok(result)
        }
    }      
    */

    async fn get_most_profitable_chain(&mut self) -> Result<ChainResult, Box<dyn std::error::Error + Send + Sync>> {
        info!("Начало поиска наиболее прибыльной цепочки");
        let profitable_chains = self.math_graph.profitable_chains.clone();
        
        info!("Обновление profitable_chain_store с {} цепочками", profitable_chains.len());
        for chain_result in profitable_chains.iter() {
            self.profitable_chain_store.add_or_update_chain(
                chain_result.clone(),
                self.config.basic_threshold,
            );
        }

        // Вывод всех цепочек в profitable_chain_store
        info!("Текущие цепочки в profitable_chain_store:");
        for entry in self.profitable_chain_store.chains.iter() {
            info!("Цепочка: {:?}, Прибыль: {:.4}%", entry.key(), entry.value().profit_percentage * 100.0);
        }  
        
        info!("Обновление валидности цепочек");
        self.profitable_chain_store.update_chain_validity(self.config.basic_threshold);
        
        info!("Получение валидных цепочек");
        let valid_chains = self.profitable_chain_store.get_valid_chains(
            self.config.basic_threshold,
            self.config.profit_threshold,
        );
        
        if valid_chains.is_empty() {
            info!("Не найдено валидных прибыльных цепочек");
            return Err("No profitable chains found".into());
        }
        
        info!("Найдено {} валидных цепочек", valid_chains.len());
        let best_chain_info = valid_chains
            .into_iter()
            .max_by(|a, b| a.profit_percentage.partial_cmp(&b.profit_percentage).unwrap())
            .unwrap();

        info!("Best chain info: {:?}", best_chain_info.chain);
        
        if let Some(chain_result) = &best_chain_info.chain_result {
            info!("Выбрана цепочка: {:?}", chain_result.chain);
            info!("Ожидаемая прибыль: {:.2}%", chain_result.profit_percentage * 100.0);
            info!("Количество выполнений цепочки: {}", best_chain_info.count);
            
            Ok(chain_result.clone())
        } else {
            // Пересчитываем ChainResult, если он не сохранен
            let result = self.math_graph.calculate_chain_profit(best_chain_info.chain.clone(), self.config.wallet_balance).await?;
            info!("ChainResult пересчитан для цепочки: {:?}", result.chain);
            Ok(result)
        }
    } 

    fn prepare_order_params(&self, operation_info: &OperationInfo, current_token: &str, balance: f64) -> (String, String, f64, bool) {
        let symbol = operation_info.symbol.clone();
        let side = match operation_info.operation {
            Operation::Multiply => "SELL",
            Operation::Divide => "BUY",
        };
        let is_quote_order = current_token == operation_info.from_token;
        let amount = balance;

        (symbol, side.to_string(), amount, is_quote_order)
    }

    async fn place_market_order(&mut self, symbol: &str, side: &str, amount: f64, is_quote_order: bool) -> Result<(f64, String), Box<dyn std::error::Error + Send + Sync>> {

        let timestamp = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 + self.time_offset) as u64;


        // Получаем step size для данного символа
        let step_size = self.step_size_store.read().await.symbols.get(symbol)
        .and_then(|info| info.lot_size_step_size.parse::<f64>().ok())
        .unwrap_or(0.0);

        // Определяем формат для баланса на основе step_size
        let format_precision = if step_size == 1.0 {
            0
        } else if step_size < 1.0 && step_size > 0.0 {
            let mut precision = 0;
            let mut temp_step_size = step_size;
            while temp_step_size < 1.0 {
                temp_step_size *= 10.0;
                precision += 1;
            }
            precision
        } else {
            8
        };

        // Форматируем amount с учетом precision
        let formatted_amount = Self::format_float_truncate(amount, format_precision);
        info!("Step size for symbol {}: {}", symbol, step_size);

        let amount_param = if side == "SELL" {
            ("quantity", format!("{}", formatted_amount))
        } else {
            ("quoteOrderQty", format!("{:.8}", amount))
        };

        let mut params = vec![
            ("apiKey", self.config.api_key.clone()),
            amount_param,
            ("side", side.to_string()),
            ("symbol", symbol.to_string()),
            ("timestamp", timestamp.to_string()),
            ("newOrderRespType", "FULL".to_string()),
            ("type", "MARKET".to_string()),
        ];
        
        params.sort_by(|a, b| a.0.cmp(&b.0));
        
        let query_string: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<String>>()
            .join("&");

        let mut mac = Hmac::<Sha256>::new_varkey(self.config.api_secret.as_bytes())
            .map_err(|_| "Invalid key length")?;
        mac.update(query_string.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let order_id = ORDER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        
        params.push(("signature", signature.clone()));
        let order_message = json!({
            "id": order_id,
            "method": "order.place",
            "params": serde_json::Map::from_iter(params.into_iter().map(|(k, v)| (k.to_string(), serde_json::Value::String(v))))
        });

        //info!("Order request: {}", order_message);
        info!("Market order placed: Symbol: {}, Side: {}, Amount: {}, Is Quote Order: {}", symbol, side, amount, is_quote_order);

        let operation_start = Instant::now();

        let result = self.order_send(order_id, order_message.to_string()).await?;
    
        info!("Отправка и получение данных сделки {:?}", operation_start.elapsed());

        Ok(result)
    }

    async fn order_send(&mut self, order_id: u64, order_message: String) -> Result<(f64, String), Box<dyn std::error::Error + Send + Sync>> {
        //info!("Sending order message: {}", order_message);
    
        match self.send_message(order_message).await {
            Ok((response_id, new_balance, received_token)) => {
                if response_id == order_id {
                    info!("Order response: New balance: {}, Received token: {}", new_balance, received_token);
                    Ok((new_balance, received_token))
                } else {
                    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Received response for wrong order ID")))
                }
            },
            Err(e) => Err(e),
        }
    }
    
    async fn add_action(&self, action: BotAction) {
        let _ = self.bot_action_sender.send(action.clone());
        let mut actions = self.actions.write().await;
        actions.push(action);
    }
}
