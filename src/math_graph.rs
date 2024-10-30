// binance math_graph.rs
// Код для 600 + токенов 

use log::info;
use async_channel;
use std::sync::Arc;
use anyhow::{Result, anyhow};

use dashmap::DashMap;
use hashbrown::HashMap;

use tokio::task;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

use crate::config::Config;
use crate::data::{LockFreeTokenDataStore, OrderBookUpdate, OrderBookEntry};
use crate::graph::ArbitrageEngine;

pub struct MathGraph {
    config: Arc<Config>,
    arbitrage_engine: Arc<ArbitrageEngine>,
    token_data_store: Arc<LockFreeTokenDataStore>,
    pub profitable_chains: Arc<DashMap<String, ChainResult>>,
    token_info: Arc<RwLock<Vec<TokenInfo>>>,
    cached_chains: Arc<DashMap<usize, Vec<String>>>,
    pub invalid_chains: Arc<DashMap<String, ChainResult>>,
}

impl Clone for MathGraph {
    fn clone(&self) -> Self {
        MathGraph {
            config: Arc::clone(&self.config),
            arbitrage_engine: Arc::clone(&self.arbitrage_engine),
            token_data_store: Arc::clone(&self.token_data_store),
            profitable_chains: Arc::clone(&self.profitable_chains),
            token_info: Arc::clone(&self.token_info),
            cached_chains: Arc::clone(&self.cached_chains),
            invalid_chains: Arc::clone(&self.invalid_chains),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChainResult {
    pub chain: Vec<String>,
    pub profit_percentage: f64,
    pub final_amount: f64,
    pub duration: Duration,
    pub operation_time: Duration,
    pub operations: Vec<OperationInfo>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct OperationInfo {
    pub symbol: String,
    pub is_inverse: bool,
    pub operation: Operation,
    pub from_token: String,
    pub to_token: String,
    pub initial_amount: f64,
    pub final_amount: f64,
    pub entries: Vec<OrderBookEntry>,
}

#[derive(Debug, Clone)]
struct TokenInfo {
    symbol: String,
    base_token: String,
    quote_token: String,
}

#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Multiply,
    Divide,
}

#[allow(unused_variables)]
#[allow(dead_code)]
impl MathGraph {
    pub fn new(config: Arc<Config>, arbitrage_engine: Arc<ArbitrageEngine>, token_data_store: Arc<LockFreeTokenDataStore>) -> Self {
        info!("Инициализация MathGraph");
        let cached_chains = Arc::new(DashMap::new());
        MathGraph {
            config,
            arbitrage_engine,
            token_data_store,
            profitable_chains: Arc::new(DashMap::new()),
            token_info: Arc::new(RwLock::new(Vec::new())),
            cached_chains,
            invalid_chains: Arc::new(DashMap::new()),
        }
    }

    
    pub async fn run(&self) -> Result<()> {
        info!("Запуск MathGraph");
        tokio::time::sleep(Duration::from_secs(90)).await;

        info!("Запуск update_token_info");
        self.update_token_info().await?;

        info!("Получение цепочек из ArbitrageEngine");
        self.get_chains_from_arbitrage_engine().await?;

        info!("Начало вычислений MathGraph");

        let mut last_log = Instant::now();
        let mut first_cycle = true;
    
        loop {
            let cycle_start = Instant::now();
            if let Err(e) = self.calculation_cycle().await {
                eprintln!("Ошибка в цикле вычислений: {:?}", e);
            }
            let cycle_duration = cycle_start.elapsed();
            
            if first_cycle || last_log.elapsed() >= Duration::from_secs(3600) {
                self.hourly_log(cycle_duration).await;
                last_log = Instant::now();
                first_cycle = false;
            }
            info!("Цикл вычислений завершен за {:?}", cycle_start.elapsed());
            //info!("Ожидание нового цикла 10 сек");
            //tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn hourly_log(&self, cycle_duration: Duration) {
        let total_chains = self.cached_chains.len();
        let profitable_chains = self.profitable_chains.len();
        
        info!("Часовой отчет:");
        info!("Время выполнения цикла: {:?}", cycle_duration);
        info!("Всего цепочек: {}", total_chains);
        info!("Прибыльных цепочек: {}", profitable_chains);
    }

    
    async fn update_token_info(&self) -> Result<()> {
        let mut new_token_info = Vec::new();
        for item in self.token_data_store.data.iter() {
            let (symbol, _) = item.pair();
            if let Ok((base_token, quote_token)) = self.split_symbol(symbol) {
                new_token_info.push(TokenInfo {
                    symbol: symbol.to_string(),
                    base_token,
                    quote_token,
                });
            }
        }

        {
            let mut token_info = self.token_info.write().await;
            *token_info = new_token_info;
        }

        #[cfg(debug_assertions)]
        info!("Token info updated");
        self.log_token_info().await;
        Ok(())
    }

    
    fn split_symbol(&self, symbol: &str) -> Result<(String, String)> {
        for quote_currency in &self.config.quote_currencies {
            if symbol.ends_with(quote_currency) {
                let base_currency = symbol[..symbol.len() - quote_currency.len()].to_string();
                return Ok((base_currency, quote_currency.to_string()));
            }
            if symbol.starts_with(quote_currency) {
                let base_currency = symbol[quote_currency.len()..].to_string();
                return Ok((quote_currency.to_string(), base_currency));
            }
        }
        // Если ни один токен из quote_currencies не найден, разделяем символ пополам
        let mid = symbol.len() / 2;
        Ok((symbol[..mid].to_string(), symbol[mid..].to_string()))
    }

    
    async fn log_token_info(&self) {
        let lock = self.token_info.read().await;
        #[cfg(debug_assertions)]
        info!("Информация о токенах:");
        for info in lock.iter() {
            info!("Symbol: {}, Base: {}, Quote: {}", info.symbol, info.base_token, info.quote_token);
        }
    }
    
    async fn calculation_cycle(&self) -> Result<()> {
        let start_time = Instant::now();
    
        let local_chains: Vec<Vec<String>> = self
            .cached_chains
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
    
        let (tx, rx) = async_channel::bounded::<Vec<String>>(1000);

        // Количество рабочих задач:
        // - Все доступные ядра, если их меньше 4
        // - 6 ядер, если доступно более 8 ядер
        // - Количество ядер минус 2 в остальных случаях
        let available_cores = num_cpus::get();
        let num_workers = if available_cores < 4 {
            available_cores
        } else if available_cores > 8 {
            6
        } else {
            available_cores.saturating_sub(2)
        };
        
        let self_arc = Arc::new(self.clone());
    
        // Создаем рабочие задачи
        let mut workers = Vec::new();
        for _ in 0..num_workers {
            let rx = rx.clone();
            let self_clone = Arc::clone(&self_arc);
            workers.push(task::spawn(async move {
                while let Ok(chain) = rx.recv().await {
                    let result = self_clone
                        .calculate_chain_profit(chain, self_clone.config.initial_amount)
                        .await;
                    self_clone.process_chain_result(result).await;
                }
            }));
        }
    
        // Отправляем цепочки в канал для обработки
        for chain in local_chains {
            if let Err(e) = tx.send(chain).await {
                eprintln!("Ошибка при отправке цепочки в канал: {:?}", e);
            }
        }
        drop(tx); // Закрываем отправитель, чтобы рабочие задачи завершились
    
        // Ждем завершения всех рабочих задач
        for worker in workers {
            if let Err(e) = worker.await {
                eprintln!("Ошибка в рабочей задаче: {:?}", e);
            }
        }
    
        info!(
            "Цикл вычислений завершен за {:?}",
            start_time.elapsed()
        );
    
        Ok(())
    }
    
    async fn process_chain_result(&self, result: Result<ChainResult>) {
        match result {
            Ok(chain_result) => {
                if chain_result.profit_percentage > self.config.basic_threshold
                    && chain_result.profit_percentage <= 0.5
                {
                    let chain_key = chain_result.chain.join("->");
                    self.profitable_chains
                        .insert(chain_key.clone(), chain_result.clone());
                    #[cfg(debug_assertions)]
                    info!(
                        "Найдена прибыльная цепочка: {}, прибыль: {:.2}%, длительность: {:?}",
                        chain_key, chain_result.profit_percentage, chain_result.duration
                    );
                } else {
                    self.profitable_chains
                        .remove(&chain_result.chain.join("->"));
                }
            }
            Err(e) => eprintln!("Ошибка при расчете прибыли цепочки: {:?}", e),
        }
    }
    
    async fn get_chains_from_arbitrage_engine(&self) -> Result<()> {
        info!("Получение цепочек из ArbitrageEngine");
        let chain_categories = self.arbitrage_engine.chain_categories.read().await;
        
        let mut index = 0;
        for chain in chain_categories.unique_chains_4.iter() {
            self.cached_chains.insert(index, chain.clone());
            index += 1;
        }
        for chain in chain_categories.unique_chains_5.iter() {
            self.cached_chains.insert(index, chain.clone());
            index += 1;
        }
        for isomorphic_group in chain_categories.isomorphic_chains_4.iter() {
            for chain in isomorphic_group.value() {
                self.cached_chains.insert(index, chain.clone());
                index += 1;
            }
        }
        for isomorphic_group in chain_categories.isomorphic_chains_5.iter() {
            for chain in isomorphic_group.value() {
                self.cached_chains.insert(index, chain.clone());
                index += 1;
            }
        }

        if self.cached_chains.is_empty() {
            info!("Предупреждение: Не получено цепочек из ArbitrageEngine");
        } else {
            info!("Получено {} цепочек из ArbitrageEngine", self.cached_chains.len());
        }
        info!("Получено {} цепочек из ArbitrageEngine", self.cached_chains.len());
        Ok(())
    }

    
    pub async fn calculate_chain_profit(&self, chain: Vec<String>, amount: f64) -> Result<ChainResult> {
        let start_time = Instant::now();
        let chain_start_time = Instant::now();
        let mut amount = amount;
        let mut total_operation_time = Duration::new(0, 0);
        info!("Расчет прибыли для цепочки: {:?}", chain);

        let mut operations = Vec::new();

        for (i, window) in chain.windows(2).enumerate() {
            let from_token = &window[0];
            let to_token = &window[1];

            let (symbol, is_inverse) = self.get_symbol_and_operation(from_token, to_token).await?;
            let order_book = self.get_order_book(&symbol).await?;
            #[cfg(debug_assertions)]
            info!("Обработка пары {}: {} -> {}, Символ: {}, Инвертирована: {}", 
                     i+1, from_token, to_token, symbol, is_inverse);

            let token_info = self.token_info.read().await;
            let pair_info = token_info.iter().find(|info| info.symbol == symbol).unwrap();
            #[cfg(debug_assertions)]
            info!("Символ: {}. Базовый токен: {}, Котируемый токен: {}.", symbol, pair_info.base_token, pair_info.quote_token);

            let operation = self.determine_operation(from_token, to_token, pair_info);
            #[cfg(debug_assertions)]
            info!("  Алгоритмическая операция: {:?}", operation);

            let previous_amount = amount;
            let (new_amount, operation_time) = self.process_pair(&order_book, amount, is_inverse, operation)?;

            total_operation_time += operation_time;

            if new_amount == 0.0 {
                let chain_duration = chain_start_time.elapsed();
                //info!("Недостаточная ликвидность для полной операции. Прерывание расчета цепочки.");
                info!("Время расчета цепочки: {:?}", chain_duration);
                
                let chain_result = ChainResult {
                    chain: chain.clone(),
                    profit_percentage: -1.0,
                    final_amount: 0.0,
                    duration: start_time.elapsed(),
                    operation_time: total_operation_time,
                    operations: operations.clone(),
                };
                
                //self.detailed_log_chain_invalid(&chain_result);
                
                return Ok(chain_result);
            }

            amount = new_amount;
        
            #[cfg(debug_assertions)]
            info!("Шаг {}: {} {} -> {} {}, Количество: {} -> {}, Время операции: {:?}", 
            i+1, previous_amount, from_token, amount, to_token, previous_amount, amount, operation_time);
    
            // Выбираем правильный стакан на основе операции
            let entries = match operation {
                Operation::Multiply => order_book.bids_updates.clone(),
                Operation::Divide => order_book.asks_updates.clone(),
            };

            operations.push(OperationInfo {
                symbol: symbol.clone(),
                is_inverse,
                operation,
                from_token: from_token.to_string(),
                to_token: to_token.to_string(),
                initial_amount: previous_amount,
                final_amount: amount,
                entries,
            });
        }

        let profit_percentage = (amount / self.config.initial_amount) - 1.0;
        let duration = start_time.elapsed();
        let chain_duration = chain_start_time.elapsed();
    
        if profit_percentage > 0.5 { // 0.5 соответствует 50%
            info!("Цепочка отклонена из-за слишком высокой прибыли: {:.2}%. Время расчета: {:?}", profit_percentage * 100.0, chain_duration);
                  let chain_result = ChainResult {
                    chain: chain.clone(),
                    profit_percentage,
                    final_amount: amount,
                    duration,
                    operation_time: total_operation_time,
                    operations: operations.clone(),
                };
                
                self.invalid_chains.insert(chain.join("->"), chain_result.clone());
                
                self.detailed_log_chain_invalid(&chain_result);
    
                return Ok(ChainResult {
                    chain,
                    profit_percentage: -1.0,
                    final_amount: 0.0,
                    duration,
                    operation_time: total_operation_time,
                    operations,
                });
            }

        #[cfg(debug_assertions)]
        if profit_percentage > self.config.basic_threshold {
            info!("Расчет прибыльной цепочки завершен:");
            info!("Цепочка: {:?}", chain);
            info!("Начальная сумма: {}", self.config.initial_amount);
            info!("Конечная сумма: {}", amount);
            info!("Процент прибыли: {:.2}%", profit_percentage * 100.0);
            info!("Общее время расчета: {:?}", duration);
            info!("Общее время расчета цепочки: {:?}", chain_duration);
            info!("Время выполнения операций: {:?}", total_operation_time);
        } else {
            info!("Процент {:.2}%. Время расчета: {:?}. Цепочка {:?}", profit_percentage * 100.0, chain_duration, chain);
        }
            
        Ok(ChainResult {
            chain,
            profit_percentage,
            final_amount: amount,
            duration,
            operation_time: total_operation_time,
            operations,
        })
    }

    
    async fn get_symbol_and_operation(&self, from: &str, to: &str) -> Result<(String, bool)> {
        let token_info = self.token_info.read().await;
        let direct = token_info.iter().find(|info| info.base_token == from && info.quote_token == to);
        let inverse = token_info.iter().find(|info| info.base_token == to && info.quote_token == from);

        match (direct, inverse) {
            (Some(info), _) => {
                #[cfg(debug_assertions)]
                info!("Операция: {} -> {}", from, to);

                Ok((info.symbol.clone(), false))
            },
            (_, Some(info)) => {
                #[cfg(debug_assertions)]
                info!("Операция: {} -> {}", from, to);

                Ok((info.symbol.clone(), true))
            },
            _ => {
                info!("Не найдена торговая пара для {} и {}", from, to);
                Err(anyhow!("Не найдена торговая пара для {} и {}", from, to))
            }
        }
    }
    
    fn determine_operation(&self, from: &str, to: &str, pair_info: &TokenInfo) -> Operation {
        if from == pair_info.base_token && to == pair_info.quote_token {
            Operation::Multiply
        } else if to == pair_info.base_token && from == pair_info.quote_token {
            Operation::Divide
        } else {
            // Добавляем случай по умолчанию, если ни одно из условий не выполняется
            panic!("Паника в fn determine_operation !!! Неожиданная комбинация токенов !!! ")
        }
    }
    
    async fn get_order_book(&self, symbol: &str) -> Result<OrderBookUpdate> {
        if let Some(entry) = self.token_data_store.data.get(symbol) {
            let order_book = entry.value().data.order_book_update.clone();
            #[cfg(debug_assertions)]
            info!("Получена книга ордеров для символа: {}, orberbook {:?}", symbol, order_book);
            Ok(order_book)
        } else {
            #[cfg(debug_assertions)]
            info!("Книга ордеров не найдена для символа: {}", symbol);
            Err(anyhow!("Книга ордеров не найдена для символа: {}", symbol))
        }
    }
    
    fn process_pair(&self, order_book: &OrderBookUpdate, amount: f64, _is_inverse: bool, operation: Operation) -> Result<(f64, Duration)> {
        let start_time = Instant::now();
        #[cfg(debug_assertions)]
        info!("Обработка пары с начальной суммой: {}", amount);
        let mut remaining = amount;
        let mut result = 0.0;
    
        // Выбор стакана только на основе типа операции
        let entries = match operation {
            Operation::Multiply => &order_book.bids_updates,
            Operation::Divide => &order_book.asks_updates,
        };
        #[cfg(debug_assertions)]
        info!("Используется стакан: {}", match operation {
            Operation::Multiply => "Bids",
            Operation::Divide => "Asks",
        });
    
        // Проверка ликвидности
        let total_available_volume: f64 = match operation {
            Operation::Multiply => entries.iter().map(|entry| entry.volume).sum(),
            Operation::Divide => entries.iter().map(|entry| entry.volume * entry.price).sum(),
        };

        if total_available_volume < amount {
            info!("Недостаточная ликвидность для операции.");
            info!("Требуемый объем: {}, Доступный объем: {}", amount, total_available_volume);
            info!("Стакан ордербука:");
            for (i, entry) in entries.iter().enumerate().take(10) {
                info!("  Уровень {}: Цена = {}, Объем = {}", i+1, entry.price, entry.volume);
            }
            return Ok((0.0, start_time.elapsed()));
        }
    
        for (i, entry) in entries.iter().enumerate() {
            let (volume, acquired) = match operation {
                Operation::Multiply => {
                    let volume = remaining.min(entry.volume);
                    (volume, volume * entry.price)
                },
                Operation::Divide => {
                    let max_volume = remaining / entry.price;
                    let volume = max_volume.min(entry.volume);
                    (volume, volume * entry.price)
                },
            };

            result += match operation {
                Operation::Multiply => acquired,
                Operation::Divide => volume,
            };
            remaining -= match operation {
                Operation::Multiply => volume,
                Operation::Divide => acquired,
            };

            #[cfg(debug_assertions)]
            info!("Шаг {}: Объем: {}, Цена: {}, Получено: {}, Осталось: {}", 
                     i+1, volume, entry.price, match operation {
                         Operation::Multiply => acquired,
                         Operation::Divide => volume,
                     }, remaining);

            if remaining <= 0.0 {
                break;
            }
        }
    
        let elapsed = start_time.elapsed();
        info!("Обработка пары завершена. Результат: {}, Время: {:?}", result, elapsed);
        Ok((result, elapsed))
    }

    pub async fn find_chains_with_token(&self, token: &str) -> Vec<Vec<String>> {
        let cycle_start = Instant::now();
        info!("Starting find_chains_with_token for token: {}", token);
    
        let mut unique_trimmed_chains: HashMap<Vec<String>, Vec<String>> = HashMap::new();
        let mut processed_chains = 0;
    
        for entry in self.cached_chains.iter() {
            let chain = entry.value();
            if let Some(start_index) = chain.iter().position(|t| t == token) {
                processed_chains += 1;
                let trimmed_chain = chain[start_index..].to_vec();
                // Используем HashMap для обеспечения уникальности на основе trimmed_chain
                unique_trimmed_chains.entry(trimmed_chain).or_insert_with(|| chain.clone());
            }
        }
    
        info!("Total processed chains: {}", processed_chains);
        info!("Relevant unique trimmed chains found: {}", unique_trimmed_chains.len());
        info!("Нахождение новых цепочек в math_graph завершено за: {:?}", cycle_start.elapsed());
    
        // Возвращаем только уникальные обрезанные цепочки
        unique_trimmed_chains.keys().cloned().collect()
    }
    

    #[cfg(debug_assertions)]
    fn log_results(&self) {
        info!("Результаты вычислений MathGraph:");
        info!("Всего прибыльных цепочек: {}", self.profitable_chains.len());

        for (i, result) in self.profitable_chains.iter().take(3).enumerate() {
            info!("Цепочка {}: {}", i+1, result.key());
            info!("  Прибыль: {:.4}%", result.profit_percentage * 100.0);
            info!("  Конечная сумма: {:.4}", result.final_amount);
            info!("  Время расчета: {:?}", result.duration);

        // Вызываем новую функцию для детального логирования
        self.detailed_log_chain(result.value());
        }
    }

    detailed_log_chain_invalid_macro!(self, chain_result);

    #[cfg(debug_assertions)]
    fn detailed_log_chain(&self, chain_result: &ChainResult) {
        info!("\n=== Детальный отчет по прибыльной цепочке ===");
        info!("Цепочка: {}", chain_result.chain.join(" -> "));
        info!("Итоговая прибыль: {:.4}%", chain_result.profit_percentage * 100.0);
        info!("Начальная сумма: {} {}", self.config.initial_amount, self.config.start_end_token);
        info!("Конечная сумма: {:.8} {}", chain_result.final_amount, self.config.start_end_token);
        info!("Общее время выполнения: {:?}", chain_result.duration);
        info!("Время выполнения операций: {:?}", chain_result.operation_time);

        let mut current_amount = self.config.initial_amount;

        for (i, window) in chain_result.chain.windows(2).enumerate() {
            let from_token = &window[0];
            let to_token = &window[1];

            if let Ok((symbol, is_inverse)) = futures::executor::block_on(self.get_symbol_and_operation(from_token, to_token)) {
                info!("\nШаг {}: {} -> {}", i + 1, from_token, to_token);
                info!("  Торговая пара: {}", symbol);
                info!("  Инвертированная операция: {}", is_inverse);

                if let Ok(order_book) = futures::executor::block_on(self.get_order_book(&symbol)) {
                    let token_info = futures::executor::block_on(self.token_info.read());
                    if let Some(pair_info) = token_info.iter().find(|info| info.symbol == symbol) {
                        let operation = self.determine_operation(from_token, to_token, pair_info);
                        
                        info!("  Операция: {:?}", operation);
                        info!("  Начальная сумма на этом шаге: {:.8} {}", current_amount, from_token);

                        let entries = match operation {
                            Operation::Multiply => &order_book.bids_updates,
                            Operation::Divide => &order_book.asks_updates,
                        };

                        info!("  Используемый стакан: {}", if matches!(operation, Operation::Multiply) { "Bids" } else { "Asks" });
                        
                        let mut remaining = current_amount;
                        let mut result = 0.0;

                        for (j, entry) in entries.iter().enumerate() {
                            let (volume, acquired) = match operation {
                                Operation::Multiply => {
                                    let volume = remaining.min(entry.volume);
                                    (volume, volume * entry.price)
                                },
                                Operation::Divide => {
                                    let max_volume = remaining / entry.price;
                                    let volume = max_volume.min(entry.volume);
                                    (volume, volume * entry.price)
                                },
                            };

                            result += match operation {
                                Operation::Multiply => acquired,
                                Operation::Divide => volume,
                            };
                            remaining -= match operation {
                                Operation::Multiply => volume,
                                Operation::Divide => acquired,
                            };

                            info!("    Уровень {}: Цена = {:.8}, Объем = {:.8}", j + 1, entry.price, entry.volume);
                            info!("      Использовано: {:.8}, Получено: {:.8}", 
                                     match operation {
                                         Operation::Multiply => volume,
                                         Operation::Divide => acquired,
                                     },
                                     match operation {
                                         Operation::Multiply => acquired,
                                         Operation::Divide => volume,
                                     }
                            );

                            if remaining <= 0.0 {
                                break;
                            }
                        }

                        info!("  Итоговая сумма после операции: {:.8} {}", result, to_token);
                        current_amount = result;
                    }
                }
            }
        }

        info!("\nИтоговый результат:");
        info!("  Начальная сумма: {} {}", self.config.initial_amount, self.config.start_end_token);
        info!("  Конечная сумма: {:.8} {}", chain_result.final_amount, self.config.start_end_token);
        info!("  Прибыль: {:.2}%", chain_result.profit_percentage * 100.0);
        info!("=====================================\n");
    }
}
