use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use std::sync::Arc;
use tokio::time::{interval, Duration, Instant};
use anyhow::Result;
use chrono::Local;
use hashbrown::HashMap;
use log::info;
use std::path::Path;

use crate::math_graph::ChainResult;
//use crate::math_graph::Operation;
use crate::brain_bot::BotAction;
use crate::config::Config;

const BUFFER_SIZE: usize = 1000;
const BUFFER_TIMEOUT: Duration = Duration::from_millis(100);
const ITERATION_DURATION: Duration = Duration::from_secs(10);

pub struct Logger {
    chain_file: Option<File>,
    bot_file: Option<File>,
    chain_buffer: String,
    bot_buffer: String,
    iteration_start: Instant,
    chain_receiver: mpsc::Receiver<ChainResult>,
    bot_receiver: mpsc::Receiver<BotAction>,
    last_recorded_chains: HashMap<String, ChainResult>,
    config: Arc<Config>,
}

impl Logger {
    pub fn new(chain_receiver: mpsc::Receiver<ChainResult>, bot_receiver: mpsc::Receiver<BotAction>, config: Arc<Config>) -> Self {
        Logger {
            chain_file: None,
            bot_file: None,
            chain_buffer: String::with_capacity(BUFFER_SIZE),
            bot_buffer: String::with_capacity(BUFFER_SIZE),
            iteration_start: Instant::now(),
            chain_receiver,
            bot_receiver,
            last_recorded_chains: HashMap::new(),
            config,
        }
    }

    async fn create_log_file(prefix: &str) -> Result<File> {
        let mut count = 1;
        let mut file_name;
        let mut path;

        // Сначала проверяем существование файла с count = 1
        file_name = format!("{}_{}.txt", prefix, count);
        path = std::env::current_dir()?.join(&file_name);
        
        if Path::new(&path).exists() {
            // Проверяем, пустой ли файл
            let metadata = tokio::fs::metadata(&path).await?;
            if metadata.len() == 0 {
                // Если файл существует и пустой, используем его
                info!("Использование существующего пустого файла логов: {}", file_name);
                return Ok(OpenOptions::new().create(true).append(true).open(path).await?);
            }
            
            // Если файл не пустой, ищем следующее доступное имя
            count += 1;
            loop {
                file_name = format!("{}_{}.txt", prefix, count);
                path = std::env::current_dir()?.join(&file_name);
                
                if !Path::new(&path).exists() {
                    break;
                }
                count += 1;
            }
        }

        info!("Создание файла логов: {}", file_name);
        Ok(OpenOptions::new().create(true).append(true).open(path).await?)
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Logger started");
        self.chain_file = Some(Self::create_log_file("log_binance").await?);
        self.bot_file = Some(Self::create_log_file("bot_binance").await?);

        let mut flush_interval = interval(BUFFER_TIMEOUT);

        loop {
            tokio::select! {
                _ = flush_interval.tick() => {
                    if !self.chain_buffer.is_empty() {
                        self.write_chain_buffer().await?;
                    }
                    if !self.bot_buffer.is_empty() {
                        self.write_bot_buffer().await?;
                    }
                }
                Some(chain_result) = self.chain_receiver.recv() => {
                    self.process_chain_result(chain_result).await?;
                }
                Some(bot_action) = self.bot_receiver.recv() => {
                    self.log_bot_action(bot_action).await?;
                }
                else => break,
            }
        }

        info!("Logger stopped");
        Ok(())
    }

    async fn process_chain_result(&mut self, chain_result: ChainResult) -> Result<()> {   
         
        // Проверяем, является ли цепочка невалидной (прибыль > 50%)
        let is_invalid = chain_result.profit_percentage > 0.5;
        let chain_key = chain_result.chain.join("->");

        let should_record = if let Some(last_result) = self.last_recorded_chains.get(&chain_key) {
            // Цепочка уже существует, проверяем, изменилась ли она
            last_result.profit_percentage != chain_result.profit_percentage 
            || last_result.final_amount != chain_result.final_amount
        } else {
            // Цепочка новая (включая случай, когда это первая цепочка)
            true
        };
    
        if should_record {
            let now = Instant::now();
            if now.duration_since(self.iteration_start) >= ITERATION_DURATION {
                self.chain_buffer.push_str("\n\n");
                self.iteration_start = now;
            }
    
            let mut log_entry = self.detailed_log_chain(&chain_result);
            
            // Добавляем информацию о времени выполнения операций
            log_entry.push_str(&format!("Время выполнения операций: {:?}\n\n", chain_result.operation_time));
    
            // Добавляем пометку о том, является ли цепочка невалидной
            if is_invalid {
                log_entry.insert_str(0, "[НЕВАЛИДНАЯ ЦЕПОЧКА] ");
            }
    
            // Добавляем информацию о том, новая это цепочка или обновленная
            let status = if self.last_recorded_chains.contains_key(&chain_key) {
                "обновлена"
            } else {
                "обнаружена"
            };
            log_entry.insert_str(0, &format!("[{}] Прибыльная цепочка {} :\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            status));
    
            self.chain_buffer.push_str(&log_entry);
    
            // Обновляем последнюю записанную версию цепочки
            self.last_recorded_chains.insert(chain_key, chain_result);
    
            if self.chain_buffer.len() >= BUFFER_SIZE {
                self.write_chain_buffer().await?;
            }
        }
    
        Ok(())
    }

    async fn write_chain_buffer(&mut self) -> Result<()> {
        if let Some(file) = &mut self.chain_file {
            file.write_all(self.chain_buffer.as_bytes()).await?;
            file.flush().await?;
            self.chain_buffer.clear();
        }
        Ok(())
    }

    pub async fn log_bot_action(&mut self, action: BotAction) -> Result<()> {
        let now = Local::now();
        let log_entry = match action {
            BotAction::CycleStart => format!("[{}] Bot cycle started\n", now),
            BotAction::ChainSelected { chain, expected_profit } => {
                format!("[{}] Selected chain: {:?}, Expected profit: {:.2}%\n", 
                        now, chain, expected_profit * 100.0)
            },
            BotAction::OperationExecuted { 
                from_token, to_token, symbol, operation, amount, previous_balance, new_balance 
            } => {
                format!("[{}] Operation executed: {} -> {}, Symbol: {}, Operation: {}, Amount: {:?}, Balance: {:?} -> {:?}\n",
                        now, from_token, to_token, symbol, operation, amount, previous_balance, new_balance)
            },
            BotAction::CycleCompleted { initial_balance, final_balance, profit_percentage, intermediate_results: _} => {
                format!("[{}] Cycle completed: Initial balance: {:?}, Final balance: {:?}, Profit: {:.2}%\n",
                        now, initial_balance, final_balance, profit_percentage)
            },
        };

        self.bot_buffer.push_str(&log_entry);
        
        if self.bot_buffer.len() >= BUFFER_SIZE {
            self.write_bot_buffer().await?;
        }

        Ok(())
    }

    async fn write_bot_buffer(&mut self) -> Result<()> {
        if let Some(file) = &mut self.bot_file {
            file.write_all(self.bot_buffer.as_bytes()).await?;
            file.flush().await?;
            self.bot_buffer.clear();
        }
        Ok(())
    }

    fn detailed_log_chain(&self, chain_result: &ChainResult) -> String {
        let mut log_entry = String::new();
        
        log_entry.push_str(&format!("\n=== Детальный отчет по цепочке ===\n"));
        log_entry.push_str(&format!("Цепочка: {}\n", chain_result.chain.join(" -> ")));
        log_entry.push_str(&format!("Итоговая прибыль: {:.2}%\n", chain_result.profit_percentage * 100.0));
        log_entry.push_str(&format!("Начальная сумма: {} {}\n", self.config.initial_amount, self.config.start_end_token));
        log_entry.push_str(&format!("Конечная сумма: {:.8} {}\n", chain_result.final_amount, self.config.start_end_token));
        log_entry.push_str(&format!("Общее время выполнения: {:?}\n", chain_result.duration));
        log_entry.push_str(&format!("Время выполнения операций: {:?}\n", chain_result.operation_time));

        /*
        for (i, operation) in chain_result.operations.iter().enumerate() {
            log_entry.push_str(&format!("\nШаг {}: {} -> {}\n", i + 1, operation.from_token, operation.to_token));
            log_entry.push_str(&format!("  Торговая пара: {}\n", operation.symbol));
            log_entry.push_str(&format!("  Инвертированная операция: {}\n", operation.is_inverse));
            log_entry.push_str(&format!("  Операция: {:?}\n", operation.operation));
            log_entry.push_str(&format!("  Начальная сумма на этом шаге: {:.8} {}\n", operation.initial_amount, operation.from_token));
            
            log_entry.push_str(&format!("  Используемый стакан: {}\n", 
                if matches!(operation.operation, Operation::Multiply) { "Bids" } else { "Asks" }));
            
                for (j, entry) in operation.entries.iter().enumerate().take(5) {
                    log_entry.push_str(&format!("    Уровень {}: Цена = {:.8}, Объем = {:.8}\n", j + 1, entry.price, entry.volume));
            }

            log_entry.push_str(&format!("  Итоговая сумма после операции: {:.8} {}\n", operation.final_amount, operation.to_token));
        }
        */

        log_entry.push_str("\nИтоговый результат:\n");
        log_entry.push_str(&format!("  Начальная сумма: {} {}\n", self.config.initial_amount, self.config.start_end_token));
        log_entry.push_str(&format!("  Конечная сумма: {:.8} {}\n", chain_result.final_amount, self.config.start_end_token));
        log_entry.push_str(&format!("  Прибыль: {:.2}%\n", chain_result.profit_percentage * 100.0));
        log_entry.push_str("=====================================\n");
        
        log_entry
    }
}
