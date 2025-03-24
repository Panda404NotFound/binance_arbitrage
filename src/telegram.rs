use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep, Duration};

use teloxide::types::{ChatId, ParseMode};
use teloxide::prelude::*;

use log::info;
use chrono::Local;
use hashbrown::HashMap;

use crate::brain_bot::BotAction;
use crate::error_status::ErrorStatus;

pub struct TelegramBot {
    bot: Bot,
    chat_id: ChatId,
    error_status: Arc<RwLock<ErrorStatus>>,
    last_sent_errors: Arc<RwLock<HashMap<String, String>>>,
    bot_action_sender: broadcast::Sender<BotAction>,
    shutdown_signal: Arc<RwLock<bool>>,
}

impl TelegramBot {
    pub fn new(
        token: &str,
        chat_id: i64,
        error_status: Arc<RwLock<ErrorStatus>>,
        bot_action_sender: broadcast::Sender<BotAction>,
    ) -> Self {
        let bot = Bot::new(token);
        let chat_id = ChatId(chat_id);
        TelegramBot {
            bot,
            chat_id,
            error_status,
            last_sent_errors: Arc::new(RwLock::new(HashMap::new())),
            bot_action_sender,
            shutdown_signal: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn shutdown(&self) {
        let mut signal = self.shutdown_signal.write().await;
        *signal = true;
    }

    pub async fn run(self: Arc<Self>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        
        let shutdown_signal_clone = Arc::clone(&self.shutdown_signal);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if *shutdown_signal_clone.read().await {
                    let _ = shutdown_tx.send(());
                    break;
                }
            }
        });

        let mut bot_action_receiver = self.bot_action_sender.subscribe();
        let telegram_bot_clone = Arc::clone(&self);
        let mut current_chat_id = self.chat_id;

        tokio::spawn(async move {
            telegram_bot_clone.test_message(current_chat_id).await;
        });

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Получен сигнал завершения для TelegramBot");
                    break;
                }
                _ = sleep(Duration::from_millis(1000)) => {
                    // Продолжаем обычное выполнение
                }
            }
            
            self.clear_resolved_errors().await;

            while let Ok(action) = bot_action_receiver.try_recv() {
                match action {
                    BotAction::CycleCompleted {
                        initial_balance,
                        final_balance,
                        profit_percentage,
                        ..
                    } => {
                        if let Err(e) = self
                            .send_completed_cycle_message(
                                &mut current_chat_id,
                                initial_balance,
                                final_balance,
                                profit_percentage,
                                None,
                            )
                            .await
                        {
                            eprintln!("Не удалось отправить сообщение о завершении цикла: {:?}", e);
                        }
                    }
                    BotAction::ChainSelected { chain, expected_profit } => {
                        if let Err(e) = self
                            .send_completed_cycle_message(
                                &mut current_chat_id,
                                0.0,
                                0.0,
                                expected_profit,
                                Some(chain),
                            )
                            .await
                        {
                            eprintln!("Не удалось отправить сообщение о выборе цепочки: {:?}", e);
                        }
                    }
                    _ => {}
                }
            }

            let error_status = self.error_status.read().await.clone();
            drop(error_status);
            
            let mut errors_to_send = Vec::new();
            {
                let last_sent_errors = self.last_sent_errors.read().await;
                
                if let Some(error) = &self.error_status.read().await.handle_shutdown_signal_error {
                    if last_sent_errors.get("handle_shutdown_signal_error") != Some(error) {
                        errors_to_send.push(("handle_shutdown_signal_error".to_string(), 
                                            "Ошибка обработки сигнала завершения".to_string(), 
                                            error.clone()));
                    }
                }

                if let Some(error) = &self.error_status.read().await.internet_connection_failures {
                    if last_sent_errors.get("internet_connection_failures") != Some(error) {
                        errors_to_send.push(("internet_connection_failures".to_string(), 
                                            "Ошибка подключения к интернету".to_string(), 
                                            error.clone()));
                    }
                }

                if let Some(error) = &self.error_status.read().await.restart_process_error {
                    if last_sent_errors.get("restart_process_error") != Some(error) {
                        errors_to_send.push(("restart_process_error".to_string(), 
                                            "Ошибка рестарта процесса".to_string(), 
                                            error.clone()));
                    }
                }

                if let Some(state) = &self.error_status.read().await.function_startup_state {
                    if last_sent_errors.get("function_startup_state") != Some(state) {
                        errors_to_send.push(("function_startup_state".to_string(), 
                                            "Состояние запуска функции".to_string(), 
                                            state.clone()));
                    }
                }
            }
            
            for (key, error_type, error_message) in errors_to_send {
                if key == "function_startup_state" {
                    if let Err(e) = self
                        .send_info_message_with_retry(
                            &mut current_chat_id,
                            &error_type,
                            &error_message,
                        )
                        .await
                    {
                        eprintln!("Не удалось отправить сообщение: {:?}", e);
                    } else {
                        let mut last_sent = self.last_sent_errors.write().await;
                        last_sent.insert(key, error_message);
                    }
                } else {
                    if let Err(e) = self
                        .send_error_message_with_retry(
                            &mut current_chat_id,
                            &error_type,
                            &error_message,
                        )
                        .await
                    {
                        eprintln!("Не удалось отправить сообщение: {:?}", e);
                    } else {
                        let mut last_sent = self.last_sent_errors.write().await;
                        last_sent.insert(key, error_message);
                    }
                }
            }
        }
        
        info!("TelegramBot корректно завершил работу");
    }

    async fn clear_resolved_errors(&self) {
        let error_status = self.error_status.read().await;
        let error_state = error_status.clone();
        drop(error_status);
        
        let mut last_sent_errors = self.last_sent_errors.write().await;
        
        if error_state.handle_shutdown_signal_error.is_none() {
            last_sent_errors.remove("handle_shutdown_signal_error");
        }
        if error_state.internet_connection_failures.is_none() {
            last_sent_errors.remove("internet_connection_failures");
        }
        if error_state.restart_process_error.is_none() {
            last_sent_errors.remove("restart_process_error");
        }
        if error_state.function_startup_state.is_none() {
            last_sent_errors.remove("function_startup_state");
        }
    }

    async fn send_error_message_with_retry(
        &self,
        current_chat_id: &mut ChatId,
        error_type: &str,
        error_message: &str,
    ) -> Result<(), teloxide::RequestError> {
        let message = format!(
            "⚠️ Обнаружена ошибка\n\nТип: {}\nСообщение: `{}`",
            error_type, error_message
        );

        let max_retries = 3;
        for _ in 0..max_retries {
            match self.send_message_with_retry(*current_chat_id, &message).await {
                Ok(new_id) => {
                    if new_id != current_chat_id.0 {
                        println!("Чат был перенесен. Обновление chat ID до: {}", new_id);
                        *current_chat_id = ChatId(new_id);
                    }
                    return Ok(());
                }
                Err(teloxide::RequestError::MigrateToChatId(new_id)) => {
                    println!("Чат был перенесен. Обновление chat ID до: {}", new_id);
                    *current_chat_id = ChatId(new_id);
                }
                Err(e) => {
                    eprintln!("Не удалось отправить сообщение: {:?}. Повторная попытка...", e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Err(teloxide::RequestError::Api(teloxide::ApiError::Unknown(
            "Превышено количество попыток отправки сообщения".into(),
        )))
    }

    async fn send_message_with_retry(
        &self,
        chat_id: ChatId,
        message: &str,
    ) -> Result<i64, teloxide::RequestError> {
        let max_retries = 3;
        for attempt in 1..=max_retries {
            let request_future = self
                .bot
                .send_message(chat_id, message)
                .parse_mode(ParseMode::Html)
                .send();
            
            match tokio::time::timeout(Duration::from_secs(5), request_future).await {
                Err(_) => {
                    info!("Тайм-аут при отправке сообщения (попытка {}), повторная попытка...", attempt);
                    if attempt == max_retries {
                        return Err(teloxide::RequestError::Api(teloxide::ApiError::Unknown(
                            "Тайм-аут API запроса".into(),
                        )));
                    }
                    sleep(Duration::from_secs(1)).await;
                    continue;
                },
                Ok(api_result) => match api_result {
                    Ok(message) => {
                        info!("Сообщение успешно отправлено. Ответ сервера: {:?}", message);
                        return Ok(message.chat.id.0);
                    }
                    Err(teloxide::RequestError::MigrateToChatId(new_id)) => {
                        println!("Чат был перенесен. Новый chat ID: {}", new_id);
                        return Ok(new_id);
                    }
                    Err(e) => {
                        if attempt < max_retries {
                            eprintln!(
                                "Не удалось отправить сообщение (попытка {}): {:?}. Повторная попытка...",
                                attempt, e
                            );
                            sleep(Duration::from_millis(500 * 2_u64.pow(attempt as u32))).await;
                        } else {
                            eprintln!(
                                "Не удалось отправить сообщение после {} попыток: {:?}",
                                max_retries, e
                            );
                            return Err(e);
                        }
                    }
                }
            }
        }
        Err(teloxide::RequestError::Api(teloxide::ApiError::Unknown(
            "Превышено количество попыток отправки сообщения".into(),
        )))
    }

    async fn test_message(&self, mut chat_id: ChatId) {
        let mut interval = interval(Duration::from_secs(3600));

        loop {
            interval.tick().await;
            let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
            let message = format!("🤖 Система работает. Почасовой отчет! Текущее время: {}", current_time);

            info!("Отправка сообщения: {}", message);

            match self.send_message_with_retry(chat_id, &message).await {
                Ok(new_id) => {
                    if new_id != chat_id.0 {
                        println!("Чат был перенесен. Обновление chat ID до: {}", new_id);
                        chat_id = ChatId(new_id);
                    }
                    info!("Тестовое сообщение успешно отправлено");
                }
                Err(e) => {
                    eprintln!("Не удалось отправить тестовое сообщение: {:?}", e);
                }
            }
        }
    }

    async fn send_completed_cycle_message(
        &self,
        current_chat_id: &mut ChatId,
        initial_balance: f64,
        final_balance: f64,
        profit_percentage: f64,
        chain: Option<Vec<String>>,
    ) -> Result<(), teloxide::RequestError> {
        let message = if let Some(chain) = chain {
            format!(
                "🔗 Новая цепочка выбрана\n\n\
                📈 Ожидаемая прибыль: `{:.2}%`\n\
                🔗 Цепочка: `{}`",
                profit_percentage * 100.0,
                chain.join(" -> ")
            )
        } else {
            format!(
                "🎉 Торговый цикл завершен\n\n\
                💰 Начальный баланс: `{:.8} USDT`\n\
                💼 Финальный баланс: `{:.8} USDT`\n\
                📈 Прибыль: `{:.2}%`",
                initial_balance,
                final_balance,
                profit_percentage,
            )
        };

        self.send_message_with_retry(*current_chat_id, &message).await?;
        Ok(())
    }

    async fn send_info_message_with_retry(
        &self,
        current_chat_id: &mut ChatId,
        info_type: &str,
        info_message: &str,
    ) -> Result<(), teloxide::RequestError> {
        let message = format!(
            "ℹ️ Информация\n\nТип: {}\nСообщение: `{}`",
            info_type, info_message
        );

        let max_retries = 3;
        for _ in 0..max_retries {
            match self.send_message_with_retry(*current_chat_id, &message).await {
                Ok(new_id) => {
                    if new_id != current_chat_id.0 {
                        println!("Чат был перенесен. Обновление chat ID до: {}", new_id);
                        *current_chat_id = ChatId(new_id);
                    }
                    return Ok(());
                }
                Err(teloxide::RequestError::MigrateToChatId(new_id)) => {
                    println!("Чат был перенесен. Обновление chat ID до: {}", new_id);
                    *current_chat_id = ChatId(new_id);
                }
                Err(e) => {
                    eprintln!("Не удалось отправить сообщение: {:?}. Повторная попытка...", e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Err(teloxide::RequestError::Api(teloxide::ApiError::Unknown(
            "Превышено количество попыток отправки сообщения".into(),
        )))
    }
}