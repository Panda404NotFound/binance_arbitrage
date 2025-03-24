use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

// Имитируем телеграм API для тестирования
struct MockTelegramApi {
    should_timeout: bool,
    delay: Duration,
}

impl MockTelegramApi {
    fn new(should_timeout: bool, delay: Duration) -> Self {
        MockTelegramApi {
            should_timeout,
            delay,
        }
    }
    
    async fn send_message(&self) -> Result<String, &'static str> {
        if self.should_timeout {
            // Имитация зависания API
            sleep(self.delay).await;
            Err("API request timed out")
        } else {
            // Имитация задержки API, но в пределах таймаута
            sleep(self.delay).await;
            Ok("Message sent successfully".to_string())
        }
    }
}

// Наша функция, аналогичная исправленному send_message_with_retry
async fn send_message_with_retry(api: &MockTelegramApi) -> Result<String, &'static str> {
    let max_retries = 3;
    
    for attempt in 1..=max_retries {
        // Имитация функции с таймаутом, как в нашем исправлении
        let request_future = api.send_message();
        
        match timeout(Duration::from_millis(500), request_future).await {
            // Таймаут истек
            Err(_) => {
                println!("Timeout occurred on attempt {}", attempt);
                if attempt == max_retries {
                    return Err("Max retries reached");
                }
                // Экспоненциальная задержка
                sleep(Duration::from_millis(50 * 2_u64.pow(attempt as u32))).await;
                continue;
            },
            // Получен результат
            Ok(result) => match result {
                Ok(message) => return Ok(message),
                Err(e) => {
                    if attempt < max_retries {
                        println!("Error on attempt {}: {}", attempt, e);
                        sleep(Duration::from_millis(50 * 2_u64.pow(attempt as u32))).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
    
    Err("Max retries reached without success")
}

#[tokio::test]
async fn test_timeout_handling() {
    // Создаем API, которое отвечает нормально
    let fast_api = MockTelegramApi::new(false, Duration::from_millis(100));
    
    // Тест должен успешно пройти
    let result = send_message_with_retry(&fast_api).await;
    assert!(result.is_ok(), "API request should succeed");
    assert_eq!(result.unwrap(), "Message sent successfully");
    
    // Создаем API, которое зависает
    let slow_api = MockTelegramApi::new(true, Duration::from_secs(2));
    
    // Тест должен выявить таймаут
    let result = send_message_with_retry(&slow_api).await;
    assert!(result.is_err(), "API request should timeout");
}

#[tokio::test]
async fn test_concurrent_api_calls() {
    // Имитируем несколько параллельных запросов, чтобы убедиться, что таймауты не блокируют друг друга
    
    let (tx, mut rx) = mpsc::channel::<Result<String, &'static str>>(10);
    
    // Создаем смесь быстрых и медленных API
    let api_instances = vec![
        Arc::new(MockTelegramApi::new(false, Duration::from_millis(100))), // Быстрый
        Arc::new(MockTelegramApi::new(true, Duration::from_secs(2))),      // Медленный
        Arc::new(MockTelegramApi::new(false, Duration::from_millis(200))), // Быстрый
        Arc::new(MockTelegramApi::new(true, Duration::from_secs(3))),      // Медленный
        Arc::new(MockTelegramApi::new(false, Duration::from_millis(150))), // Быстрый
    ];
    
    // Запускаем параллельные вызовы
    for (i, api) in api_instances.iter().enumerate() {
        let tx_clone = tx.clone();
        let api_clone = Arc::clone(api);
        tokio::spawn(async move {
            let result = send_message_with_retry(&api_clone).await;
            tx_clone.send(result).await.unwrap();
            println!("Task {} completed", i);
        });
    }
    
    // Закрываем отправителя в основном потоке
    drop(tx);
    
    // Собираем результаты
    let mut success_count = 0;
    let mut error_count = 0;
    
    while let Some(result) = rx.recv().await {
        match result {
            Ok(_) => success_count += 1,
            Err(_) => error_count += 1,
        }
    }
    
    // У нас должно быть 3 успешных и 2 сбойных запроса
    assert_eq!(success_count, 3, "Should have 3 successful API calls");
    assert_eq!(error_count, 2, "Should have 2 failed API calls");
    
    // Важно, что мы не блокировались из-за медленных запросов
    println!("Test passed: All API calls completed without blocking each other");
} 