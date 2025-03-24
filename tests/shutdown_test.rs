use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock};
use tokio::time::{sleep, timeout};

// Имитация TelegramBot для тестирования корректного завершения
struct MockTelegramBot {
    is_running: Arc<RwLock<bool>>,
    shutdown_signal: Arc<RwLock<bool>>,
}

impl MockTelegramBot {
    fn new() -> Self {
        MockTelegramBot {
            is_running: Arc::new(RwLock::new(false)),
            shutdown_signal: Arc::new(RwLock::new(false)),
        }
    }
    
    async fn shutdown(&self) {
        println!("Sending shutdown signal");
        let mut signal = self.shutdown_signal.write().await;
        *signal = true;
    }
    
    async fn run(self: Arc<Self>) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        
        // Устанавливаем флаг, что бот работает
        {
            let mut is_running = self.is_running.write().await;
            *is_running = true;
        }
        
        let shutdown_signal_clone = Arc::clone(&self.shutdown_signal);
        let is_running_clone = Arc::clone(&self.is_running);
        
        // Стартуем цикл "работы" бота
        tokio::spawn(async move {
            println!("Bot started, entering main loop");
            
            // Имитация основного цикла как в нашем TelegramBot
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            
            loop {
                interval.tick().await;
                
                // Проверяем сигнал завершения
                let shutdown_requested = *shutdown_signal_clone.read().await;
                if shutdown_requested {
                    println!("Shutdown signal received, exiting loop");
                    break;
                }
                
                // Имитация работы
                println!("Bot is working...");
            }
            
            // Установка флага завершения работы
            {
                let mut is_running = is_running_clone.write().await;
                *is_running = false;
            }
            
            // Сигнализируем о завершении работы
            println!("Bot shutdown complete");
            let _ = tx.send(());
        });
        
        rx
    }
    
    async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }
}

#[tokio::test]
async fn test_graceful_shutdown() {
    // Создаем и запускаем бота
    let bot = Arc::new(MockTelegramBot::new());
    let shutdown_receiver = Arc::clone(&bot).run().await;
    
    // Даем боту время на запуск
    sleep(Duration::from_millis(200)).await;
    
    // Проверяем, что бот работает
    assert!(bot.is_running().await, "Bot should be running after start");
    
    // Отправляем сигнал завершения
    bot.shutdown().await;
    
    // Ожидаем завершения с таймаутом
    match timeout(Duration::from_secs(5), shutdown_receiver).await {
        Ok(_) => {
            // Проверяем, что бот действительно остановился
            assert!(!bot.is_running().await, "Bot should be stopped after shutdown");
            println!("Test passed: Bot shutdown gracefully");
        },
        Err(_) => {
            panic!("Timeout waiting for bot to shutdown");
        }
    }
}

#[tokio::test]
async fn test_multiple_shutdown_signals() {
    // Создаем и запускаем бота
    let bot = Arc::new(MockTelegramBot::new());
    let shutdown_receiver = Arc::clone(&bot).run().await;
    
    // Даем боту время на запуск
    sleep(Duration::from_millis(200)).await;
    
    // Отправляем несколько сигналов завершения параллельно, что не должно вызвать проблем
    let bot_clone1 = Arc::clone(&bot);
    let bot_clone2 = Arc::clone(&bot);
    let bot_clone3 = Arc::clone(&bot);
    
    let shutdown_tasks = vec![
        tokio::spawn(async move { bot_clone1.shutdown().await }),
        tokio::spawn(async move { bot_clone2.shutdown().await }),
        tokio::spawn(async move { bot_clone3.shutdown().await }),
    ];
    
    // Ждем, пока все сигналы будут отправлены
    for task in shutdown_tasks {
        let _ = task.await;
    }
    
    // Ожидаем завершения с таймаутом
    match timeout(Duration::from_secs(5), shutdown_receiver).await {
        Ok(_) => {
            // Проверяем, что бот действительно остановился
            assert!(!bot.is_running().await, "Bot should be stopped after shutdown");
            println!("Test passed: Bot handled multiple shutdown signals correctly");
        },
        Err(_) => {
            panic!("Timeout waiting for bot to shutdown");
        }
    }
} 