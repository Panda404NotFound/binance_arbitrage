use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, timeout};

// Имитация компонентов системы
#[derive(Debug, Clone)]
struct ErrorStatus {
    errors: Arc<RwLock<Vec<String>>>,
}

impl ErrorStatus {
    fn new() -> Self {
        ErrorStatus {
            errors: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    async fn add_error(&self, error: String) {
        let mut errors = self.errors.write().await;
        errors.push(error);
    }
    
    async fn get_errors(&self) -> Vec<String> {
        let errors = self.errors.read().await;
        errors.clone()
    }
    
    async fn clear_errors(&self) {
        let mut errors = self.errors.write().await;
        errors.clear();
    }
}

// Имитация телеграм-бота
struct MockTelegramBot {
    error_status: Arc<ErrorStatus>,
    is_running: Arc<RwLock<bool>>,
    shutdown_signal: Arc<RwLock<bool>>,
    message_queue: Arc<RwLock<Vec<String>>>,
}

impl MockTelegramBot {
    fn new(error_status: Arc<ErrorStatus>) -> Self {
        MockTelegramBot {
            error_status,
            is_running: Arc::new(RwLock::new(false)),
            shutdown_signal: Arc::new(RwLock::new(false)),
            message_queue: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    async fn send_message(&self, message: String) {
        // Имитация отправки сообщения через Telegram API
        let mut queue = self.message_queue.write().await;
        queue.push(message);
    }
    
    async fn handle_errors(&self) -> Result<(), &'static str> {
        // Получаем список ошибок без удержания блокировки надолго
        let errors = self.error_status.get_errors().await;
        
        for error in errors {
            // Отправляем сообщение о каждой ошибке
            self.send_message(format!("Ошибка: {}", error)).await;
        }
        
        // Очищаем список ошибок после обработки
        self.error_status.clear_errors().await;
        
        Ok(())
    }
    
    async fn run(&self, notify_started: mpsc::Sender<()>) -> mpsc::Receiver<()> {
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        
        // Устанавливаем флаг запуска
        {
            let mut is_running = self.is_running.write().await;
            *is_running = true;
        }
        
        // Уведомляем, что бот запущен
        let _ = notify_started.send(()).await;
        
        let shutdown_signal = Arc::clone(&self.shutdown_signal);
        let is_running = Arc::clone(&self.is_running);
        let error_status = Arc::clone(&self.error_status);
        
        // Основной поток работы бота
        tokio::spawn(async move {
            println!("Bot started");
            
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            
            while !*shutdown_signal.read().await {
                interval.tick().await;
                
                // Периодически проверяем ошибки с использованием улучшенного метода
                let bot = MockTelegramBot::new(Arc::clone(&error_status));
                let _ = bot.handle_errors().await;
            }
            
            // Устанавливаем флаг завершения
            {
                let mut running = is_running.write().await;
                *running = false;
            }
            
            let _ = shutdown_complete_tx.send(()).await;
            println!("Bot shutdown complete");
        });
        
        shutdown_complete_rx
    }
    
    async fn shutdown(&self) {
        let mut signal = self.shutdown_signal.write().await;
        *signal = true;
    }
    
    async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }
}

// Имитация системы мониторинга
struct MockMonitor {
    error_status: Arc<ErrorStatus>,
    is_running: Arc<RwLock<bool>>,
    shutdown_signal: Arc<RwLock<bool>>,
}

impl MockMonitor {
    fn new(error_status: Arc<ErrorStatus>) -> Self {
        MockMonitor {
            error_status,
            is_running: Arc::new(RwLock::new(false)),
            shutdown_signal: Arc::new(RwLock::new(false)),
        }
    }
    
    async fn monitor_system(&self) {
        // Имитация мониторинга системы
        if rand::random::<f32>() < 0.3 {
            self.error_status.add_error(format!("Системная ошибка {}", rand::random::<u32>())).await;
        }
    }
    
    async fn run(&self, notify_started: mpsc::Sender<()>) -> mpsc::Receiver<()> {
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        
        // Устанавливаем флаг запуска
        {
            let mut is_running = self.is_running.write().await;
            *is_running = true;
        }
        
        // Уведомляем, что монитор запущен
        let _ = notify_started.send(()).await;
        
        let shutdown_signal = Arc::clone(&self.shutdown_signal);
        let is_running = Arc::clone(&self.is_running);
        let error_status = Arc::clone(&self.error_status);
        
        // Основной поток работы монитора
        tokio::spawn(async move {
            println!("Monitor started");
            
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            let monitor = MockMonitor::new(error_status);
            
            while !*shutdown_signal.read().await {
                interval.tick().await;
                
                // Периодически проверяем систему
                monitor.monitor_system().await;
            }
            
            // Устанавливаем флаг завершения
            {
                let mut running = is_running.write().await;
                *running = false;
            }
            
            let _ = shutdown_complete_tx.send(()).await;
            println!("Monitor shutdown complete");
        });
        
        shutdown_complete_rx
    }
    
    async fn shutdown(&self) {
        let mut signal = self.shutdown_signal.write().await;
        *signal = true;
    }
    
    async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }
}

// Интеграционный тест
#[tokio::test]
async fn test_system_integration() {
    // Инициализация общего состояния ошибок
    let error_status = Arc::new(ErrorStatus::new());
    
    // Создаем компоненты системы
    let telegram_bot = MockTelegramBot::new(Arc::clone(&error_status));
    let monitor = MockMonitor::new(Arc::clone(&error_status));
    
    // Каналы для синхронизации запуска
    let (bot_notify_tx, mut bot_notify_rx) = mpsc::channel(1);
    let (monitor_notify_tx, mut monitor_notify_rx) = mpsc::channel(1);
    
    // Запускаем компоненты
    let mut bot_shutdown_rx = telegram_bot.run(bot_notify_tx).await;
    let mut monitor_shutdown_rx = monitor.run(monitor_notify_tx).await;
    
    // Ждем, пока оба компонента запустятся
    let _ = bot_notify_rx.recv().await;
    let _ = monitor_notify_rx.recv().await;
    
    // Основная проверка - запускаем нагрузочное тестирование
    for i in 0..10 {
        println!("Iteration {}", i);
        
        // Добавляем тестовые ошибки
        error_status.add_error(format!("Test error {}", i)).await;
        
        // Даем компонентам время на обработку
        sleep(Duration::from_millis(20)).await;
        
        // Проверяем, что оба компонента все еще работают
        assert!(telegram_bot.is_running().await, "Bot should still be running");
        assert!(monitor.is_running().await, "Monitor should still be running");
    }
    
    // Корректно завершаем компоненты
    telegram_bot.shutdown().await;
    monitor.shutdown().await;
    
    // Ожидаем завершения с таймаутом
    match timeout(Duration::from_secs(5), async {
        let _ = bot_shutdown_rx.recv().await;
        let _ = monitor_shutdown_rx.recv().await;
    }).await {
        Ok(_) => {
            // Проверяем, что компоненты действительно остановились
            assert!(!telegram_bot.is_running().await, "Bot should be stopped after shutdown");
            assert!(!monitor.is_running().await, "Monitor should be stopped after shutdown");
            println!("Test passed: System integration successful");
        },
        Err(_) => {
            panic!("Timeout waiting for components to shutdown");
        }
    }
} 