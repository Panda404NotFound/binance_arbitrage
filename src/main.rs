// binance main.rs

#[macro_use]
mod macros;
mod tokens;
mod websocket;
mod data;
mod config;
mod graph;
mod math_graph;
mod bot_monitor;
mod profit_monitor;
mod logger;
mod brain_bot;
mod tokenfilter;
mod telegram;
mod error_status;

use crate::brain_bot::Bot;
use crate::logger::Logger;
use crate::config::load_config;
use crate::brain_bot::BotAction;
use crate::telegram::TelegramBot;
use crate::math_graph::MathGraph;
use crate::graph::ArbitrageEngine;
use crate::bot_monitor::BotMonitor;
use crate::error_status::ErrorStatus;
use crate::websocket::WebSocketManager;
use crate::data::LockFreeTokenDataStore;
use crate::profit_monitor::ProfitMonitor;

use std::env;
use env_logger;
use std::process;
use std::sync::Arc;
use std::net::IpAddr;
use std::error::Error;
use std::path::PathBuf;
use std::process::{Command, exit};
use std::time::{Duration, Instant};

use tokio::task;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio::sync::RwLock;
use tokio::sync::broadcast;

use nix::unistd::Pid;
use nix::sys::signal::{kill, Signal};

use log::info;
use rand::random;
use dotenv::dotenv;
use anyhow::Result;
use prometheus::{Registry, IntCounter};
use surge_ping::{Client, Config, IcmpPacket, 
    PingIdentifier, SurgeError, PingSequence};

#[allow(unreachable_code, unused_variables)]
#[tokio::main]
async fn main() -> Result<()> {
    
    env_logger::init();

    // Загрузка переменных окружения из .env файла
    dotenv().ok();

    //
    // Логика рестарта
    //
    let start_time = Instant::now();
    let restart_interval = Duration::from_secs(12 * 60 * 60);

    // Получаем путь к исполняемому файлу
    let executable_path = env::current_exe().expect("Failed to get executable path");
    info!("Executable path: {:?}", executable_path);
    //
    // Логика рестарта
    //

    let config = load_config();
    let config_arc = Arc::new(config.clone());

    // Инициализация метрик не реализована 
    // let registry = Registry::new();
    
    // Инициализация структур данных
    let token_data_store = Arc::new(LockFreeTokenDataStore::new());

    // Инициализация структуры ошибок
    let error_status = Arc::new(RwLock::new(ErrorStatus::default()));

    // Обработка и логирование информации о топовых токенах
    let initial_tokens = tokens::get_top_tokens(config.top_tokens_count, &config.quote_currencies).await?;

    info!("Top {} liquid tokens on Binance (X/{:?} pairs):", config.top_tokens_count, config.quote_currencies);
    for (i, token) in initial_tokens.iter().enumerate() {
        info!("{}. {} - 24h Volume: ${:.2}", i + 1, token.symbol, token.volume_24h);
    }

    /*
    // Оптимизируем использование Tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(num_cpus::get())
    .enable_all()
    .build()?;

    let _guard = runtime.enter();

    */
    
    // Интегрируем систему метрик
    let registry = Registry::new();
    let updates_processed = IntCounter::new("updates_processed", "Number of updates processed")?;
    registry.register(Box::new(updates_processed.clone()))?;

    // Создание каналов
    let (unused_pairs_sender, unused_pairs_receiver) = mpsc::channel(100);
    let (token_update_sender, _token_update_receiver) = mpsc::channel(100);
    let (subscription_sender, subscription_receiver) = mpsc::channel(100);

    // Инициализация WebSocket менеджера
    let mut websocket_manager = WebSocketManager::new(
        token_data_store.clone(),
        subscription_receiver,
        unused_pairs_receiver,
        token_update_sender.clone(),
        config_arc.clone()
    );

    // Запуск основных задач
    let backend_handle = tokio::spawn(async move {
        let update_task = task::spawn(tokens::update_top_tokens_hourly(
            config.top_tokens_count, 
            config.quote_currencies, 
            subscription_sender, 
        ));

        let websocket_manager_task = task::spawn(async move {
            websocket_manager.run(initial_tokens).await
        });

        let _ = tokio::try_join!(update_task, websocket_manager_task);
    });

    // Периодический вывод метрик
    // Бесполезный функционал
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            info!("Updates processed: {}", updates_processed.get());
        }
    });

    let arbitrage_engine = Arc::new(ArbitrageEngine::new(
        config_arc.clone(),
        token_data_store.clone(), // Добавляем token_data_store
        unused_pairs_sender.clone(),
        25, // task_group_size
        num_cpus::get() / 2, // max_concurrent_tasks
        num_cpus::get() // worker_count
    ));

    // Запуск ArbitrageEngine
    let arbitrage_engine_clone = Arc::clone(&arbitrage_engine);
    let token_data_store_clone = Arc::clone(&token_data_store);
    tokio::spawn(async move {
        if let Err(e) = arbitrage_engine_clone.run(token_data_store_clone).await {
            eprintln!("Error running arbitrage engine: {:?}", e);
        }
    });

    // Инициализация MathGraph
    let math_graph = Arc::new(MathGraph::new(
        config_arc.clone(),
        arbitrage_engine.clone(),
        token_data_store.clone()
    ));

    // Запуск MathGraph
    let math_graph_clone = Arc::clone(&math_graph);
    tokio::spawn(async move {
        if let Err(e) = math_graph_clone.run().await {
            eprintln!("Error running MathGraph: {:?}", e);
        }
    });

    // Не используемые и не рабочие сигналы отправки 
    let (shutdown_sender, shutdown_receiver) = mpsc::channel::<()>(100);

    // Создаем канал с буфером на 100 сообщений для brain_bot
    let (bot_action_sender, _) = broadcast::channel::<BotAction>(100);

    // Инициализируем Bot
    let brain_bot = Arc::new(RwLock::new(Bot::new(
        Arc::clone(&config_arc),
        Arc::clone(&math_graph),
        Arc::clone(&token_data_store),
        bot_action_sender.clone(),
    )));

    // Создание brain_bot_handle
    let brain_bot_clone = Arc::clone(&brain_bot);
    let bot_handle = tokio::spawn(async move {
        if let Err(e) = brain_bot_clone.write().await.run().await {
            eprintln!("Brain Bot error: {}", e);
            if let Some(source) = e.source() {
                eprintln!("Error source: {}", source);
            }
            // Разделить ошибки на критические и не критические
            // чтобы обрабатывать их без выхода из проекта
            // и в случае необходимости продолжить или перезапустить
            if e.to_string().contains("Error 52") || e.to_string().contains("Error 53") || e.to_string().contains("Error 54") {
                println!("Критическая ошибка. Остановка проекта");
                simulate_ctrl_c();
            }
            // Если ошибка 55, то запускать функцию
            // перезагрузки проекта "restart_process"
        } else {
            println!("Brain Bot finished successfully");
        }
    });
    
    // Инициализация TelegramBot
    let telegram_bot = Arc::new(TelegramBot::new(
        &std::env::var("TELEGRAM_TOKEN").expect("TELEGRAM_TOKEN не установлена"), // Токен Telegram бота
        std::env::var("CHAT_ID").expect("CHAT_ID не установлена").parse::<i64>().expect("Неверный формат CHAT_ID"), // Chat ID, который может изменяться
        Arc::clone(&error_status),
        bot_action_sender.clone(),
    ));

    // Запуск TelegramBot
    let telegram_bot_clone = Arc::clone(&telegram_bot);
    let telegram_task = tokio::spawn(async move {
        telegram_bot_clone.run().await;
    });

    // Добавим telegram_bot в вектор задач, которые нужно корректно завершить
    let shutdown_components = Arc::new(RwLock::new(vec![telegram_bot]));
    let shutdown_components_clone = Arc::clone(&shutdown_components);

    // Создаем каналы для передачи данных между Bot, BotMonitor и Logger
    let (chain_sender, chain_receiver) = mpsc::channel(100);
    let (bot_action_sender, _) = broadcast::channel::<BotAction>(100);
    let (bot_action_mpsc_sender, bot_action_mpsc_receiver) = mpsc::channel(100);

    // Инициализируем BotMonitor
    let mut bot_monitor = BotMonitor::new(bot_action_sender.clone(), bot_action_mpsc_sender);

    // Запускаем BotMonitor в отдельной задаче
    tokio::spawn(async move {
        bot_monitor.start().await;
    });

    // Инициализируем ProfitMonitor
    let profit_monitor = ProfitMonitor::new(Arc::clone(&math_graph), chain_sender);

    // Запускаем ProfitMonitor в отдельной задаче
    tokio::spawn(async move {
        profit_monitor.start().await;
    });

    // Инициализируем Logger
    let mut logger = Logger::new(chain_receiver, bot_action_mpsc_receiver, Arc::clone(&config_arc));

    // Запускаем Logger в отдельной задаче
    tokio::spawn(async move {
        if let Err(e) = logger.run().await {
            eprintln!("Error running logger: {:?}", e);
        }
    });

    // Создаем и запускаем задачу для обработки сигнала завершения
    let (shutdown_sender, _) = mpsc::channel(100);
    let shutdown_sender_clone = shutdown_sender.clone();

    // Функция проверки интернета 
    let executable_path = Arc::new(executable_path);
    let executable_path_clone = Arc::clone(&executable_path);
    let shutdown_sender_clone = shutdown_sender.clone();
    let error_status_clone = Arc::clone(&error_status);
    let error_status_clone2 = Arc::clone(&error_status);

    // Функция проверки времени выполнения
    let mut shutdown_handle = tokio::spawn(async move {
        handle_shutdown_signal(shutdown_sender_clone, error_status.clone(), shutdown_components_clone).await;
    });

    let shutdown_sender_clone = shutdown_sender.clone();
    tokio::spawn(async move {
        let _ = check_internet_connection(
            executable_path_clone, 
            shutdown_sender_clone,
            error_status_clone,
        ).await;
    });    

    // После этого цикла код достижим
    loop {
        tokio::select! {
            _ = &mut shutdown_handle => {
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                if start_time.elapsed() >= restart_interval {
                    println!("Перезапуск приложения после 23 часов работы...");
                    restart_process(&executable_path).await;
                }
            }
        }
    }

    async fn check_internet_connection(
        executable_path: Arc<PathBuf>,
        shutdown_sender: mpsc::Sender<()>,
        error_status: Arc<RwLock<ErrorStatus>>,
    ) -> Result<(), Box<dyn Error>> {
        println!("Starting internet connection check task");
        let mut consecutive_failures = 0;
        let address: IpAddr = "8.8.8.8".parse().expect("Invalid IP address");
    
        // Создаем клиента
        let client = Client::new(&Config::default())?;
        let identifier = PingIdentifier(random());

        let mut pinger = client.pinger(address, identifier).await;
        let mut seq = 0;
    
        loop {
            seq += 1;
    
            // Отправляем ICMP-запрос
            match pinger.ping(PingSequence(seq), &[]).await {
                Ok((IcmpPacket::V4(_packet), rtt)) => {
                    let latency_ms = rtt.as_millis();
                    if latency_ms > 50 {
                        println!("High latency detected: {}ms", latency_ms);
                        consecutive_failures += 1;
                    } else {
                        info!("Connection is good. Latency: {}ms", latency_ms);
                        consecutive_failures = 0;
                    }
                }
                Ok((IcmpPacket::V6(_), _)) => {
                    // Не ожидаем получить ICMPv6 пакет при пинге IPv4 адреса
                    println!("Received unexpected ICMPv6 packet.");
                    consecutive_failures += 1;
                }
                Err(SurgeError::Timeout { .. }) => {
                    //println!("Ping request timed out.");
                    consecutive_failures += 1;
                }
                Err(e) => {
                    println!("Failed to ping: {}", e);
                    consecutive_failures += 1;
                }
            }
    
            // Проверяем счетчик ошибок
            if consecutive_failures >= 3 {
                println!("Internet connection issues detected. Initiating restart sequence.");
                let mut error_status = error_status.write().await;
                error_status.internet_connection_failures = Some("Сбой подключения к интернету".to_string());
                sleep(Duration::from_secs(3)).await;
                simulate_ctrl_c();
                sleep(Duration::from_secs(3)).await;
                restart_process(&executable_path).await;
            }
    
            // Ждем 2.5 секунды перед следующей проверкой
            sleep(Duration::from_millis(1000)).await;
        }
    }

    async fn handle_shutdown_signal(
        shutdown_sender: mpsc::Sender<()>, 
        error_status: Arc<RwLock<ErrorStatus>>,
        shutdown_components: Arc<RwLock<Vec<Arc<TelegramBot>>>>,
    ) {
        signal::ctrl_c().await.expect("Запуск остановки Ctrl+C");
        {
            let mut status = error_status.write().await;
            status.function_startup_state = Some("Запущен сигнала завершения. Критическая остановка".to_string());
        }
        println!("Received Ctrl+C, preparing to shut down...");
        
        // Корректно завершаем компоненты
        let components = shutdown_components.read().await;
        for component in components.iter() {
            component.shutdown().await;
        }
        println!("Components shutdown signals sent");
        
        if let Err(e) = shutdown_sender.send(()).await {
            eprintln!("Запущен сигнал остановки: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(4)).await;
        println!("Shutting down now.");
        process::exit(0);
    }

    fn simulate_ctrl_c() {
        let pid = Pid::this();
        if let Err(e) = kill(pid, Signal::SIGINT) {
            eprintln!("Не удалось отправить сигнал SIGINT: {}", e);
        }
    }

    async fn restart_process(executable_path: &PathBuf) {
        println!("Restarting process...");

        // Запускаем новый процесс
        let mut command = Command::new(executable_path);
        command.args(env::args().skip(1));
        
        match command.spawn() {
            Ok(_) => {
                println!("New process started successfully");
                tokio::time::sleep(Duration::from_millis(1500)).await;
                exit(0);
            }
            Err(e) => {
                eprintln!("Failed to start new process: {}", e);
                exit(1);
            }
        }
    }

    println!("Main loop exited. Waiting for background tasks to complete...");

    backend_handle.await?;
    
    Ok(())
}
