// binance config.rs 
//
// Для релизной работы, установить wallet_balance
// на 10% меньше чем initial_amount для корректных калькуляций
//
// Ставить зазор "basic_threshold" на 1 процент меньше от профитного. 
//

use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub api_key: String,
    pub api_secret: String,
    pub wallet_balance: f64,
    pub top_tokens_count: usize,
    pub quote_currencies: Vec<&'static str>, 
    pub start_end_token: &'static str,  
    pub initial_amount: f64,            
    pub profit_threshold: f64,
    pub basic_threshold: f64,
    pub mandatory_tokens: Vec<&'static str>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            api_key: env::var("API_KEY").expect("API_KEY не установлен в .env"),
            api_secret: env::var("API_SECRET").expect("API_SECRET не установлен в .env"),
            wallet_balance: 90.0, // Рабочий баланс портфеля в USDT 
            top_tokens_count: 707, // Количество токенов которые будут учитаны для создания цепочек 
            quote_currencies: vec!["USDT", "USDC", "BNB", "BTC", "DAI", "ETH"], // Обязательные токены для парсинга. НЕ МЕНЯТЬ!  
            mandatory_tokens: vec!["ADAUSDT", "ADABTC", "ADABNB", "BTCUSDT", "BTCUSDC", "USDCUSDT", "BNBDAI"], // Обязательные токен пары для парсинга. НЕ МЕНЯТЬ! 
            start_end_token: "USDT", // Начальный и конечный токен в цепочке для логики. НЕ МЕНЯТЬ!
            initial_amount: 250.0, // Расчетный баланс просцета цепочек. НЕ РАБОЧИЙ БАЛАНС ПОРТФЕЛЯ. ЗНАЧЕНИЕ ДОЛЖНО БЫТЬ НА 50% БОЛЬШЕ ЧЕМ wallet_balance
            profit_threshold: 0.025,   // 0.02 - 2% profit , 0.0001 - 0.01% profit 
            basic_threshold: 0.015, // 0.01 - 1% зазор, 0.0001 - 0.01% зазор
        }
    }
}

pub fn load_config() -> Config {
    // Здесь вы можете добавить логику для загрузки конфигурации из файла,
    // если это необходимо. Пока что мы просто возвращаем значения по умолчанию.
    Config::default()
}
