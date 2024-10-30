// binance tokens.rs
use crate::config::Config;

use serde::{Deserialize, Serialize};
use reqwest::Client;
use anyhow::Result;
use tokio::time::Duration;
use tokio::sync::mpsc;
use hashbrown::HashSet;
use log::info;

#[derive(Debug, Deserialize)]
#[allow(dead_code, non_snake_case)]
struct TickerResponse {
    symbol: String,
    volume: String,
    quoteVolume: String,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct TokenLiquidity {
    pub symbol: String,
    pub volume_24h: f64,
}

// Изменено на 60 часов вместо 1 часа, чтобы не менять логику проекта
pub async fn update_top_tokens_hourly(
    count: usize,
    quote_currencies: Vec<&str>,
    subscription_sender: mpsc::Sender<Vec<TokenLiquidity>>,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(60 * 60 * 60));
    let mut current_tokens = get_top_tokens(count, &quote_currencies).await?;
    
    loop {
        interval.tick().await;
        let new_tokens = get_top_tokens(count, &quote_currencies).await?;
        
        let changes = detect_changes(&current_tokens, &new_tokens);
        if !changes.is_empty() {
            info!("Changes detected: {:?}", changes);
            if let Err(e) = subscription_sender.send(new_tokens.clone()).await {
                eprintln!("Failed to send updated tokens: {}", e);
            } else {
                current_tokens = new_tokens;
                info!("Top tokens updated and sent for resubscription");
            }
        } else {
            info!("No changes in top tokens");
        }
    }
}

fn detect_changes(old_tokens: &[TokenLiquidity], new_tokens: &[TokenLiquidity]) -> Vec<String> {
    let old_set: HashSet<_> = old_tokens.iter().map(|t| &t.symbol).collect();
    let new_set: HashSet<_> = new_tokens.iter().map(|t| &t.symbol).collect();
    
    let mut changes = Vec::new();
    
    for symbol in old_set.difference(&new_set) {
        changes.push(format!("Removed: {}", symbol));
    }
    
    for symbol in new_set.difference(&old_set) {
        changes.push(format!("Added: {}", symbol));
    }
    
    changes
}

pub async fn get_top_tokens(count: usize, quote_currencies: &[impl AsRef<str>]) -> Result<Vec<TokenLiquidity>> {
    let client = Client::new();
    let url = "https://api.binance.com/api/v3/ticker/24hr";

    let response: Vec<TickerResponse> = client.get(url).send().await?.json().await?;

    let mut all_tokens = Vec::new();

    for quote_currency in quote_currencies {
        let quote_currency_str = quote_currency.as_ref();
        let mut tokens: Vec<TokenLiquidity> = response
            .iter()
            .filter(|ticker| ticker.symbol.ends_with(quote_currency_str))
            .map(|ticker| TokenLiquidity {
                symbol: ticker.symbol.clone(),
                volume_24h: ticker.volume.parse().unwrap_or(0.0),
            })
            .collect();

        tokens.sort_by(|a, b| b.volume_24h.partial_cmp(&a.volume_24h).unwrap());
        all_tokens.extend(tokens);
    }

    all_tokens.sort_by(|a, b| b.volume_24h.partial_cmp(&a.volume_24h).unwrap());
    let top_tokens = all_tokens.into_iter().take(count).collect();

    Ok(top_tokens)
}

pub async fn get_mandatory_tokens(config: &Config) -> Vec<TokenLiquidity> {
    config.mandatory_tokens
        .iter()
        .map(|&symbol| TokenLiquidity {
            symbol: symbol.to_string(),
            volume_24h: 0.0, // Для обязательных токенов объем не важен
        })
        .collect()
}