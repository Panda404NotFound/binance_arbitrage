# BINANCE MULTI-CURRENCY ARBITRAGE BOT

<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=FF0000&height=200&section=header&text=SYSTEM%20OVERRIDE&fontSize=60&fontColor=FFFFFF&animation=fadeIn&fontAlignY=38&desc=SECURE%20ARBITRAGE%20PROTOCOL&descAlignY=55&descAlign=50&strokeWidth=1" width="100%"/>
</div>

## [ SYSTEM OVERVIEW ]

### 🔥 PROJECT DESCRIPTION

Introducing a sophisticated multi-currency arbitrage bot engineered for executing complex arbitrage operations across multiple currency pairs. The system implements trading chains consisting of 4-5 pairs, following the pattern: **USDT → BTC → SOL → (XPR optional) → USDT**, where both the initial and final token remain USDT to safeguard portfolio stability during operation.

I'm sharing this project because its profitability fell short of expectations during release testing. The bot is designed to maximize profit while minimizing risks, targeting a profit threshold of 2.5% per arbitrage opportunity.

I'm now pivoting to develop a more advanced project focused on automated trading in spot and futures markets using machine learning and neural networks.

### ⚡ OPERATIONAL PRINCIPLES

- Processes trading chains of 4-5 pairs
- Core workflow: **USDT → BTC → SOL → (XPR*) → USDT**
- *XPR inclusion optional for 5-link chains
- USDT anchors both ends of each chain for portfolio protection

## 🔥 TECHNICAL ARCHITECTURE

- Core logic housed in `brain_bot.rs`
- Foundational configurations in `config.rs`
- Chain construction via recursive DFS algorithm
- Asynchronous parallel processing of all chains
- Non-blocking data access implementation
- Stack overflow protection mechanisms

## 🔥 DEPLOYMENT INSTRUCTIONS

### ⚡ PREREQUISITES

- Linux distribution
- Docker
- Git

### ⚡ INSTALLATION STEPS

1. Create `.env` file in the project directory. Add necessary configurations from the `.env_example.txt` file.

2. Clone the repository:
```bash
git clone [repository-url]
```

3. Configure permissions and execute script:
```bash
cd binance_cex && chmod +x key.sh && ./key.sh
```

4. Build and launch Docker container:
```bash
sudo docker build -t binance . && sudo docker run -it -d --name binance_cont --restart unless-stopped binance
```

## 🔥 CONTAINER MANAGEMENT

### ⚡ ACCESS CONTAINER SHELL:
```bash
sudo docker exec -it binance_cont sh
```

Available commands inside container:
```bash
ps aux  # View running processes
tail -f /var/log/app.log  # View application logs
df -h  # Check available disk space
```

### ⚡ VIEW CONTAINER LOGS:
```bash
sudo docker logs binance_cont
```

For real-time log monitoring:
```bash
sudo docker logs -f binance_cont
```

### ⚡ ATTACH TO CONTAINER:
```bash
sudo docker attach binance_cont
```

Exit container without stopping using: `Ctrl+P, Ctrl+Q`

## 🔥 TESTING PROCEDURES

Follow these steps to conduct testing:

### ⚡ INSTALL CARGO

Install the Cargo package manager with:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

> **Note**: Cargo installs from the official Rust website. Follow terminal instructions.

### ⚡ NAVIGATE TO PROJECT DIRECTORY

Move to the project's root directory.

### ⚡ EXECUTE TEST RUN

Run with debug output enabled:

```bash
RUST_LOG=debug cargo run --release
```

## 🔥 SYSTEM REQUIREMENTS

- CPU: 4+ cores
- RAM: 8+ GB
- SSD: 150 GB
- OS: Linux

## 🔥 PERFORMANCE METRICS

- Processing capacity: ~30,000 chains in 250 ms
- WebSocket message handling: 2-8 µs (average 4-5 µs)
- Orderbook processing: 317 ns - 10.914 µs (average 1 µs)
- WebSocket response time: up to 300 ms (as low as 5 ms in Japan region)

## 🔥 BOT CAPABILITIES

### ⚡ MONITORING & ALERTS
- Hourly operational status via Telegram (disabled in `math_graph.rs`, available in logs)
- Critical notifications through Telegram
- Transaction logging, profitable chain tracking, and "lifetime" recording to debug files
- Chain selection and transaction result notifications via Telegram

### ⚡ FAULT TOLERANCE
- Automatic restart on internet connection issues
- Critical halts on anomalous errors
- Multi-component chain accounting logic
- Profitability recalculation at each step

### ⚡ TRADE OPTIMIZATION
- Nanosecond-level orderbook data acquisition without delays
- Dual-layer chain validation
- Alternative chain selection for:
  - Profit maximization
  - Loss minimization
- Comprehensive tracking of all profitable chains with optimal selection

### ⚡ RISK MANAGEMENT
- Market slippage accounting
- Analysis of "false" market maker volumes

## 🔥 RECOMMENDATIONS

- Prefer Japan region (Azure Cloud) for optimal performance
- Conduct thorough testing before release deployment
- Use VPN for connection issue mitigation
- Supported regions: Asia, Europe, America

## 🔥 DISABLING TELEGRAM NOTIFICATIONS

To disable Telegram signal functionality, comment out these lines in `main.rs`:

```rust
mod telegram;
use crate::telegram::TelegramBot;

// TelegramBot initialization
let telegram_bot = Arc::new(TelegramBot::new(
    &std::env::var("TELEGRAM_TOKEN").expect("TELEGRAM_TOKEN missing"),
    std::env::var("CHAT_ID").expect("CHAT_ID missing").parse::<i64>().expect("Invalid CHAT_ID format"),
    Arc::clone(&error_status),
    bot_action_sender.clone(),
));

// TelegramBot execution
let telegram_bot_clone = Arc::clone(&telegram_bot);
tokio::spawn(async move {
    telegram_bot_clone.run().await;
});
```

## 🔥 CONTACT

<div align="center">
  
  [![GitHub](https://img.shields.io/badge/GitHub-Panda404NotFound-ff0066?style=for-the-badge&logo=github)](https://github.com/Panda404NotFound)
  [![Telegram](https://img.shields.io/badge/Telegram-@code__0110-ff0066?style=for-the-badge&logo=telegram)](https://t.me/code_0110)
  [![Email](https://img.shields.io/badge/Email-synthstudioteam@gmail.com-ff0066?style=for-the-badge&logo=gmail)](mailto:synthstudioteam@gmail.com)
  
</div>

<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=FF0000&height=120&section=footer&text=END%20OF%20TRANSMISSION&fontSize=30&fontColor=FFFFFF&animation=fadeIn&fontAlignY=70" width="100%"/>
</div>
