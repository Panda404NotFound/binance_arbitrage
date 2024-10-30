use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use std::sync::Arc;
use crate::math_graph::{MathGraph, ChainResult};
use log::info;

pub struct ProfitMonitor {
    math_graph: Arc<MathGraph>,
    sender: mpsc::Sender<ChainResult>,
}

impl ProfitMonitor {
    pub fn new(math_graph: Arc<MathGraph>, sender: mpsc::Sender<ChainResult>) -> Self {
        ProfitMonitor {
            math_graph,
            sender,
        }
    }

    pub async fn start(&self) {
        let mut interval = interval(Duration::from_millis(1000));

        loop {
            interval.tick().await;
            if let Err(e) = self.check_profitable_chains().await {
                info!("Error checking profitable chains: {:?}", e);
            }
        }
    }

    async fn check_profitable_chains(&self) -> Result<(), Box<dyn std::error::Error>> {
        for chain_result in self.math_graph.profitable_chains.iter().map(|r| r.value().clone()) {
            self.sender.send(chain_result).await?;
        }

        for invalid_chain_result in self.math_graph.invalid_chains.iter().map(|r| r.value().clone()) {
            self.sender.send(invalid_chain_result).await?;
        }

        Ok(())
    }
}