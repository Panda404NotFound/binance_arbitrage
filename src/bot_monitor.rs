use tokio::sync::{broadcast, mpsc};
use crate::brain_bot::BotAction;

pub struct BotMonitor {
    bot_action_receiver: broadcast::Receiver<BotAction>,
    sender: mpsc::Sender<BotAction>,
}

impl BotMonitor {
    pub fn new(
        bot_action_sender: broadcast::Sender<BotAction>,
        sender: mpsc::Sender<BotAction>,
    ) -> Self {
        BotMonitor {
            bot_action_receiver: bot_action_sender.subscribe(),
            sender,
        }
    }

    pub async fn start(&mut self) {
        loop {
            match self.bot_action_receiver.recv().await {
                Ok(action) => {
                    if let Err(e) = self.sender.send(action.clone()).await {
                        eprintln!("Failed to send action to logger: {:?}", e);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    eprintln!("Missed {} messages in BotMonitor.", count);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    eprintln!("BotMonitor channel was closed.");
                    break;
                }
            }
        }
    }
}