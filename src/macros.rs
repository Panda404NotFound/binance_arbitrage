macro_rules! detailed_log_chain_invalid_macro {
    ($self:expr, $chain_result:expr) => {
        fn detailed_log_chain_invalid(&self, chain_result: &ChainResult) {
            println!("\n=== Детальный отчет по прибыльной цепочке ===");
            println!("Цепочка: {}", chain_result.chain.join(" -> "));
            println!("Итоговая прибыль: {:.2}%", chain_result.profit_percentage * 100.0);
            println!("Начальная сумма: {} {}", self.config.initial_amount, self.config.start_end_token);
            println!("Конечная сумма: {:.8} {}", chain_result.final_amount, self.config.start_end_token);
            println!("Общее время выполнения: {:?}", chain_result.duration);
            println!("Время выполнения операций: {:?}", chain_result.operation_time);
    
            let mut current_amount = self.config.initial_amount;
    
            for (i, window) in chain_result.chain.windows(2).enumerate() {
                let from_token = &window[0];
                let to_token = &window[1];
    
                if let Ok((symbol, is_inverse)) = futures::executor::block_on(self.get_symbol_and_operation(from_token, to_token)) {
                    println!("\nШаг {}: {} -> {}", i + 1, from_token, to_token);
                    println!("  Торговая пара: {}", symbol);
                    println!("  Инвертированная операция: {}", is_inverse);
    
                    if let Ok(order_book) = futures::executor::block_on(self.get_order_book(&symbol)) {
                        let token_info = futures::executor::block_on(self.token_info.read());
                        if let Some(pair_info) = token_info.iter().find(|println| println.symbol == symbol) {
                            let operation = self.determine_operation(from_token, to_token, pair_info);
                            
                            println!("  Операция: {:?}", operation);
                            println!("  Начальная сумма на этом шаге: {:.8} {}", current_amount, from_token);
    
                            let entries = match operation {
                                Operation::Multiply => &order_book.bids_updates,
                                Operation::Divide => &order_book.asks_updates,
                            };
    
                            println!("  Используемый стакан: {}", if matches!(operation, Operation::Multiply) { "Bids" } else { "Asks" });
                            
                            let mut remaining = current_amount;
                            let mut result = 0.0;
    
                            for (j, entry) in entries.iter().enumerate() {
                                let (volume, acquired) = match operation {
                                    Operation::Multiply => {
                                        let volume = remaining.min(entry.volume);
                                        (volume, volume * entry.price)
                                    },
                                    Operation::Divide => {
                                        let max_volume = remaining / entry.price;
                                        let volume = max_volume.min(entry.volume);
                                        (volume, volume * entry.price)
                                    },
                                };
    
                                result += match operation {
                                    Operation::Multiply => acquired,
                                    Operation::Divide => volume,
                                };
                                remaining -= match operation {
                                    Operation::Multiply => volume,
                                    Operation::Divide => acquired,
                                };
    
                                println!("    Уровень {}: Цена = {:.8}, Объем = {:.8}", j + 1, entry.price, entry.volume);
                                println!("      Использовано: {:.8}, Получено: {:.8}", 
                                         match operation {
                                             Operation::Multiply => volume,
                                             Operation::Divide => acquired,
                                         },
                                         match operation {
                                             Operation::Multiply => acquired,
                                             Operation::Divide => volume,
                                         }
                                );
    
                                if remaining <= 0.0 {
                                    break;
                                }
                            }
    
                            println!("  Итоговая сумма после операции: {:.8} {}", result, to_token);
                            current_amount = result;
                        }
                    }
                }
            }
    
            println!("\nИтоговый результат:");
            println!("  Начальная сумма: {} {}", self.config.initial_amount, self.config.start_end_token);
            println!("  Конечная сумма: {:.8} {}", chain_result.final_amount, self.config.start_end_token);
            println!("  Прибыль: {:.2}%", chain_result.profit_percentage * 100.0);
            println!("=====================================\n");
        }
    };
}