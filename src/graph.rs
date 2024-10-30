use std::sync::Arc;
use std::pin::Pin;
use std::time::Instant;
use std::time::Duration;
use std::future::Future;
use std::collections::VecDeque;
use log::info;

use tokio::sync::RwLock; 
use tokio::sync::{mpsc, Semaphore};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use dashmap::DashMap;
use hashbrown::{HashMap, HashSet};
use crossbeam::queue::SegQueue;

use itertools::Itertools;
use petgraph::graph::{Graph, NodeIndex};

use crate::data::LockFreeTokenDataStore;
use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPair {
    pub symbol: String,
    pub base_token: String,
    pub quote_token: String,
    pub is_inverse: bool,
    pub pairs: Vec<String>,
    pub profit_percentage: f64,
}

#[derive(Debug, Clone)]
pub struct TradingGraph {
    graph: Arc<RwLock<Graph<String, ()>>>,
    token_to_index: Arc<DashMap<String, NodeIndex>>,
}

impl TradingGraph {
    fn new() -> Self {
        TradingGraph {
            graph: Arc::new(RwLock::new(Graph::new())),
            token_to_index: Arc::new(DashMap::new()),
        }
    }

    async fn add_token_pair(&self, base_token: &str, quote_token: &str) {
        info!("TradingGraph::add_token_pair: Добавление пары: base={}, quote={}", base_token, quote_token);

        let mut graph = self.graph.write().await;
        
        let base_index = self.get_or_insert_node(&mut graph, base_token).await;
        let quote_index = self.get_or_insert_node(&mut graph, quote_token).await;

        // Добавляем ребра в обоих направлениях
        graph.add_edge(base_index, quote_index, ());
        graph.add_edge(quote_index, base_index, ());

        info!("TradingGraph::add_token_pair: Пара добавлена. Текущее количество узлов: {}, рёбер: {}", 
            graph.node_count(), graph.edge_count());
    }

    async fn get_or_insert_node(&self, graph: &mut Graph<String, ()>, token: &str) -> NodeIndex {
        if let Some(index) = self.token_to_index.get(token) {
            *index
        } else {
            let index = graph.add_node(token.to_string());
            self.token_to_index.insert(token.to_string(), index);
            index
        }
    }

    async fn get_connected_tokens(&self, token: &str) -> Vec<String> {
        let graph = self.graph.read().await;
        if let Some(node_index) = self.token_to_index.get(token) {
            graph.neighbors_directed(*node_index, petgraph::Direction::Outgoing)
                .map(|idx| graph[idx].clone())
                .collect()
        } else {
            info!("TradingGraph::get_connected_tokens: Токен не найден в графе");
            vec![]
        }
    }
}

struct ChainFinder<'a> {
    graph: &'a TradingGraph,
    start_token: String,
    max_depth: usize,
    all_chains: Vec<Vec<String>>,
}

impl<'a> ChainFinder<'a> {
    fn new(graph: &'a TradingGraph, start_token: &str, max_depth: usize) -> Self {
        ChainFinder {
            graph,
            start_token: start_token.to_string(),
            max_depth,
            all_chains: Vec::new(),
        }
    }

    async fn find_all_chains_from_node(&mut self, start_node: &str) {
        self.dfs(vec![start_node.to_string()]).await;
    }

    fn dfs<'b>(&'b mut self, current_chain: Vec<String>) -> Pin<Box<dyn Future<Output = ()> + 'b>> {
        Box::pin(async move {
            let current_token = current_chain.last().unwrap();

            if current_chain.len() > 3 && current_token == &self.start_token {
                self.all_chains.push(current_chain.clone());
            }

            if current_chain.len() >= self.max_depth {
                return;
            }

            let connected_tokens = self.graph.get_connected_tokens(current_token).await;
            for next_token in connected_tokens {
                if self.can_add_token(&next_token, &current_chain) {
                    let mut new_chain = current_chain.clone();
                    new_chain.push(next_token);
                    self.dfs(new_chain).await;
                }
            }
        })
    }

    fn can_add_token(&self, token: &str, chain: &[String]) -> bool {
        if token == &self.start_token {
            chain.len() >= 3
        } else if chain.len() >= 2 && token == &chain[chain.len() - 2] {
            false
        } else {
            !chain.contains(&token.to_string())
        }
    }

    fn get_results(&self) -> Vec<Vec<String>> {
        self.all_chains.clone()
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct LocalTokenData {
    pub symbol: String,
    pub base_token: String,
    pub quote_token: String,
    pub is_inverse: bool,
}

struct GraphLocalData {
    pub token_data: Vec<LocalTokenData>,
}

impl GraphLocalData {
    fn new(token_data_store: &LockFreeTokenDataStore, config: &Config) -> Self {
        let mut token_data = Vec::new();
        for item in token_data_store.data.iter() {
            let (symbol, _) = item.pair();
            if let Ok((base_token, quote_token, is_inverse)) = Self::split_symbol(symbol, config) {
                token_data.push(LocalTokenData {
                    symbol: symbol.to_string(),
                    base_token,
                    quote_token,
                    is_inverse,
                });
            }
        }
        GraphLocalData { token_data }
    }

    fn split_symbol(symbol: &str, config: &Config) -> Result<(String, String, bool), anyhow::Error> {
        for quote_currency in &config.quote_currencies {
            if symbol.ends_with(quote_currency) {
                let base_currency = symbol[..symbol.len() - quote_currency.len()].to_string();
                return Ok((base_currency, quote_currency.to_string(), false));
            }
            if symbol.starts_with(quote_currency) {
                let base_currency = symbol[quote_currency.len()..].to_string();
                return Ok((base_currency, quote_currency.to_string(), true));
            }
        }
        Err(anyhow::anyhow!("Invalid symbol format"))
    }

    fn update(&mut self, token_data_store: &LockFreeTokenDataStore, config: &Config) {
        self.token_data.clear();
        for item in token_data_store.data.iter() {
            let (symbol, _) = item.pair();
            if let Ok((base_token, quote_token, is_inverse)) = Self::split_symbol(symbol, config) {
                self.token_data.push(LocalTokenData {
                    symbol: symbol.to_string(),
                    base_token,
                    quote_token,
                    is_inverse,
                });
            }
        }
    }
}

pub struct ChainCategories {
    pub unique_chains_4: Vec<Vec<String>>,
    pub unique_chains_5: Vec<Vec<String>>,
    pub isomorphic_chains_4: DashMap<String, Vec<Vec<String>>>,
    pub isomorphic_chains_5: DashMap<String, Vec<Vec<String>>>,
}

impl ChainCategories {
    pub fn new() -> Self {
        ChainCategories {
            unique_chains_4: Vec::new(),
            unique_chains_5: Vec::new(),
            isomorphic_chains_4: DashMap::new(),
            isomorphic_chains_5: DashMap::new(),
        }
    }
}

#[allow(dead_code)]
pub struct ArbitrageEngine {
    config: Arc<Config>,
    local_data: Arc<RwLock<GraphLocalData>>,
    graph: Arc<TradingGraph>,
    chains: Arc<SegQueue<Vec<String>>>,
    chains_by_length: Arc<DashMap<usize, Vec<Vec<String>>>>,
    isomorphic_chains: Arc<DashMap<usize, Vec<Vec<String>>>>,
    pub chain_categories: Arc<RwLock<ChainCategories>>,
    unused_pairs_sender: mpsc::Sender<Vec<String>>,
    task_group_size: usize,
    semaphore: Arc<Semaphore>,
    task_queue: Arc<RwLock<VecDeque<(String, String)>>>,
    worker_count: usize,
}

impl ArbitrageEngine {
    pub fn new(
        config: Arc<Config>,
        token_data_store: Arc<LockFreeTokenDataStore>,
        unused_pairs_sender: mpsc::Sender<Vec<String>>,
        task_group_size: usize,
        max_concurrent_tasks: usize,
        worker_count: usize,
    ) -> Self {
        let local_data = Arc::new(RwLock::new(GraphLocalData::new(&token_data_store, &config)));

        ArbitrageEngine {
            config,
            local_data,
            graph: Arc::new(TradingGraph::new()),
            chains: Arc::new(SegQueue::new()),
            chains_by_length: Arc::new(DashMap::new()),
            isomorphic_chains: Arc::new(DashMap::new()),
            chain_categories: Arc::new(RwLock::new(ChainCategories::new())),
            unused_pairs_sender,
            task_group_size,
            semaphore: Arc::new(Semaphore::new(max_concurrent_tasks)),
            task_queue: Arc::new(RwLock::new(VecDeque::new())),
            worker_count,
        }
    }

    pub async fn run(&self, token_data_store: Arc<LockFreeTokenDataStore>) -> Result<(), anyhow::Error> {
        tokio::time::sleep(Duration::from_secs(75)).await;
        info!("Starting arbitrage engine");
    
        let start = Instant::now();

        // Инициализация LocalTokenData
        {
            let mut local_data = self.local_data.write().await;
            local_data.update(&token_data_store, &self.config);
        }
        info!("Local token data initialized");
    
        // Построение графа
        let graph_build_start = Instant::now();

        self.build_graph_incrementally().await?;
        info!("Graph building took: {:?}", graph_build_start.elapsed());

        // Поиск арбитражных цепочек
        let chain_find_start = Instant::now();

        self.find_all_arbitrage_chains().await?;
        info!("Finding arbitrage chains took: {:?}", chain_find_start.elapsed());
    
        info!("Total run time: {:?}", start.elapsed());

        let unused_pairs = self.find_unused_pairs().await;
        if let Err(e) = self.unused_pairs_sender.send(unused_pairs).await {
            info!("Failed to send unused pairs: {:?}", e);
        }

        info!("Graph.rs завершил работу");

        Ok(())
    }

    async fn build_graph_incrementally(&self) -> Result<(), anyhow::Error> {
        let start_time = Instant::now();
        info!("Starting incremental graph building");

        // Логируем начальное состояние
        {
            let graph = self.graph.graph.read().await;
            info!("Initial graph state: Nodes: {}, Edges: {}", 
            graph.node_count(), graph.edge_count());
        }

        // Заполняем очередь задач
        let task_count;
        {
            let local_data = self.local_data.read().await;
            let mut queue = self.task_queue.write().await;
            for token_data in &local_data.token_data {
                queue.push_back((token_data.base_token.clone(), token_data.quote_token.clone()));
            }
            task_count = queue.len();
        }
        info!("Task queue filled with {} tasks", task_count);

        // Запускаем рабочие потоки
        let mut handles = vec![];
        for i in 0..self.worker_count {
            let task_queue = self.task_queue.clone();
            let graph = self.graph.clone();
            let semaphore = self.semaphore.clone();

            let handle = tokio::spawn(async move {
                let mut processed_count = 0;
                let worker_start_time = Instant::now();
                loop {
                    let task = {
                        let mut queue = task_queue.write().await;
                        queue.pop_front()
                    };

                    match task {
                        Some((base_token, quote_token)) => {
                            let task_start_time = Instant::now();
                            let _permit = semaphore.acquire().await.unwrap();
                            info!("Worker {} processing task: {} - {}", i, base_token, quote_token);

                            graph.add_token_pair(&base_token, &quote_token).await;
                            info!("Worker {} successfully added pair: {} - {} (Time: {:?})", 
                                     i, base_token, quote_token, task_start_time.elapsed());
                            processed_count += 1;
                        }
                        None => {
                            info!("Worker {} finished - no more tasks. Processed {} tasks in {:?}", 
                                     i, processed_count, worker_start_time.elapsed());
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // Ожидаем завершения всех рабочих потоков
        for (i, handle) in handles.into_iter().enumerate() {
            if let Err(e) = handle.await {
                info!("Error in worker thread {}: {:?}", i, e);
            }
        }

        // Логируем финальное состояние
        {
            let graph = self.graph.graph.read().await;
            info!("Final graph state: Nodes: {}, Edges: {}", 
            graph.node_count(), graph.edge_count());
        }

        info!("Incremental graph building completed in {:?}", start_time.elapsed());
        Ok(())
    }

    async fn find_all_arbitrage_chains(&self) -> Result<(), anyhow::Error> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        info!("find_all_arbitrage_chains: Начало асинхронного поиска арбитражных цепочек");
    
        let start_token = self.config.start_end_token;
        let max_depth = 5;
        let chunk_size = self.task_group_size;
    
        let node_count = {
            let graph = self.graph.graph.read().await;
            graph.node_count()
        };
    
        if node_count == 0 {
            info!("Ошибка: граф пуст. Невозможно выполнить поиск арбитражных цепочек.");
            return Ok(());
        }
    
        let results = Arc::new(DashMap::new());
        let mut handles = Vec::new();
    
        for chunk_start in (0..node_count).step_by(chunk_size) {
            let chunk_end = (chunk_start + chunk_size).min(node_count);
            let graph_clone = self.graph.clone();
            let start_token_clone = start_token;
            let semaphore = self.semaphore.clone();
            let results_clone = results.clone();
    
            let handle = tokio::task::spawn_blocking(move || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(async {
                    let _permit = semaphore.acquire().await.unwrap();
                    let graph = graph_clone.graph.read().await;
                    let mut chains = Vec::new();
                    for node_index in chunk_start..chunk_end {
                        if let Some(node) = graph.node_weight(NodeIndex::new(node_index)) {
                            let mut finder = ChainFinder::new(&*graph_clone, &start_token_clone, max_depth);
                            finder.find_all_chains_from_node(node).await;
                            chains.extend(finder.get_results());
                        }
                    }
                    if !chains.is_empty() {
                        results_clone.insert(chunk_start, chains);
                    } else {
                        info!("Не найдено цепочек в чанке {}-{}", chunk_start, chunk_end);
                    }
                });
            });
            handles.push(handle);
        }
    
        info!("Ожидание завершения всех рабочих потоков...");
        for handle in handles {
            if let Err(e) = handle.await {
                info!("Ошибка в рабочем потоке: {:?}", e);
            }
        }
    
        info!("Все рабочие потоки завершили работу. Обработка результатов...");
        self.process_results(results).await;
        info!("Обработка результатов завершена");
    
        Ok(())
    }

    async fn find_unused_pairs(&self) -> Vec<String> {
        let mut all_pairs = HashSet::new();
        let mut used_pairs = HashSet::new();

        // Собираем все пары из локальных данных
        {
            let local_data = self.local_data.read().await;
            for token_data in &local_data.token_data {
                all_pairs.insert(token_data.symbol.clone());
            }
        }

        // Проверяем использование пар в цепочках
        let categories = self.chain_categories.read().await;
        
        let mut check_chain = |chain: &Vec<String>| {
            for window in chain.windows(2) {
                if let [from, to] = window {
                    used_pairs.insert(format!("{}{}", from, to));
                    used_pairs.insert(format!("{}{}", to, from));
                }
            }
        };

        for chain in &categories.unique_chains_4 { check_chain(chain); }
        for chain in &categories.unique_chains_5 { check_chain(chain); }
        for chains in categories.isomorphic_chains_4.iter() {
            for chain in chains.value() {
                check_chain(chain);
            }
        }
        for chains in categories.isomorphic_chains_5.iter() {
            for chain in chains.value() {
                check_chain(chain);
            }
        }

        // Находим неиспользуемые пары
        let unused_pairs: Vec<String> = all_pairs.difference(&used_pairs).cloned().collect();

        info!("Found {} unused pairs", unused_pairs.len());
        info!("List of unused pairs:");
        for (index, pair) in unused_pairs.iter().enumerate() {
            info!("  {}. {}", index + 1, pair);
        }
        unused_pairs
    }

    async fn process_results(&self, results: Arc<DashMap<usize, Vec<Vec<String>>>>) {
        let all_chains: Vec<Vec<String>> = results.iter().flat_map(|r| r.value().clone()).collect();
        info!("process_results: Найдено цепочек: {}", all_chains.len());
    
        let unique_chains = self.remove_duplicate_chains(all_chains);
        info!("process_results: Уникальных цепочек: {}", unique_chains.len());
    
        self.categorize_chains(unique_chains).await;
        info!("process_results: Категоризация цепочек завершена");
    
        #[cfg(debug_assertions)]
        self.log_chain_statistics().await;
    }

    fn remove_duplicate_chains(&self, chains: Vec<Vec<String>>) -> Vec<Vec<String>> {
        info!("remove_duplicate_chains: Начало удаления дубликатов. Входных цепочек: {}", chains.len());
        chains
            .into_iter()
            .unique()
            .collect()
    }

    async fn categorize_chains(&self, chains: Vec<Vec<String>>) {
        info!("categorize_chains: Начало категоризации цепочек. Входных цепочек: {}", chains.len());
        let mut categories = self.chain_categories.write().await;
    
        // Временное хранилище для группировки изоморфных цепочек
        let mut temp_isomorphic_4: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        let mut temp_isomorphic_5: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    
        for chain in chains {
            if !self.is_valid_chain(&chain) {
                continue;
            }
    
            let len = chain.len();
            let isomorphic_key = self.get_isomorphic_key(&chain);
    
            match len {
                4 => {
                    temp_isomorphic_4.entry(isomorphic_key).or_default().push(chain);
                },
                5 => {
                    temp_isomorphic_5.entry(isomorphic_key).or_default().push(chain);
                },
                _ => info!("Игнорирование цепочки неверной длины: {}", self.get_chain_key(&chain)),
            }
        }
    
        // Распределение цепочек по категориям
        for (key, chains) in temp_isomorphic_4 {
            if chains.len() == 1 {
                categories.unique_chains_4.push(chains[0].clone());
            } else {
                categories.isomorphic_chains_4.insert(key, chains);
            }
        }
    
        for (key, chains) in temp_isomorphic_5 {
            if chains.len() == 1 {
                categories.unique_chains_5.push(chains[0].clone());
            } else {
                categories.isomorphic_chains_5.insert(key, chains);
            }
        }
    
        info!("categorize_chains: Завершено. Уникальных цепочек длины 4: {}, длины 5: {}",
                 categories.unique_chains_4.len(), categories.unique_chains_5.len());
        info!("Изоморфных групп длины 4: {}, длины 5: {}",
                 categories.isomorphic_chains_4.len(), categories.isomorphic_chains_5.len());
    }

    fn get_isomorphic_key(&self, chain: &[String]) -> String {
        let mut token_map = HashMap::new();
        let mut next_id = 0;
        let mut key = Vec::new();
    
        for token in chain {
            let id = token_map.entry(token).or_insert_with(|| {
                next_id += 1;
                next_id
            });
            key.push(id.to_string());
        }
    
        key.join("-")
    }

    fn is_valid_chain(&self, chain: &[String]) -> bool {
        let start_end_token = &self.config.start_end_token;
        chain.first().map(|s| s == start_end_token).unwrap_or(false) &&
        chain.last().map(|s| s == start_end_token).unwrap_or(false)
    }

    fn get_chain_key(&self, chain: &[String]) -> String {
        chain.join("-")
    }

    #[cfg(debug_assertions)]
    async fn log_chain_statistics(&self) {
        let categories = self.chain_categories.read().await;
        info!("Arbitrage Chain Statistics:");
        info!("Уникальные цепочки длины 4: {}", categories.unique_chains_4.len());
        info!("Уникальные цепочки длины 5: {}", categories.unique_chains_5.len());
    
        let total_isomorphic_4 = categories.isomorphic_chains_4.iter().map(|e| e.value().len()).sum::<usize>();
        let total_isomorphic_5 = categories.isomorphic_chains_5.iter().map(|e| e.value().len()).sum::<usize>();
        info!("Общее количество изоморфных цепочек длины 4: {}", total_isomorphic_4);
        info!("Общее количество изоморфных цепочек длины 5: {}", total_isomorphic_5);
    
        drop(categories);
        self.log_detailed_chains().await;
    }

    #[cfg(debug_assertions)]
    async fn log_detailed_chains(&self) {
        let categories = self.chain_categories.read().await;
        info!("Detailed list of found chains:");
    
        info!("\nУникальные цепочки длины 4:");
        for (i, chain) in categories.unique_chains_4.iter().take(101).enumerate() {
            info!("Chain {}: {}", i + 1, chain.join(" -> "));
        }
    
        info!("\nУникальные цепочки длины 5:");
        for (i, chain) in categories.unique_chains_5.iter().take(101).enumerate() {
            info!("Chain {}: {}", i + 1, chain.join(" -> "));
        }
    
        info!("\nИзоморфные цепочки длины 4:");
        let mut count = 0;
        for item in categories.isomorphic_chains_4.iter() {
            let key = item.key();
            let chains = item.value();
            info!("Изоморфная группа {}:", key);
            for (i, chain) in chains.iter().take(101).enumerate() {
                info!("  Chain {}: {}", i + 1, chain.join(" -> "));
                count += 1;
                if count >= 101 {
                    break;
                }
            }
            if count >= 101 {
                break;
            }
        }
    
        info!("\nИзоморфные цепочки длины 5:");
        let mut count = 0;
        for item in categories.isomorphic_chains_5.iter() {
            let key = item.key();
            let chains = item.value();
            info!("Изоморфная группа {}:", key);
            for (i, chain) in chains.iter().take(101).enumerate() {
                info!("  Chain {}: {}", i + 1, chain.join(" -> "));
                count += 1;
                if count >= 101 {
                    break;
                }
            }
            if count >= 101 {
                break;
            }
        }
    }
}