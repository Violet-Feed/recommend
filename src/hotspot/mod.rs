use std::collections::{BinaryHeap, HashSet};
use std::cmp::Reverse;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::cmp::Ordering;
use anyhow::{Context, Result};
use sentinel_core::{base, flow, EntryBuilder};
use dashmap::DashMap;
use tokio::sync::{Mutex, OnceCell};
use tokio::time::{interval, Duration};
use crate::dal::redis;

#[derive(Clone, Eq, PartialEq)]
struct Hotspot {
    key: String,
    timestamp: i64,
}
impl Ord for Hotspot {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}
impl PartialOrd for Hotspot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct HotspotManager {
    set: HashSet<String>,
    heap: BinaryHeap<Reverse<Hotspot>>,
}
impl HotspotManager {
    fn new() -> Arc<Mutex<Self>> {
        let manager = Arc::new(Mutex::new(HotspotManager {
            set: HashSet::new(),
            heap: BinaryHeap::new(),
        }));
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(60));
            loop {
                ticker.tick().await;
                let mut guard = manager_clone.lock().await;
                guard.auto_remove();
            }
        });
        manager
    }

    fn insert(&mut self, hotspot: Hotspot) {
        self.set.insert(hotspot.key.clone());
        self.heap.push(Reverse(hotspot));
    }

    fn contains(&self, hotspot: &Hotspot) -> bool {
        self.set.contains(&hotspot.key)
    }

    fn auto_remove(&mut self) {
        let now = now_ts();
        let expire_ts = now - 3600;
        loop {
            let need_remove = if let Some(Reverse(hotspot)) = self.heap.peek() {
                hotspot.timestamp <= expire_ts
            } else {
                break;
            };
            if need_remove {
                if let Some(Reverse(hotspot)) = self.heap.pop() {
                    self.set.remove(&hotspot.key);
                }
            } else {
                break;
            }
        }
    }
}

static HOTSPOT_MANAGERS: OnceCell<DashMap<String, Arc<Mutex<HotspotManager>>>> = OnceCell::const_new();

async fn get_hotspot_managers() -> &'static DashMap<String, Arc<Mutex<HotspotManager>>> {
    HOTSPOT_MANAGERS.get_or_init(|| async { DashMap::new() }).await
}

fn now_ts() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

pub async fn detect_hotspot(namespace: &str, key: &str) -> Result<()> {
    let resource = format!("{}:{}", namespace, key);
    flow::append_rule(Arc::new(flow::Rule {
        id: resource.clone(),
        resource: resource.clone(),
        threshold: 2.0,
        calculate_strategy: flow::CalculateStrategy::Direct,
        control_strategy: flow::ControlStrategy::Reject,
        stat_interval_ms: 600000,
        ..Default::default()
    }));

    let entry_builder = EntryBuilder::new(resource)
        .with_traffic_type(base::TrafficType::Inbound);

    if let Ok(entry) = entry_builder.build() {
        entry.exit();
    } else {
        let manager = get_hotspot_managers().await
            .entry(namespace.to_string())
            .or_insert_with(|| {
                HotspotManager::new()
            })
            .clone();
        let mut guard = manager.lock().await;

        let hotspot = Hotspot {
            key: key.to_string(),
            timestamp: now_ts(),
        };
        if !guard.contains(&hotspot) {
            guard.insert(hotspot);
            //TODO:push by grpc streaming
            redis::set_hotspot(namespace, key).await
                .context("[detect_hotspot] redis set err.")?;
            tracing::info!("[detect_hotspot] new hotspot. namespace = {}, key = {}", namespace, key);
        }
    }
    Ok(())
}