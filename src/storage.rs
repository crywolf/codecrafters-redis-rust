use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::config::Config;

pub struct Storage {
    data: Mutex<HashMap<String, Item>>,
    config: Arc<Config>,
}

impl Storage {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            config,
        }
    }

    pub fn set(&self, key: String, item: Item) {
        self.data
            .lock()
            .expect("should be able to lock the mutex")
            .insert(key, item);
    }

    pub fn get(&self, key: &String) -> Option<Item> {
        if let Some(item) = self
            .data
            .lock()
            .expect("should be able to lock the mutex")
            .get(key)
            .cloned()
        {
            if item.expiry_ms == 0 {
                return Some(item);
            }

            let expiry = Duration::from_millis(item.expiry_ms);
            if item.changed.elapsed() > expiry {
                return None;
            }

            return Some(item);
        }
        None
    }

    pub fn get_info(&self, arg: &str) -> Option<String> {
        let mut info = String::from("# Replication");

        if self.is_replica() {
            info.push_str("\nrole:slave");
        } else {
            info.push_str("\nrole:master");
            info.push_str("\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
            info.push_str("\nmaster_repl_offset:0");
        }

        if arg == "replication" {
            Some(format!("${}\r\n{}\r\n", info.len(), info))
        } else {
            None
        }
    }

    fn is_replica(&self) -> bool {
        self.config.is_replica()
    }
}

#[derive(Debug, Clone)]
pub struct Item {
    pub value: String,
    pub expiry_ms: u64,
    changed: Instant,
}

impl Item {
    pub fn new(value: String, expiry_ms: u64) -> Self {
        let changed = Instant::now();
        Self {
            value,
            expiry_ms,
            changed,
        }
    }
}
