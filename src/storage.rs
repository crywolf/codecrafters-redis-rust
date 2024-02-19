use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

pub struct Storage {
    data: Mutex<HashMap<String, Item>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
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
