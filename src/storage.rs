use std::{collections::HashMap, sync::Mutex};

pub struct Storage {
    data: Mutex<HashMap<String, String>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&self, key: String, value: String) {
        self.data
            .lock()
            .expect("should be able to lock the mutex")
            .insert(key, value);
    }

    pub fn get(&self, key: &String) -> Option<String> {
        self.data
            .lock()
            .expect("should be able to lock the mutex")
            .get(key)
            .cloned()
    }
}
