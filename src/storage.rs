use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;

use crate::config::Config;

pub struct Storage {
    data: Mutex<HashMap<String, Item>>,
    config: Arc<Config>,
    /// Number of bytes of commands processed by this replica
    processed_bytes: Mutex<usize>,
    /// Number of bytes of write commands processed by this replica
    processed_write_commands_bytes: Mutex<usize>,
    // Number of bytes of write commands sent by master
    //    sent_write_command_bytes: Mutex<usize>,
}

impl Storage {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            config,
            processed_bytes: Mutex::new(0),
            processed_write_commands_bytes: Mutex::new(0),
            //  sent_write_command_bytes: Mutex::new(0),
        }
    }

    pub fn set(&self, key: &str, item: Item) {
        self.data
            .lock()
            .expect("should be able to lock the mutex")
            .insert(key.to_owned(), item);
    }

    pub fn get(&self, key: &str) -> Option<Item> {
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
            if let Some((master_replid, master_repl_offset)) = self.get_repl_id_and_offset() {
                info.push_str("\nmaster_replid:");
                info.push_str(&master_replid);
                info.push_str("\nmaster_repl_offset:");
                info.push_str(&master_repl_offset);
            }
        }

        if arg == "replication" {
            Some(format!("${}\r\n{}\r\n", info.len(), info))
        } else {
            None
        }
    }

    pub fn get_repl_id_and_offset(&self) -> Option<(String, String)> {
        if self.is_master() {
            Some((
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
                "0".to_owned(),
            ))
        } else {
            None
        }
    }

    pub fn get_rbd_file(&self) -> Bytes {
        let rbd_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        Bytes::from(Self::hex_to_bytes(rbd_hex))
    }

    pub fn is_replica(&self) -> bool {
        self.config.is_replica()
    }

    pub fn is_master(&self) -> bool {
        !self.config.is_replica()
    }

    pub fn add_processed_bytes(&self, count: usize) {
        *self
            .processed_bytes
            .lock()
            .expect("shoul be able to lock the mutex") += count;
    }

    pub fn get_processed_bytes(&self) -> usize {
        *self
            .processed_bytes
            .lock()
            .expect("shoul be able to lock the mutex")
    }

    pub fn add_processed_write_command_bytes(&self, count: usize) {
        *self
            .processed_write_commands_bytes
            .lock()
            .expect("shoul be able to lock the mutex") += count;
    }

    pub fn get_processed_write_command_bytes(&self) -> usize {
        *self
            .processed_write_commands_bytes
            .lock()
            .expect("shoul be able to lock the mutex")
    }

    pub fn reset_processed_write_command_bytes(&self) {
        *self
            .processed_write_commands_bytes
            .lock()
            .expect("shoul be able to lock the mutex") = 0;
    }

    /*    pub fn add_sent_write_command_bytes(&self, count: usize) {
           *self
               .sent_write_command_bytes
               .lock()
               .expect("shoul be able to lock the mutex") += count;
       }

       pub fn get_sent_write_command_bytes(&self) -> usize {
           *self
               .sent_write_command_bytes
               .lock()
               .expect("shoul be able to lock the mutex")
       }

       pub fn reset_sent_write_command_bytes(&self) {
           *self
               .sent_write_command_bytes
               .lock()
               .expect("shoul be able to lock the mutex") = 0;
       }
    */
    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap_or_default())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Item {
    pub value: String,
    pub expiry_ms: u64,
    changed: Instant,
}

impl Item {
    pub fn new(value: &str, expiry_ms: u64) -> Self {
        let changed = Instant::now();
        Self {
            value: value.to_owned(),
            expiry_ms,
            changed,
        }
    }
}
