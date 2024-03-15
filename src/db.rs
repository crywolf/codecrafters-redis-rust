use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes};

#[allow(dead_code)]
pub struct DB {
    magic_str: String,      // "REDIS"
    version: String,        // "0003"
    table_size: u32,        // Size of the corresponding hash table
    table_expiry_size: u32, // Size of the corresponding expire hash table
    data: Mutex<HashMap<String, Item>>,
}

impl DB {
    pub fn from<P: AsRef<Path>>(filepath: Option<P>) -> std::io::Result<DB> {
        // https://rdb.fnordig.de/file_format.html

        let db = Self {
            magic_str: "REDIS".to_string(),
            version: "0003".to_string(),
            table_size: 0,
            table_expiry_size: 0,
            data: Mutex::new(HashMap::new()),
        };

        if filepath.is_none() {
            return Ok(db);
        }

        let file_data = fs::read(filepath.expect("filepath is set"));

        // Unfortunately Codecrafters tests needs to pass even if the file does not exist
        if file_data.is_err() {
            eprintln!("Error: {}", file_data.expect_err("error is set here"));
            return Ok(db);
        }

        let mut f = Bytes::from(file_data.expect("file is correctly read"));

        let magic_str =
            String::from_utf8(f.split_to(5).to_vec()).map_err(Self::invalid_data_err)?;
        let version = String::from_utf8(f.split_to(4).to_vec()).map_err(Self::invalid_data_err)?;
        for (i, &v) in f.iter().enumerate() {
            // 0xFB indicates a resizedb field
            if v == 0xFB {
                let _ = f.split_to(i);
                break;
            }
        }
        let _ = f.get_u8();

        // size of the corresponding hash table
        let length = f.get_u8();
        let table_size = Self::decode_length(&mut f, length);

        // size of the corresponding expire hash table
        let length = f.get_u8();
        let table_expiry_size = Self::decode_length(&mut f, length);

        let db = Self {
            magic_str,
            version,
            table_size,
            table_expiry_size,
            data: Mutex::new(HashMap::new()),
        };

        for _ in 0..table_size {
            let value_type = f.get_u8();
            if value_type != 0 {
                // support only for this length encoding type: 00 - The next 6 bits represent the length
                unimplemented!();
            }

            let key = Self::decode_string(&mut f)?;
            let value = Item::new(Self::decode_string(&mut f)?.as_str(), 0);

            db.data
                .lock()
                .expect("should be able to lock the mutex")
                .insert(key, value);
        }

        Ok(db)
    }

    fn decode_length(buf: &mut Bytes, length: u8) -> u32 {
        // https://rdb.fnordig.de/file_format.html#length-encoding
        match length {
            0b00000000..=0b00111111 => u32::from_be_bytes([0x00, 0x00, 0x00, length]),
            0b01000000..=0b01111111 => u32::from_be_bytes([0x00, 0x00, buf.get_u8(), length]),
            0b10000000..=0b10111111 => buf.get_u32(),
            0b11000000..=0b11111111 => 00,
        }
    }

    fn decode_string(buf: &mut Bytes) -> std::io::Result<String> {
        let length = buf.get_u8();
        let key_length = Self::decode_length(buf, length) as usize;
        let i = buf.split_to(key_length).to_vec();
        String::from_utf8(i).map_err(Self::invalid_data_err)
    }

    fn invalid_data_err(err: std::string::FromUtf8Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, err)
    }

    pub fn keys(&self, _pattern: &str) -> Vec<String> {
        self.data
            .lock()
            .expect("should be able to lock the mutex")
            .keys()
            .map(|k| k.to_owned())
            .collect()
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
