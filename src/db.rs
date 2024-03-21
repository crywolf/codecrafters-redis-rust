use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::stream::{Entry, Streams};

use anyhow::Result;
use bytes::{Buf, Bytes};

#[allow(dead_code)]
pub struct DB {
    magic_str: String,      // "REDIS"
    version: String,        // "0003"
    table_size: u32,        // Size of the corresponding hash table
    table_expiry_size: u32, // Size of the corresponding expire hash table
    data: Mutex<HashMap<String, Item>>,
    streams: Streams,
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
            streams: Streams::new(),
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

        // We support only the first database
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
            streams: Streams::new(),
        };

        // Import key-value pairs from the table
        for _ in 0..table_size {
            let mut expiry_time_ms: u64 = 0;
            let mut value_type = f.get_u8();

            if value_type == 0xFD {
                // expiry time in seconds (4 bytes little endian)
                expiry_time_ms = f.get_u32_le() as u64 * 1000;
            } else if value_type == 0xFC {
                // expiry time in ms (8 bytes little endian)
                expiry_time_ms = f.get_u64_le();
            }

            if expiry_time_ms != 0 {
                value_type = f.get_u8();
            }

            if value_type != 0 {
                // support only for this length encoding type: 00 - The next 6 bits represent the length
                unimplemented!();
            }

            let key = Self::decode_string(&mut f)?;
            let value = Item::new(Self::decode_string(&mut f)?.as_str(), expiry_time_ms);

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
            0b00000000..=0b00111111 => u32::from_be_bytes([0x00, 0x00, 0x00, length]), // The next 6 bits represent the length
            0b01000000..=0b01111111 => u32::from_be_bytes([0x00, 0x00, buf.get_u8(), length]), // Read one additional byte. The combined 14 bits represent the length
            0b10000000..=0b10111111 => buf.get_u32(), // Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            0b11000000..=0b11111111 => 00, // The next object is encoded in a special format
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

            if SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_millis() as u64
                > item.expiry_ms
            {
                return None;
            }

            return Some(item);
        }
        None
    }

    pub fn xadd(&self, key: &str, entry: Entry) -> Result<String> {
        self.streams.add(key, entry)
    }

    pub fn xrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<Entry>> {
        self.streams.range(key, start, end)
    }

    pub fn xread(&self, key: &str, start: &str) -> Result<Vec<Entry>> {
        self.streams.read(key, start)
    }

    pub fn streams(&self) -> &Streams {
        &self.streams
    }
}

#[derive(Debug, Clone)]
pub struct Item {
    pub value: String,
    pub expiry_ms: u64,
}

impl Item {
    pub fn new(value: &str, expiry_ms: u64) -> Self {
        Self {
            value: value.to_owned(),
            expiry_ms,
        }
    }
}
