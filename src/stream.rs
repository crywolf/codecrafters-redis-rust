use std::{collections::HashMap, sync::Mutex};

use anyhow::{bail, Context, Result};

pub struct Streams {
    streams: Mutex<HashMap<String, Stream>>,
}

impl Streams {
    pub fn new() -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
        }
    }

    pub fn add(&self, stream_key: &str, entry: Entry) -> Result<String> {
        let id = self
            .streams
            .lock()
            .expect("sould be able lick the mutex")
            .entry(stream_key.to_owned())
            .or_insert(Stream::new())
            .add(entry)
            .context("adding entry to stream")?;

        Ok(id)
    }

    pub fn exists(&self, stream_key: &str) -> bool {
        self.streams
            .lock()
            .expect("sould be able lick the mutex")
            .contains_key(stream_key)
    }
}

struct Stream {
    max_time: u64,
    max_sequence_num: u64,
    entries: Vec<Entry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            max_time: 0,
            max_sequence_num: 0,
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, entry: Entry) -> Result<String> {
        let id = entry.get_id();

        let time_ms = id.time_part.parse().context(format!(
            "parsing entry id (milisecondsTime): {}",
            id.time_part
        ))?;

        let sequence_num = if id.sequence_part.eq_ignore_ascii_case("*") {
            if time_ms == self.max_time {
                self.max_sequence_num + 1
            } else {
                0
            }
        } else {
            id.sequence_part.parse().context(format!(
                "parsing entry id (sequenceNumber): {}",
                id.sequence_part
            ))?
        };

        if time_ms == 0 && sequence_num == 0 {
            bail!("The ID specified in XADD must be greater than 0-0")
        }
        if time_ms < self.max_time {
            bail!("The ID specified in XADD is equal or smaller than the target stream top item")
        }
        if time_ms == self.max_time && sequence_num <= self.max_sequence_num {
            bail!("The ID specified in XADD is equal or smaller than the target stream top item")
        }

        self.max_time = time_ms;
        self.max_sequence_num = sequence_num;
        self.entries.push(entry);

        let id = format!("{}-{}", time_ms, sequence_num);

        Ok(id)
    }
}

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub struct Entry {
    raw_id: String,
    id: ID,
    key_values: Vec<KeyValue>,
}

impl Entry {
    pub fn new(id: String, key_values: Vec<KeyValue>) -> Result<Self> {
        let raw_id = id.clone();

        let parts: Vec<_> = id.split('-').collect();
        if parts.len() != 2 {
            bail!("The ID specified in XADD is invalid: {}", id);
        }

        let time_part = parts[0].to_string();
        let sequence_part = parts[1].to_string();

        let id = ID {
            time_part,
            sequence_part,
        };

        Ok(Self {
            raw_id,
            id,
            key_values,
        })
    }

    pub fn get_id(&self) -> ID {
        self.id.clone()
    }
}

#[derive(Clone)]
pub struct ID {
    pub time_part: String,
    pub sequence_part: String,
}
