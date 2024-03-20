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
        let id = entry.raw_id.clone();

        self.streams
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

    pub fn add(&mut self, entry: Entry) -> Result<()> {
        let id = entry.get_id();

        if id.time_ms == 0 && id.sequence_num == 0 {
            bail!("The ID specified in XADD must be greater than 0-0")
        }
        if id.time_ms < self.max_time {
            bail!("The ID specified in XADD is equal or smaller than the target stream top item")
        }
        if id.time_ms == self.max_time && id.sequence_num <= self.max_sequence_num {
            bail!("The ID specified in XADD is equal or smaller than the target stream top item")
        }

        self.max_time = id.time_ms;
        self.max_sequence_num = id.sequence_num;
        self.entries.push(entry);

        Ok(())
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

        let id = ID {
            time_ms: parts[0]
                .parse()
                .context(format!("parsing entry id (milisecondsTime): {}", parts[0]))?,
            sequence_num: parts[1]
                .parse()
                .context(format!("parsing entry id (sequenceNumber): {}", parts[1]))?,
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
    pub time_ms: u64,
    pub sequence_num: u64,
}
