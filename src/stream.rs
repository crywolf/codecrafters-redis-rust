use anyhow::{bail, Context, Result};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, sync::Mutex};

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

    pub fn range(&self, stream_key: &str, start: &str, end: &str) -> Result<Vec<Entry>> {
        let mut nstart = start;
        let mut nend = end;

        let s: String = format!("{start}-0");
        let e: String = format!("{end}-{}", u64::MAX);
        if !start.contains('-') {
            nstart = &s;
        }
        if !end.contains('-') {
            nend = &e;
        }

        let e: String = format!("{}-{}", u64::MAX, u64::MAX);
        if start == "-" {
            nstart = "0-0";
        }
        if end == "+" {
            nend = &e;
        }

        let streams = self.streams.lock().expect("sould be able lick the mutex");

        let stream = streams.get(stream_key).context("range querying stream")?;

        let range = stream
            .entries
            .iter()
            .filter(|&e| e.raw_id.as_str() >= nstart && e.raw_id.as_str() <= nend)
            .cloned()
            .collect();

        Ok(range)
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

        let time_ms = if id.time_part.eq_ignore_ascii_case("*") {
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64
        } else {
            id.time_part.parse().context(format!(
                "parsing entry id (milisecondsTime): {}",
                id.time_part
            ))?
        };

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

#[derive(Clone, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct Entry {
    raw_id: String,
    id: ID,
    key_values: Vec<KeyValue>,
}

impl Entry {
    pub fn new(id: String, key_values: Vec<KeyValue>) -> Result<Self> {
        let raw_id = id.clone();
        let time_part: String;
        let sequence_part: String;

        if !id.eq_ignore_ascii_case("*") {
            let parts: Vec<_> = id.split('-').collect();
            if parts.len() != 2 {
                bail!("The ID specified in XADD is invalid: {}", id);
            }
            time_part = parts[0].to_string();
            sequence_part = parts[1].to_string();
        } else {
            time_part = String::from("*");
            sequence_part = String::from("*");
        }

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

    fn get_id(&self) -> ID {
        self.id.clone()
    }

    pub fn get_raw_id(&self) -> String {
        self.raw_id.clone()
    }

    pub fn get_key_values(&self) -> &[KeyValue] {
        &self.key_values
    }
}

#[derive(Clone, Debug)]
pub struct ID {
    pub time_part: String,
    pub sequence_part: String,
}
