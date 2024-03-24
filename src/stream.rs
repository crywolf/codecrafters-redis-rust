use anyhow::{bail, Context, Ok, Result};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, sync::Mutex};
use tokio::sync::mpsc;

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
        let mut streams = self.streams.lock().expect("should be able lock the mutex");

        let stream = streams
            .entry(stream_key.to_owned())
            .or_insert(Stream::new());

        let id = stream.add(entry).context("adding entry to stream")?;

        stream.notify()?;

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

        let streams = self.streams.lock().expect("should be able lock the mutex");

        let stream = streams
            .get(stream_key)
            .context(format!("range querying stream '{stream_key}'"))?;

        let range = stream
            .entries
            .iter()
            .filter(|&e| e.raw_id.as_str() >= nstart && e.raw_id.as_str() <= nend)
            .cloned()
            .collect();

        Ok(range)
    }

    pub fn read(&self, stream_keys: &[&str], starts: &[&str]) -> Result<Vec<Entry>> {
        if stream_keys.len() != starts.len() {
            bail!("IDs count does not match keys count");
        }

        let mut response = Vec::new();

        for (i, &stream_key) in stream_keys.iter().enumerate() {
            let start = starts[i];

            let mut nstart = start;

            let s: String = format!("{start}-0");
            if !start.contains('-') {
                nstart = &s;
            }
            let e: String = format!("{}-{}", u64::MAX, u64::MAX);
            let nend = e.as_str();

            let streams = self.streams.lock().expect("should be able lock the mutex");

            let stream = streams
                .get(stream_key)
                .context(format!("range querying stream '{stream_key}'"))?;

            let mut range: Vec<_> = stream
                .entries
                .iter()
                .filter(|&e| e.raw_id.as_str() > nstart && e.raw_id.as_str() <= nend)
                .cloned()
                .map(|mut e| {
                    e.stream_key = Some(stream_key.to_string());
                    e
                })
                .collect();

            response.append(&mut range);
        }

        Ok(response)
    }

    pub fn exists(&self, stream_key: &str) -> bool {
        self.streams
            .lock()
            .expect("should be able lock the mutex")
            .contains_key(stream_key)
    }

    pub fn subscribe_to_stream(
        &self,
        connection_id: u64,
        stream_key: &str,
        tx: mpsc::Sender<()>,
    ) -> Result<()> {
        self.streams
            .lock()
            .expect("should be able lock the mutex")
            .entry(stream_key.to_owned())
            .and_modify(|e| {
                e.notifiers.insert(connection_id, Notifier::new(tx));
                println!("Subscribed to notifications for stream '{stream_key}'");
            });

        Ok(())
    }

    pub fn unsubscribe_from_stream(&self, connection_id: u64, stream_key: &str) -> Result<()> {
        self.streams
            .lock()
            .expect("should be able lock the mutex")
            .entry(stream_key.to_owned())
            .and_modify(|e| {
                e.notifiers.remove(&connection_id);
                println!("Unsubscribed from notifications for stream '{stream_key}'");
            });

        Ok(())
    }

    pub fn unsubscribe_from_all_streams(&self, connection_id: u64) -> Result<()> {
        println!("Unsubscribing connection '{connection_id}' from notifications for all streams (if any)");

        let mut streams = self.streams.lock().expect("should be able lock the mutex");

        for (stream_key, stream) in streams.iter_mut() {
            if stream.notifiers.remove(&connection_id).is_some() {
                println!("Unsubscribed  connection '{connection_id})'from notifications for stream '{stream_key}'");
            }
        }

        Ok(())
    }
}

struct Stream {
    max_time: u64,
    max_sequence_num: u64,
    entries: Vec<Entry>,
    notifiers: HashMap<u64, Notifier>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            max_time: 0,
            max_sequence_num: 0,
            entries: Vec::new(),
            notifiers: HashMap::new(),
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

    pub fn notify(&self) -> Result<()> {
        for (_, n) in self.notifiers.iter() {
            n.notify()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Notifier {
    tx: mpsc::Sender<()>,
}

impl Notifier {
    pub fn new(tx: mpsc::Sender<()>) -> Self {
        Self { tx }
    }

    pub fn notify(&self) -> Result<()> {
        let tx = self.tx.clone();
        println!("Sending notification from notifier");
        let sync_code = std::thread::spawn(move || tx.blocking_send(()));
        let _ = sync_code.join().unwrap();
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct Entry {
    stream_key: Option<String>,
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
            stream_key: None,
            raw_id,
            id,
            key_values,
        })
    }

    pub fn get_stream_key(&self) -> Option<String> {
        self.stream_key.clone()
    }

    pub fn get_raw_id(&self) -> String {
        self.raw_id.clone()
    }

    pub fn get_key_values(&self) -> &[KeyValue] {
        &self.key_values
    }

    fn get_id(&self) -> ID {
        self.id.clone()
    }
}

#[derive(Clone, Debug)]
pub struct ID {
    pub time_part: String,
    pub sequence_part: String,
}
