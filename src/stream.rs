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

    pub fn add(&self, stream_key: &str, entry: Entry) -> String {
        let id = entry.id.clone();

        self.streams
            .lock()
            .expect("sould be able lick the mutex")
            .entry(stream_key.to_owned())
            .or_insert(Stream::new())
            .add(entry);

        id
    }

    pub fn exists(&self, stream_key: &str) -> bool {
        self.streams
            .lock()
            .expect("sould be able lick the mutex")
            .contains_key(stream_key)
    }
}

struct Stream {
    entries: Vec<Entry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, entry: Entry) {
        self.entries.push(entry);
    }
}

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub struct Entry {
    pub id: String,
    pub key_values: Vec<KeyValue>,
}
