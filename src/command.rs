use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::Item;
use crate::resp::{BulkString, RESPType};
use crate::storage::Storage;
use crate::stream::{Entry, KeyValue};
use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Info(String),
    Set(String, String, String),
    Get(String),
    Replconf(Vec<String>),
    Psync(String, String),
    Wait {
        args: (String, String),
        replicas_count: usize,
    },
    Config(String, String),
    Keys(String),
    Type(String),
    Xadd(Vec<String>),
    Xrange(String, String, String),
}

impl Command {
    pub fn parse(resp: RESPType) -> Result<Self> {
        let RESPType::Array(resp) = resp else {
            bail!("invalid command, must be an array: {:?}", resp);
        };

        let mut parts = resp.into_iter();

        let Some(RESPType::Bulk(cmd)) = parts.next() else {
            bail!("invalid command name, command must be a bulk string");
        };

        let command = match cmd.data.to_uppercase().as_str() {
            "PING" => Self::Ping,
            "ECHO" => {
                let arg = Self::get_arg(&mut parts)?;
                Self::Echo(arg.data)
            }
            "INFO" => {
                let arg = Self::get_arg(&mut parts)?;
                Self::Info(arg.data)
            }
            "SET" => {
                let key = Self::get_arg(&mut parts)?;
                let val = Self::get_arg(&mut parts)?;
                let mut exp_key = "".to_string();
                let expiry = if let Some(_exp) = parts.find(|arg| {
                    if let RESPType::Bulk(key) = arg {
                        exp_key = key.data.to_uppercase();
                        &key.data.to_uppercase() == "PX" || &key.data.to_lowercase() == "PXAT"
                    } else {
                        false
                    }
                }) {
                    if let Some(RESPType::Bulk(mut exp_val)) = parts.next() {
                        if exp_key == "PX" {
                            // Time was provided as relative, change it to absolute time
                            let exp_at = SystemTime::now().duration_since(UNIX_EPOCH)?
                                + Duration::from_millis(exp_val.data.parse()?);
                            exp_val.data = exp_at.as_millis().to_string();
                        }
                        exp_val
                    } else {
                        bail!("SET command is missing PX or PXAT value (miliseconds)")
                    }
                } else {
                    BulkString {
                        len: 1,
                        data: "0".to_string(),
                    }
                };
                Self::Set(key.data, val.data, expiry.data)
            }
            "GET" => {
                let arg = Self::get_arg(&mut parts)?;
                Self::Get(arg.data)
            }
            "REPLCONF" => {
                if parts.len() < 2 {
                    bail!("not enough arguments");
                }
                let args = parts
                    .filter_map(|s| {
                        if let RESPType::Bulk(arg) = s {
                            Some(arg.data)
                        } else {
                            None
                        }
                    })
                    .collect();
                Self::Replconf(args)
            }
            "PSYNC" => {
                let arg1 = Self::get_arg(&mut parts)?;
                let arg2 = Self::get_arg(&mut parts)?;
                Self::Psync(arg1.data, arg2.data)
            }
            "WAIT" => {
                let arg1 = Self::get_arg(&mut parts)?;
                let arg2 = Self::get_arg(&mut parts)?;
                Self::Wait {
                    args: (arg1.data, arg2.data),
                    replicas_count: 0,
                }
            }
            "CONFIG" => {
                let arg1 = Self::get_arg(&mut parts)?;
                let arg2 = Self::get_arg(&mut parts)?;
                Self::Config(arg1.data.to_uppercase(), arg2.data)
            }
            "KEYS" => {
                let arg = Self::get_arg(&mut parts)?;
                Self::Keys(arg.data)
            }
            "TYPE" => {
                let arg = Self::get_arg(&mut parts)?;
                Self::Type(arg.data)
            }
            "XADD" => {
                if parts.len() < 4 {
                    // xadd stream_key 1526919030474-0 temperature 36
                    bail!("not enough arguments");
                }
                let args = parts
                    .filter_map(|s| {
                        if let RESPType::Bulk(arg) = s {
                            Some(arg.data)
                        } else {
                            None
                        }
                    })
                    .collect();
                Self::Xadd(args)
            }
            "XRANGE" => {
                // xrange some_key 1526985054069 1526985054079
                let stream_key = Self::get_arg(&mut parts)?.data;
                let start = Self::get_arg(&mut parts)?.data;
                let end = Self::get_arg(&mut parts)?.data;
                Self::Xrange(stream_key, start, end)
            }
            _ => unimplemented!(),
        };

        Ok(command)
    }

    pub fn is_write(&self) -> bool {
        matches!(self, Self::Set { .. } | Self::Xadd { .. })
    }

    pub fn response(&self, storage: Arc<Storage>) -> Result<Bytes> {
        let response = match self {
            Self::Ping => Bytes::from("+PONG\r\n"),
            Self::Echo(arg) => Bytes::from(format!("+{arg}\r\n")),
            Self::Info(arg) => Bytes::from(storage.get_info(arg).unwrap_or("$-1\r\n".to_owned())),
            Self::Set(key, value, expiry) => {
                let expiry_ms = expiry.parse().unwrap_or_default();
                let item = Item::new(value, expiry_ms);
                storage.db.set(key, item);
                Bytes::from("+OK\r\n")
            }
            Self::Get(key) => {
                let val = match storage.db.get(key) {
                    Some(item) => format!("${}\r\n{}\r\n", item.value.len(), item.value),
                    None => String::from("$-1\r\n"),
                };
                Bytes::from(val)
            }
            Self::Replconf(args) => {
                if args.len() == 2 && args[0].to_uppercase() == "GETACK" {
                    let mut processed_bytes = "0".to_owned();
                    if args[1] == "*" {
                        processed_bytes = storage.get_processed_bytes().to_string();
                    }
                    if args[1] == "WRITE" {
                        processed_bytes = storage.get_processed_write_command_bytes().to_string();
                    }

                    println!("Responding with: REPLCONF ACK {}", processed_bytes);
                    storage.reset_processed_write_command_bytes();
                    Bytes::from(format!(
                        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                        processed_bytes.len(),
                        processed_bytes,
                    ))
                } else {
                    println!("Recieved handhake from replica: REPLCONF {:?}", args);
                    Bytes::from("+OK\r\n")
                }
            }
            Self::Psync(arg1, arg2) => {
                println!("Recieved handhake from replica: PSYNC {arg1} {arg2}");
                if let Some((master_replid, master_repl_offset)) = storage.get_repl_id_and_offset()
                {
                    let rdb_file = storage.get_rdb_file();
                    let msg = format!(
                        "+FULLRESYNC {} {}\r\n${}\r\n",
                        master_replid,
                        master_repl_offset,
                        rdb_file.len()
                    );
                    println!("Sending resync data to replica: FULLRESYNC...");

                    let mut b = BytesMut::from(msg.as_bytes());
                    b.extend_from_slice(&rdb_file);
                    Bytes::from(b)
                } else {
                    Bytes::from("$-1\r\n")
                }
            }
            Self::Wait {
                args: _,
                replicas_count,
            } => Bytes::from(format!(":{}\r\n", replicas_count)),
            Self::Config(comm, key) => {
                let conf = storage.get_config();
                if comm != "GET" {
                    bail!("Invalid config command: {}", comm)
                }
                if !["dir", "dbfilename"].contains(&key.as_str()) {
                    bail!("Invalid config key: {}", key)
                }
                if key == "dir" {
                    if let Some(val) = &conf.dir {
                        Bytes::from(format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", val.len(), val))
                    } else {
                        Bytes::from("$-1\r\n")
                    }
                } else if key == "dbfilename" {
                    let out = if let Some(val) = &conf.db_filename {
                        format!(
                            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key,
                            val.len(),
                            val
                        )
                    } else {
                        "$-1\r\n".to_owned()
                    };
                    Bytes::from(out)
                } else {
                    Bytes::from("$-1\r\n")
                }
            }
            Self::Keys(pattern) => {
                if pattern == "*" {
                    let keys = storage.db.keys(pattern);
                    let count = keys.len();
                    let mut res = format!("*{count}\r\n");
                    for key in keys {
                        let len = key.len();
                        res.push_str(&format!("${len}\r\n{key}\r\n"));
                    }
                    Bytes::from(res)
                } else {
                    Bytes::from("$-1\r\n")
                }
            }
            Self::Type(key) => {
                let val = match storage.db.get(key) {
                    None => {
                        if storage.db.streams().exists(key) {
                            String::from("+stream\r\n")
                        } else {
                            String::from("+none\r\n")
                        }
                    }
                    Some(_) => String::from("+string\r\n"),
                };
                Bytes::from(val)
            }
            Self::Xadd(args) => {
                let stream_key = &args[0];
                let mut id = args[1].clone();

                let mut key_values: Vec<KeyValue> = Vec::new();
                for double in args[2..].chunks_exact(2) {
                    if double.len() != 2 {
                        bail!("Expected 'field' and 'value', got {:?}", double)
                    }
                    key_values.push(KeyValue {
                        key: double[0].clone(),
                        value: double[1].clone(),
                    });
                }
                id = storage.db.xadd(stream_key, Entry::new(id, key_values)?)?;

                Bytes::from(format!("${}\r\n{id}\r\n", id.len()))
            }
            Self::Xrange(stream_key, start, end) => {
                let entries = storage.db.xrange(stream_key, start, end)?;
                let mut out = format!("*{}\r\n", entries.len());

                for entry in entries.iter() {
                    let id = entry.get_raw_id();
                    out.push_str(format!("*2\r\n${}\r\n{}\r\n", id.len(), id).as_str());

                    let key_values = entry.get_key_values();
                    out.push_str(format!("*{}\r\n", key_values.len() * 2).as_str());

                    for kv in key_values.iter() {
                        out.push_str(format!("${}\r\n{}\r\n", kv.key.len(), kv.key).as_str());
                        out.push_str(format!("${}\r\n{}\r\n", kv.value.len(), kv.value).as_str());
                    }
                }
                Bytes::from(out)
            }
        };
        Ok(response)
    }

    fn get_arg(parts: &mut std::vec::IntoIter<RESPType>) -> Result<BulkString> {
        let Some(RESPType::Bulk(arg)) = parts.next() else {
            bail!("command is missing argument");
        };
        Ok(arg)
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Set(key, value, expiry) => {
                if expiry != "0" {
                    return format!(
                        "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nPX\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        value.len(),
                        value,
                        expiry.len(),
                        expiry
                    )
                    .into_bytes();
                }
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value,
                )
                .into_bytes()
            }
            Self::Xadd(args) => {
                let stream_key = &args[0];
                let id = &args[1];

                let mut s = format!(
                    "*{}\r\n$4\r\nXADD\r\n${}\r\n{stream_key}\r\n${}\r\n{id}\r\n",
                    args.len() + 1,
                    stream_key.len(),
                    id.len()
                );

                for arg in args[2..].iter() {
                    s.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg))
                }

                s.into_bytes()
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;

    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_echo_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        let command = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let r = call_command(command, storage);
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+hey\r\n"));
    }

    #[test]
    fn test_info_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        let command = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
        let r = call_command(command, storage);
        let response = r.unwrap();
        assert_eq!(
            response,
            Bytes::from_static(b"$101\r\n# Replication\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n")
        );
    }

    #[test]
    fn test_replconf_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        // REPLCONF listening-port <PORT>
        let command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+OK\r\n"));

        // REPLCONF capa eof capa psync2
        let command =
            "*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+OK\r\n"));
    }

    #[test]
    fn test_psync_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        let command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        let r = call_command(command, storage);
        let response = r.unwrap();
        assert!(response.starts_with(&Bytes::from_static(
            b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n$88\r\nREDIS0011"
        )),);
    }

    #[test]
    fn test_set_and_get_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        // SET command
        let command = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\n\"Hello\"\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+OK\r\n"));

        // GET command
        let command = "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$7\r\n\"Hello\"\r\n"));

        // GET command - nonexistent key
        let command = "*2\r\n$3\r\nGET\r\n$5\r\nwrong\r\n";
        let r = call_command(command, storage);
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$-1\r\n"));
    }

    #[test]
    fn test_set_and_get_command_with_expiration() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        let command = "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+OK\r\n"));

        // GET command - immediatelly
        let command = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();

        // GET command - before expiration
        assert_eq!(response, Bytes::from_static(b"$3\r\nbar\r\n"));

        std::thread::sleep(std::time::Duration::from_millis(110));

        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();

        // GET command - after expiration
        assert_eq!(response, Bytes::from_static(b"$-1\r\n"));
    }

    #[test]
    fn test_commands_pipelining() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        let mut responses = vec![];

        // multiple commands in one go (pipelining)
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n".as_bytes());

        while !buf.is_empty() {
            let out = RESPType::parse(&mut buf);
            assert!(out.is_ok());
            let resp = out.unwrap();

            let r = Command::parse(resp);
            assert!(r.is_ok());
            let command = r.unwrap();

            let r = command.response(Arc::clone(&storage));
            assert!(r.is_ok());
            let response = r.unwrap();
            assert_eq!(response, Bytes::from_static(b"+OK\r\n"));

            responses.push(response);
        }

        assert_eq!(responses.len(), 3);
    }

    #[test]
    fn test_type_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        // TYPE command - nonexisting key
        let command = "*2\r\n$4\r\nTYPE\r\n$3\r\nfoo\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+none\r\n"));

        // SET command: SET foo bar PX 100
        let command = "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+OK\r\n"));

        // TYPE command - existing key
        let command = "*2\r\n$4\r\nTYPE\r\n$3\r\nfoo\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+string\r\n"));

        // STREAMS
        // XADD command: XADD stream_key 1526919030474-0 temperature 36 humidity 95
        let command = "*7\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$15\r\n1526919030474-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n";
        let r = call_command(command, Arc::clone(&storage));
        assert!(r.is_ok());

        // TYPE command - existing stream key
        let command = "*2\r\n$4\r\nTYPE\r\n$10\r\nstream_key\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+stream\r\n"));
    }

    #[test]
    fn test_xadd_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        let command = "*7\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$15\r\n1526919030474-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n";
        let r = call_command(command, storage);
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$15\r\n1526919030474-0\r\n"));
    }

    #[test]
    fn test_xadd_command_partial_id_generation() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        // 1st call
        let command =
            "*5\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$3\r\n1-*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$3\r\n1-0\r\n"));

        // 2nd call
        let command =
            "*5\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$3\r\n1-*\r\n$3\r\nfoo\r\n$3\r\nbaz\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$3\r\n1-1\r\n"));

        // 3nd call
        let command =
            "*5\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$3\r\n5-*\r\n$3\r\nfoo\r\n$3\r\nbaz\r\n";
        let r = call_command(command, storage);
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$3\r\n5-0\r\n"));
    }

    #[test]
    fn test_xadd_command_full_id_generation() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        // 1st call
        let command = "*5\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$1\r\n*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let r = call_command(command, Arc::clone(&storage));
        let response = r.unwrap();
        assert!(response.starts_with(b"$15\r\n"));
        assert!(response.ends_with(b"-0\r\n"));

        let ms = response
            .strip_prefix(b"$15\r\n")
            .unwrap()
            .strip_suffix(b"-0\r\n")
            .unwrap();
        assert_eq!(ms.len(), 13);

        let time_ms_orig = std::str::from_utf8(ms).unwrap().parse::<u64>().unwrap(); // 1710945822609;

        std::thread::sleep(std::time::Duration::from_millis(2));

        // 2nd call
        let command = "*5\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$1\r\n*\r\n$3\r\nfoo\r\n$3\r\nbaz\r\n";
        let r = call_command(command, storage);
        let response = r.unwrap();
        assert!(response.starts_with(b"$15\r\n"));
        assert!(response.ends_with(b"-0\r\n"));

        let ms = response
            .strip_prefix(b"$15\r\n")
            .unwrap()
            .strip_suffix(b"-0\r\n")
            .unwrap();
        assert_eq!(ms.len(), 13);

        let time_ms = std::str::from_utf8(ms).unwrap().parse::<u64>().unwrap(); // 1710945822611;
        assert!(time_ms >= time_ms_orig + 2);
    }

    #[test]
    fn test_xrange_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config).unwrap());

        let command = "*7\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$15\r\n1526985054059-3\r\n$11\r\ntemperature\r\n$2\r\n30\r\n$8\r\nhumidity\r\n$2\r\n72\r\n";
        let r = call_command(command, Arc::clone(&storage));
        assert!(r.is_ok());

        let command = "*7\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$15\r\n1526985054069-0\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n";
        let r = call_command(command, Arc::clone(&storage));
        assert!(r.is_ok());

        let command = "*7\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$15\r\n1526985054079-0\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n";
        let r = call_command(command, Arc::clone(&storage));
        assert!(r.is_ok());

        let command = "*7\r\n$4\r\nXADD\r\n$8\r\nsome_key\r\n$15\r\n1526985054089-0\r\n$11\r\ntemperature\r\n$2\r\n28\r\n$8\r\nhumidity\r\n$2\r\n65\r\n";
        let r = call_command(command, Arc::clone(&storage));
        assert!(r.is_ok());

        // XRANGE some_key 152698505406 1526985054079
        let command =
            "*4\r\n$6\r\nXRANGE\r\n$8\r\nsome_key\r\n$13\r\n1526985054069\r\n$13\r\n1526985054079\r\n";
        let r = call_command(command, Arc::clone(&storage));

        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"));

        // XRANGE some_key - 1526985054079
        let command = "*4\r\n$6\r\nXRANGE\r\n$8\r\nsome_key\r\n$1\r\n-\r\n$13\r\n1526985054079\r\n";
        let r = call_command(command, Arc::clone(&storage));

        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"*3\r\n*2\r\n$15\r\n1526985054059-3\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n30\r\n$8\r\nhumidity\r\n$2\r\n72\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"));

        // XRANGE some_key 152698505406 +
        let command = "*4\r\n$6\r\nXRANGE\r\n$8\r\nsome_key\r\n$13\r\n1526985054069\r\n$1\r\n+\r\n";
        let r = call_command(command, Arc::clone(&storage));

        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"*3\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n*2\r\n$15\r\n1526985054089-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n28\r\n$8\r\nhumidity\r\n$2\r\n65\r\n"));
    }

    /// helper function
    fn call_command(command_str: &str, storage: Arc<Storage>) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(command_str.as_bytes());

        let resp = RESPType::parse(&mut buf)?;
        let command = Command::parse(resp)?;

        command.response(storage)
    }
}
