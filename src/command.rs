use std::sync::Arc;

use crate::resp::{BulkString, RESPType};
use crate::storage::{Item, Storage};
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
                let expiry = if let Some(_px) = parts.find(|arg| {
                    if let RESPType::Bulk(k) = arg {
                        &k.data.to_lowercase() == "px"
                    } else {
                        false
                    }
                }) {
                    if let Some(RESPType::Bulk(exp)) = parts.next() {
                        exp
                    } else {
                        bail!("SET command is missing PX value miliseconds")
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
            _ => unimplemented!(),
        };

        Ok(command)
    }

    pub fn is_write(&self) -> bool {
        matches!(self, Self::Set { .. })
    }

    pub fn response(&self, storage: Arc<Storage>) -> Result<Bytes> {
        let response = match self {
            Self::Ping => Bytes::from("+PONG\r\n"),
            Self::Echo(arg) => Bytes::from(format!("+{arg}\r\n")),
            Self::Info(arg) => Bytes::from(storage.get_info(arg).unwrap_or("$-1\r\n".to_owned())),
            Self::Set(key, value, expiry) => {
                let expiry_ms = expiry.parse().unwrap_or_default();
                let item = Item::new(value, expiry_ms);
                storage.set(key, item);
                Bytes::from("+OK\r\n")
            }
            Self::Get(key) => {
                let val = match storage.get(key) {
                    Some(item) => format!("${}\r\n{}\r\n", item.value.len(), item.value),
                    None => String::from("$-1\r\n"),
                };
                Bytes::from(val)
            }
            Self::Replconf(args) => {
                if args.len() == 2 && args[0].to_lowercase() == "getack" && args[1] == "*" {
                    println!("Recieved command: REPLCONF {:?}", args);
                    // TODO: get real number of bytes
                    let sent = "0".to_string();
                    println!("Responding with: REPLCONF ACK {}", sent);
                    Bytes::from(format!(
                        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                        sent.len(),
                        sent,
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
                    let file = storage.get_rbd_file();
                    let msg = format!(
                        "+FULLRESYNC {} {}\r\n${}\r\n",
                        master_replid,
                        master_repl_offset,
                        file.len()
                    );
                    println!("Sending resync data to replica: FULLRESYNC...");

                    let mut b = BytesMut::from(msg.as_bytes());
                    b.extend_from_slice(&file);
                    Bytes::from(b)
                } else {
                    Bytes::from("$-1\r\n")
                }
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
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();
        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config));
        let r = command.response(storage);
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+hey\r\n"));
    }

    #[test]
    fn test_info_command() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config));
        let r = command.response(storage);
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(
            response,
            Bytes::from_static(b"$101\r\n# Replication\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n")
        );
    }

    #[test]
    fn test_replconf_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config));

        let mut buf = BytesMut::new();
        // REPLCONF listening-port <PORT>
        buf.extend_from_slice(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".as_bytes(),
        );

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

        // REPLCONF capa eof capa psync2
        buf.extend_from_slice(
            "*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
                .as_bytes(),
        );
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
    }

    #[test]
    fn test_psync_command() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config));
        let r = command.response(storage);
        assert!(r.is_ok());
        let response = r.unwrap();
        assert!(response.starts_with(&Bytes::from_static(
            b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n$88\r\nREDIS0011"
        )),);
    }

    #[test]
    fn test_set_and_get_command() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config));

        // SET command
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\n\"Hello\"\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        let r = command.response(Arc::clone(&storage)); // SET command
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+OK\r\n"));

        // GET command
        buf.extend_from_slice("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        let r = command.response(Arc::clone(&storage)); // GET command
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$7\r\n\"Hello\"\r\n"));

        // GET command - nonexistent key
        buf.extend_from_slice("*2\r\n$3\r\nGET\r\n$5\r\nwrong\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        let r = command.response(storage); // GET command
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$-1\r\n"));
    }

    #[test]
    fn test_set_and_get_command_with_expiration() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config));

        // SET command: SET foo bar PX 100
        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n".as_bytes(),
        );

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        let r = command.response(Arc::clone(&storage)); // SET command
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+OK\r\n"));

        // GET command - immediatelly
        buf.extend_from_slice("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();

        // GET command - before expiration
        let r = command.response(Arc::clone(&storage));
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$3\r\nbar\r\n"));

        std::thread::sleep(std::time::Duration::from_millis(300));

        // GET command - after expiration
        let r = command.response(Arc::clone(&storage));
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$-1\r\n"));
    }

    #[test]
    fn test_commands_pipelining() {
        let config = Arc::new(Config::new());
        let storage: Arc<Storage> = Arc::new(Storage::new(config));

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
}
