use std::sync::Arc;

use crate::resp::{BulkString, RESPType};
use crate::storage::{Item, Storage};
use anyhow::{bail, Result};
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Info(String),
    Set(String, String, String),
    Get(String),
}

impl Command {
    pub fn parse(resp: RESPType) -> Result<Self> {
        let RESPType::Array(resp) = resp else {
            bail!("invalid command, must be an array");
        };

        let mut parts = resp.into_iter();

        let Some(RESPType::Bulk(cmd)) = parts.next() else {
            bail!("invalid command name, command must be a bulk string");
        };

        let command = match cmd.data.to_uppercase().as_str() {
            "PING" => Self::Ping,
            "ECHO" => {
                let arg = Self::get_arg(parts)?;
                Self::Echo(arg.data)
            }
            "INFO" => {
                let arg = Self::get_arg(parts)?;
                Self::Info(arg.data)
            }
            "SET" => {
                let Some(RESPType::Bulk(key)) = parts.next() else {
                    bail !("SET command is missing arguments");
                };
                let Some(RESPType::Bulk(val)) = parts.next() else {
                    bail !("SET command is missing value");
                };
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
                let arg = Self::get_arg(parts)?;
                Self::Get(arg.data)
            }
            _ => unimplemented!(),
        };

        Ok(command)
    }

    pub fn response(self, storage: Arc<Storage>) -> Result<Bytes> {
        let response = match self {
            Self::Ping => Bytes::from("+PONG\r\n"),
            Self::Echo(arg) => Bytes::from(format!("+{arg}\r\n")),
            Self::Info(arg) => Bytes::from(storage.get_info(&arg).unwrap_or("$-1\r\n".to_owned())),
            Self::Set(key, value, expiry) => {
                let expiry_ms = expiry.parse().unwrap_or_default();
                let item = Item::new(value, expiry_ms);
                storage.set(key, item);
                Bytes::from("+OK\r\n")
            }
            Self::Get(key) => {
                let val = match storage.get(&key) {
                    Some(item) => format!("${}\r\n{}\r\n", item.value.len(), item.value),
                    None => String::from("$-1\r\n"),
                };
                Bytes::from(val)
            }
        };
        Ok(response)
    }

    fn get_arg(mut parts: std::vec::IntoIter<RESPType>) -> Result<BulkString> {
        let Some(RESPType::Bulk(arg)) = parts.next() else {
            bail!("command is missing an argument");
        };
        Ok(arg)
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
            Bytes::from_static(b"$25\r\n# Replication\nrole:master\r\n")
        );
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
        let command2 = command.clone();

        // GET command - before expiration
        let r = command.response(Arc::clone(&storage));
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$3\r\nbar\r\n"));

        std::thread::sleep(std::time::Duration::from_millis(300));

        // GET command - after expiration
        let r = command2.response(Arc::clone(&storage));
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"$-1\r\n"));
    }
}
