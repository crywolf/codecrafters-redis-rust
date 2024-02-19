use std::sync::Arc;

use crate::resp::RESPType;
use crate::storage::Storage;
use anyhow::{bail, Result};
use bytes::Bytes;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set(String, String),
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
                let Some(RESPType::Bulk(arg)) = parts.next() else {
                    bail!("ECHO command is missing an argument");
                };
                Self::Echo(arg.data)
            }
            "SET" => {
                let Some(RESPType::Bulk(key)) = parts.next() else {
                    bail !("SET command is missing arguments");
                };
                let Some(RESPType::Bulk(val)) = parts.next() else {
                    bail !("SET command is missing value");
                };
                Self::Set(key.data, val.data)
            }
            "GET" => {
                let Some(RESPType::Bulk(key)) = parts.next() else {
                    bail!("GET command is missing an argument");
                };
                Self::Get(key.data)
            }
            _ => unimplemented!(),
        };

        Ok(command)
    }

    pub fn response(self, storage: Arc<Storage>) -> Result<Bytes> {
        let response = match self {
            Self::Ping => Bytes::from("+PONG\r\n"),
            Self::Echo(arg) => Bytes::from(format!("+{arg}\r\n")),
            Self::Set(key, val) => {
                storage.set(key, val);
                Bytes::from("+OK\r\n")
            }
            Self::Get(key) => {
                let val = match storage.get(&key) {
                    Some(val) => format!("${}\r\n{}\r\n", val.len(), val),
                    None => String::from("$-1\r\n"),
                };
                Bytes::from(val)
            }
        };
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
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

        let storage: Arc<Storage> = Arc::new(Storage::new());
        let r = command.response(storage);
        assert!(r.is_ok());
        let response = r.unwrap();
        assert_eq!(response, Bytes::from_static(b"+hey\r\n"));
    }

    #[test]
    fn test_set_and_get_command() {
        let storage: Arc<Storage> = Arc::new(Storage::new());

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
}
