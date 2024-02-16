use crate::resp::RESPType;
use anyhow::{bail, Result};
use bytes::Bytes;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
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
            _ => unimplemented!(),
        };

        Ok(command)
    }

    pub fn response(&self) -> Result<Bytes> {
        let response = match self {
            Self::Ping => Bytes::from("+PONG\r\n"),
            Self::Echo(arg) => Bytes::from(format!("+{arg}\r\n")),
        };
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_echo_command_parsing() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let resp = out.unwrap();

        let r = Command::parse(resp);
        assert!(r.is_ok());
        let command = r.unwrap();
        let r = command.response();
        assert!(r.is_ok());
        let response = r.unwrap();
        dbg!(&response);
        assert_eq!(response, Bytes::from_static(b"+hey\r\n"));
    }
}
