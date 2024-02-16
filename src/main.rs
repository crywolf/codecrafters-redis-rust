use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(stream)
                .await
                .map_err(|e| eprintln!("Error: {:?}", e))
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        buf.clear();
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            continue;
        }

        let t = match RESPType::parse(&mut buf.split_to(n)).context("parsing RESP type") {
            Ok(t) => t,
            Err(err) => {
                handle_error(&mut stream, err).await?;
                continue;
            }
        };

        let command = match Command::parse(t).context("parsing command") {
            Ok(c) => c,
            Err(err) => {
                handle_error(&mut stream, err).await?;
                continue;
            }
        };

        let response = match command.response() {
            Ok(r) => r,
            Err(err) => {
                handle_error(&mut stream, err).await?;
                continue;
            }
        };

        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}

async fn handle_error(stream: &mut TcpStream, e: anyhow::Error) -> Result<()> {
    eprintln!("Error: {:?}", e);
    let msg = format!("-ERR {}\r\n", e.root_cause());
    stream.write_all(msg.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

const CRLF: &[u8; 2] = b"\r\n";

#[derive(Debug, PartialEq)]
enum RESPType {
    String(String),
    Array(Vec<RESPType>),
    Bulk(BulkString),
}

#[derive(Debug, PartialEq)]
struct BulkString {
    len: usize,
    data: String,
}

impl RESPType {
    pub fn parse(buf: &mut BytesMut) -> Result<Self> {
        let kind = buf.get_u8();
        Ok(match kind {
            b'+' => Self::String(Self::parse_string(buf)?),
            b'$' => Self::Bulk(Self::parse_bulk(buf)?),
            b'*' => Self::Array(Self::parse_array(buf)?),
            b':' => unimplemented!(),
            b'-' => unimplemented!(),
            e => bail!("invalid type marker '{}'", (e as char).escape_default()),
        })
    }

    fn parse_string(buf: &mut BytesMut) -> Result<String> {
        let mut s = String::new();
        while buf[0] != b'\r' {
            s.push(buf.get_u8() as char);
        }

        remove_crlf(buf)?;

        Ok(s)
    }

    fn parse_bulk(buf: &mut BytesMut) -> Result<BulkString> {
        let len = read_len(buf);
        remove_crlf(buf)?;

        let mut s = String::new();
        for _ in 0..len {
            s.push(buf.get_u8() as char);
        }

        remove_crlf(buf)?;

        Ok(BulkString {
            len: s.len(),
            data: s,
        })
    }

    fn parse_array(buf: &mut BytesMut) -> Result<Vec<Self>> {
        let len = read_len(buf);
        remove_crlf(buf)?;
        let mut v = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(RESPType::parse(buf).context("parsing array")?);
        }
        Ok(v)
    }
}

fn read_len(buf: &mut BytesMut) -> usize {
    (buf.get_u8() - 48) as usize // numbers in ASCII start at 48 (48 means 0)
}

fn remove_crlf(buf: &mut BytesMut) -> Result<()> {
    if buf.get_u16() != u16::from_be_bytes(*CRLF) {
        bail!("invalid string, missing '\\r\\n'")
    }
    Ok(())
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
}

impl Command {
    fn parse(resp: RESPType) -> Result<Self> {
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

    fn response(&self) -> Result<Bytes> {
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

    #[test]
    fn test_parse_array_of_simple_strings() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*2\r\n+ONE\r\n+TWO\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let out = out.unwrap();
        assert!(
            out == RESPType::Array(vec![
                RESPType::String("ONE".to_string()),
                RESPType::String("TWO".to_string())
            ])
        );
    }

    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice("+PING\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let out = out.unwrap();

        assert_eq!(out, RESPType::String("PING".to_string()));
    }

    #[test]
    fn test_parse_array_of_bulk_strings() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n".as_bytes());

        let out = RESPType::parse(&mut buf);
        assert!(out.is_ok());
        let out = out.unwrap();

        assert_eq!(
            out,
            RESPType::Array(vec![
                RESPType::Bulk(BulkString {
                    len: 4,
                    data: "ECHO".to_string()
                }),
                RESPType::Bulk(BulkString {
                    len: 3,
                    data: "hey".to_string()
                })
            ])
        );
    }

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
