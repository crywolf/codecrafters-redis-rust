use anyhow::{bail, Context, Result};
use bytes::{Buf, BytesMut};

const CRLF: &[u8; 2] = b"\r\n";

#[derive(Debug, PartialEq)]
pub enum RESPType {
    String(String),
    Array(Vec<RESPType>),
    Bulk(BulkString),
}

#[derive(Debug, PartialEq)]
pub struct BulkString {
    pub len: usize,
    pub data: String,
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
    let mut v = Vec::new();
    while buf[0] != b'\r' {
        let num = buf.get_u8() - 48; // numbers in ASCII start at 48 (48 means 0)
        v.push(num);
    }
    let mut exp = 10usize.pow((v.len() - 1) as u32);

    let len: usize = v
        .iter()
        .map(|n| {
            let len = *n as usize * exp;
            exp /= 10;
            len
        })
        .sum();

    len
}

fn remove_crlf(buf: &mut BytesMut) -> Result<()> {
    if buf.get_u16() != u16::from_be_bytes(*CRLF) {
        bail!("invalid string, missing '\\r\\n'")
    }
    Ok(())
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
}
