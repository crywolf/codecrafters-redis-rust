mod command;
mod resp;

use command::Command;
use resp::RESPType;

use anyhow::{Context, Result};
use bytes::BytesMut;
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
