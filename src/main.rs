mod command;
mod resp;
mod storage;

use command::Command;
use resp::RESPType;
use storage::Storage;

use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use std::sync::Arc;

const DEFAULT_ADDRESS: &str = "127.0.0.1";
const DEFAULT_PORT: &str = "6379";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut addr = DEFAULT_ADDRESS.to_string();
    let mut port = DEFAULT_PORT.to_string();

    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                addr = args.next().unwrap_or(addr);
            }
            "--port" => {
                port = args.next().unwrap_or(port);
            }
            _ => {}
        }
    }

    let addr = format!("{DEFAULT_ADDRESS}:{port}");

    let listener = TcpListener::bind(&addr).await?;
    println!("Server running on address: {addr}");

    let storage: Arc<Storage> = Arc::new(Storage::new());

    loop {
        let (stream, _) = listener.accept().await?;
        let storage = Arc::clone(&storage);

        tokio::spawn(async move {
            handle_connection(stream, storage)
                .await
                .map_err(|e| eprintln!("Error: {:?}", e))
        });
    }
}

async fn handle_connection(mut stream: TcpStream, storage: Arc<Storage>) -> Result<()> {
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

        let response = match command.response(Arc::clone(&storage)) {
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
