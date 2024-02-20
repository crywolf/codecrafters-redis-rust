mod command;
mod config;
mod resp;
mod storage;

use command::Command;
use config::Config;
use resp::RESPType;
use storage::Storage;

use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use std::sync::Arc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut config = Config::new();

    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                config.addr = args.next().unwrap_or(config.addr);
            }
            "--port" => {
                config.port = args.next().unwrap_or(config.port);
            }
            "--replicaof" => {
                config.master_addr = args.next();
                config.master_port = args.next();
            }
            _ => {}
        }
    }

    let addr = config.get_address();

    let listener = TcpListener::bind(&addr).await?;
    println!("Server is running on address: {addr}");
    if config.is_replica() {
        println!(
            "Server is running as a replica of '{}:{}'",
            config.master_addr.as_ref().expect("addr of master is set"),
            config.master_port.as_ref().expect("port of master is set")
        );
    }

    let storage: Arc<Storage> = Arc::new(Storage::new(Arc::new(config)));

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
