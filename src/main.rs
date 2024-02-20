mod command;
mod config;
mod resp;
mod storage;

use command::Command;
use config::Config;
use resp::RESPType;
use storage::Storage;

use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
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
        connect_to_master(&config).await?;
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

/// Replica server connects to master server
async fn connect_to_master(config: &Config) -> std::io::Result<()> {
    // Handshake with master
    let master_addr = config.get_master_address().expect("master address is set");
    println!("Connecting to master server '{}'", &master_addr);

    let mut stream = TcpStream::connect(&master_addr).await?;

    // 1. PING
    let ping = b"*1\r\n$4\r\nPING\r\n";
    stream.write_all(ping).await?;
    stream.flush().await?;

    let mut buf = BytesMut::with_capacity(128);
    stream.read_buf(&mut buf).await?;

    let mut master_response = BytesMut::with_capacity(128);
    master_response.put_slice(b"+PONG\r\n");
    assert_eq!(buf, master_response);

    // 2. REPLCONF listening-port <PORT>
    let port = &config.port;
    let replconf_1 = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{port}\r\n");
    stream.write_all(replconf_1.as_bytes()).await?;
    stream.flush().await?;

    let mut buf = BytesMut::with_capacity(128);
    stream.read_buf(&mut buf).await?;

    master_response.clear();
    master_response.put_slice(b"+OK\r\n");
    assert_eq!(buf, master_response);

    // 3. REPLCONF capa psync2
    let replconf_2 = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    stream.write_all(replconf_2).await?;
    stream.flush().await?;

    let mut buf = BytesMut::with_capacity(128);
    stream.read_buf(&mut buf).await?;
    assert_eq!(buf, master_response);

    // 4. PSYNC ? -1 (PSYNC replicationid offset)
    let psync = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    stream.write_all(psync).await?;
    stream.flush().await?;

    let mut buf = BytesMut::with_capacity(128);
    stream.read_buf(&mut buf).await?;

    master_response.clear();
    master_response.put_slice(b"+FULLRESYNC");
    assert!(buf.starts_with(master_response.to_vec().as_slice()));

    println!("Server is running as a replica of '{}'", &master_addr);

    Ok(())
}
