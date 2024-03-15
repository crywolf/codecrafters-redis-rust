mod command;
mod config;
mod connection;
mod db;
mod resp;
mod server;
mod storage;

use config::Config;
use server::Server;
use storage::Storage;

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
            "--dir" => {
                config.dir = args.next();
            }
            "--dbfilename" => {
                config.db_filename = args.next();
            }
            _ => {}
        }
    }

    let storage: Arc<Storage> = Arc::new(Storage::new(Arc::new(config.clone()))?);

    let mut server = Server::new(config, storage);

    server.run().await
}
