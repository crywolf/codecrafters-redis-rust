use std::sync::{Arc, Mutex};

use crate::command::Command;
use crate::resp::RESPType;
use crate::{config::Config, storage::Storage};

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub struct Server {
    config: Config,
    storage: Arc<Storage>,
    state: Arc<SharedState>,
}

impl Server {
    pub fn new(config: Config, storage: Arc<Storage>) -> Server {
        Self {
            config,
            storage,
            state: Arc::new(SharedState::new()),
        }
    }

    pub async fn run(&mut self) -> std::io::Result<()> {
        let addr = self.config.get_address();

        if self.config.is_replica() {
            let master_conn = self.connect_to_master().await?;
            let storage = Arc::clone(&self.storage);
            let state = Arc::clone(&self.state);
            let mut handler = ConnectionHandler::new(
                master_conn,
                ConnectionMode::ReplicaToMaster,
                storage,
                state,
            );
            tokio::spawn(async move {
                handler
                    .handle_connection()
                    .await
                    .map_err(|e| eprintln!("Error: {:?}", e))
            });
        }

        let listener = TcpListener::bind(&addr).await?;
        println!("Server is running on address: {addr}");

        loop {
            let (stream, addr) = listener.accept().await?;
            println!("New connection from {}", addr);
            let storage = Arc::clone(&self.storage);
            let state = Arc::clone(&self.state);
            let mut handler = ConnectionHandler::new(stream, ConnectionMode::Main, storage, state);
            tokio::spawn(async move {
                handler
                    .handle_connection()
                    .await
                    .map_err(|e| eprintln!("Error: {:?}", e))
            });
        }
    }

    /// Replica server connects to master server
    async fn connect_to_master(&mut self) -> std::io::Result<TcpStream> {
        // Handshake with master
        let mut buf = BytesMut::with_capacity(2048); // Reader buffer

        let master_addr = self
            .config
            .get_master_address()
            .expect("master address is set");
        println!("Connecting to master server '{}'", &master_addr);

        let mut stream = TcpStream::connect(&master_addr).await?;

        // 1. PING
        let ping = b"*1\r\n$4\r\nPING\r\n";
        stream.write_all(ping).await?;
        stream.flush().await?;

        buf.clear();
        stream.read_buf(&mut buf).await?;

        let mut master_response = BytesMut::with_capacity(128);
        master_response.put_slice(b"+PONG\r\n");
        assert_eq!(buf, master_response);

        // 2. REPLCONF listening-port <PORT>
        let port = &self.config.port;
        let replconf_1 =
            format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{port}\r\n");
        stream.write_all(replconf_1.as_bytes()).await?;
        stream.flush().await?;

        buf.clear();
        stream.read_buf(&mut buf).await?;

        master_response.clear();
        master_response.put_slice(b"+OK\r\n");
        assert_eq!(buf, master_response);

        // 3. REPLCONF capa psync2
        let replconf_2 = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        stream.write_all(replconf_2).await?;
        stream.flush().await?;

        buf.clear();
        stream.read_buf(&mut buf).await?;
        assert_eq!(buf, master_response);

        // 4. PSYNC ? -1 (PSYNC replicationid offset)
        let psync = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        stream.write_all(psync).await?;
        stream.flush().await?;

        buf.clear();
        stream.read_buf(&mut buf).await?;
        if !buf.starts_with(b"+FULLRESYNC") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "handshake failed, could not synchronize with master",
            ));
        }

        println!("Server is running as a replica of '{}'", &master_addr);

        Ok(stream)
    }
}

struct ConnectionHandler {
    stream: TcpStream,
    storage: Arc<Storage>,
    state: Arc<SharedState>,
    mode: ConnectionMode,
}

enum ConnectionMode {
    /// Main connection serving clients requests
    Main,
    /// Connection between replica and master
    ReplicaToMaster,
}

impl ConnectionHandler {
    pub fn new(
        stream: TcpStream,
        mode: ConnectionMode,
        storage: Arc<Storage>,
        state: Arc<SharedState>,
    ) -> Self {
        Self {
            stream,
            storage,
            state,
            mode,
        }
    }

    async fn handle_connection(&mut self) -> Result<()> {
        let mut buf = BytesMut::with_capacity(2048);

        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();

        loop {
            buf.clear();

            tokio::select! {
                n = self.stream.read_buf(&mut buf) => {
                    let n = n.unwrap_or(0);
                    if n == 0 {
                        if let ConnectionMode::ReplicaToMaster = self.mode {
                            println!("Connection with master closed");
                        } else {
                            println!("Connection closed");
                        }
                        break;
                    }

                    let command_bytes = Bytes::copy_from_slice(&buf);

                    let t = match RESPType::parse(&mut buf.split_to(n)).context("parsing RESP type") {
                        Ok(t) => t,
                        Err(err) => {
                            self.handle_error(err).await?;
                            continue;
                        }
                    };

                    let command = match Command::parse(t).context("parsing command") {
                        Ok(c) => c,
                        Err(err) => {
                            self.handle_error(err).await?;
                            continue;
                        }
                    };
                    if let Command::Replconf(args) = &command {
                        // Add connected replica
                        if let Some(index) = args.iter().position(|r| r == "listening-port") {
                            let port = args[index + 1].as_str();
                            self.add_replica(port.to_owned(), tx.clone());
                        }
                    }

                    let response = match command.response(Arc::clone(&self.storage)) {
                        Ok(r) => r,
                        Err(err) => {
                            self.handle_error(err).await?;
                            continue;
                        }
                    };

                    // Master sends writing (state changing) commands to replicas
                    if self.storage.is_master() && command.is_write() {
                        self.broadcast_to_replicas(command_bytes)?;
                    }

                    if let ConnectionMode::ReplicaToMaster = self.mode { // Connection between replica and master
                        continue; // we do not send responses to master
                    }
                    self.stream.write_all(&response).await?;
                    self.stream.flush().await?;
                },
                cmd = rx.recv() => {
                    println!("Sending command to replicas");
                    if let Some(cmd) = cmd {
                        self.stream.write_all(&cmd).await?;
                        self.stream.flush().await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_error(&mut self, e: anyhow::Error) -> Result<()> {
        eprintln!("Error: {:?}", e);
        let msg = format!("-ERR {}\r\n", e.root_cause());
        self.stream.write_all(msg.as_bytes()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub fn add_replica(&mut self, port: String, channel: mpsc::UnboundedSender<Bytes>) {
        let r = Replica::new(port, channel);
        self.state.add_replica(r);
    }

    pub fn broadcast_to_replicas(&self, command: Bytes) -> Result<()> {
        self.state.broadcast_to_replicas(command)
    }
}

struct SharedState {
    state: Mutex<State>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::new()),
        }
    }

    pub fn add_replica(&self, r: Replica) {
        self.state
            .lock()
            .expect("should be able to lock the mutex")
            .add_replica(r);
    }

    pub fn broadcast_to_replicas(&self, command: Bytes) -> Result<()> {
        for replica in self
            .state
            .lock()
            .expect("should be able to lock the mutex")
            .replicas
            .iter()
        {
            println!("broadcasting command: {:?}", &command);
            replica.channel.send(command.clone())?;
        }
        Ok(())
    }
}

struct State {
    pub replicas: Vec<Replica>,
}

impl State {
    pub fn new() -> Self {
        Self {
            replicas: Vec::new(),
        }
    }

    pub fn add_replica(&mut self, r: Replica) {
        println!("Added replica listening on port: {}", &r.port);
        self.replicas.push(r);
    }
}

#[derive(Debug)]
struct Replica {
    pub port: String,
    pub channel: mpsc::UnboundedSender<Bytes>,
}

impl Replica {
    pub fn new(port: String, channel: mpsc::UnboundedSender<Bytes>) -> Self {
        Self { port, channel }
    }
}
