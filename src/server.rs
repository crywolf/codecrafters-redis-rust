use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::command::Command;
use crate::resp::RESPType;
use crate::{config::Config, storage::Storage};

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
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

        // Ensure that replica to master connection loop starts first (before main connection loop)
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        let addr = self.config.get_address();
        let listener = TcpListener::bind(&addr).await?;
        println!("Server is running on address: {addr}");

        loop {
            let (stream, addr) = listener.accept().await?;
            println!("New connection from {}", addr);
            let storage = Arc::clone(&self.storage);
            let state = Arc::clone(&self.state);
            let mut handler = ConnectionHandler::new(
                BufReader::new(stream),
                ConnectionMode::Main,
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
    }

    /// Replica server connects to master server
    async fn connect_to_master(&mut self) -> std::io::Result<BufReader<TcpStream>> {
        // Handshake with master
        let mut buf = BytesMut::with_capacity(2048); // Reader buffer

        let master_addr = self
            .config
            .get_master_address()
            .expect("master address is set");
        println!("Connecting to master server '{}'", &master_addr);

        let mut stream = BufReader::new(TcpStream::connect(&master_addr).await?);

        // 1. send PING
        let ping = b"*1\r\n$4\r\nPING\r\n";
        stream.write_all(ping).await?;
        stream.flush().await?;

        buf.clear();
        stream.read_buf(&mut buf).await?;

        let mut master_response = BytesMut::with_capacity(128);
        master_response.put_slice(b"+PONG\r\n");
        assert_eq!(buf, master_response);

        // 2. send REPLCONF listening-port <PORT>
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

        // 3. send REPLCONF capa psync2
        let replconf_2 = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        stream.write_all(replconf_2).await?;
        stream.flush().await?;

        buf.clear();
        stream.read_buf(&mut buf).await?;
        assert_eq!(buf, master_response);

        // 4. send PSYNC ? -1 (PSYNC replicationid offset)
        let psync = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        stream.write_all(psync).await?;
        stream.flush().await?;

        buf.clear();

        // 5. receive FULLSYNC response (+FULLRESYNC hdtdb24osaagg3pklp48uhf0297kgwsge1rp2l5n 0)
        let mut line = String::new();
        stream.read_line(&mut line).await?;
        buf.put_slice(line.as_bytes());

        let t = match RESPType::parse(&mut buf).context("parsing RESP type") {
            Ok(t) => t,
            Err(err) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "handshake failed, error parsing RESP type: {}",
                        err.root_cause()
                    ),
                ));
            }
        };

        if let RESPType::String(s) = t {
            if !(s.len() == 53 && s.starts_with("FULLRESYNC")) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "handshake failed, could not synchronize with master (FULLRESYNC)",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "handshake failed, could not synchronize with master (parsing FULLRESYNC)",
            ));
        }

        // 6. receive RDB file response
        line.clear();
        stream.read_line(&mut line).await?;

        if let Ok(Some(len)) = line
            .strip_prefix('$')
            .map(str::trim_end)
            .map(str::parse::<usize>)
            .transpose()
        {
            let mut rdb_file_buf = BytesMut::with_capacity(len);
            stream.read_buf(&mut rdb_file_buf).await?;

            if !rdb_file_buf.starts_with(b"REDIS0011") {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "handshake failed, could not read RDB file",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "handshake failed, could not synchronize with master (parsing RDB file - length)",
            ));
        }

        println!("Server is running as a replica of '{}'", &master_addr);

        Ok(stream)
    }
}

struct ConnectionHandler {
    stream: BufReader<TcpStream>,
    storage: Arc<Storage>,
    state: Arc<SharedState>,
    mode: ConnectionMode,
}

#[derive(Debug, Clone)]
enum ConnectionMode {
    /// Main connection serving clients requests
    Main,
    /// Connection between replica and master, initiated by replica
    ReplicaToMaster,
}

impl ConnectionHandler {
    pub fn new(
        stream: BufReader<TcpStream>,
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
        let mut connected_replica_id: Option<String> = None;
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();

        loop {
            buf.clear();

            tokio::select! {
                n = self.stream.read_buf(&mut buf) => {
                    let n = n.unwrap_or(0);
                    if n == 0 {
                        if let ConnectionMode::ReplicaToMaster = self.mode {
                            println!("Connection with master closed");
                        } else if let Some(id) = connected_replica_id.clone() {
                            println!("Replica {} disconnected", id);
                            self.remove_replica(id);
                        } else {
                            println!("Connection closed");
                        }
                        break;
                    }

                    println!("Listening as {:?}", self.mode);

                    while !buf.is_empty() {
                        dbg!(&buf);
                        let bytes_read = buf.len();

                        let t = match RESPType::parse(&mut buf).context("parsing RESP type") {
                            Ok(t) => t,
                            Err(err) => {
                                self.handle_error(err).await?;
                                break;
                            }
                        };

                        let mut command = match Command::parse(t).context("parsing command") {
                            Ok(c) => c,
                            Err(err) => {
                                self.handle_error(err).await?;
                                break;
                            }
                        };

                        if let Command::Replconf(args) = &command {
                            // Replica wants to connect -> add connected replica
                            if let Some(index) = args.iter().position(|r| r == "listening-port") {
                                let port = args[index + 1].as_str();
                                let replica_id = self.add_replica(port.to_owned(), tx.clone());
                                connected_replica_id = Some(replica_id)
                            } else if let Some(index) = args.iter().position(|r| r.to_uppercase() == "GETACK") {
                                let what = args[index + 1].as_str();
                                println!("Received command (from master) REPLCONF GETACK {}", what);
                            } else if let Some(index) = args.iter().position(|r| r.to_uppercase() == "ACK") {
                                let ack_bytes = args[index + 1].parse::<usize>()?;

                                if ack_bytes >= self.state.get_sent_write_command_bytes() {
                                    self.state.incr_synced_replicas();
                                }
                                println!("Synced replicas currently {}", self.state.synced_replicas());

                                // Do not send response to REPLCONF ACK command because IT IS actually a response to previous command REPLCONF GETACK
                                continue;
                            }
                        }

                        if let Command::Wait{args, ..} = command {
                            command = Command::Wait{args: args.clone(), replicas_count: self.state.replicas_count() };

                            if self.storage.is_master() && self.state.get_sent_write_command_bytes() > 0 {
                                let (num_replicas, timeout) = args.clone();
                                let expected_replicas = num_replicas.parse::<usize>()?;

                                println!("> WAIT command - expected replicas: {}, timeout: {}", expected_replicas, timeout);

                                self.state.reset_synced_replicas();

                                let timeout = Duration::from_millis(timeout.parse::<u64>()?);
                                let start = Instant::now();

                                println!("Asking replicas to acknowledge accepted write commands");
                                self.broadcast_to_replicas(Bytes::from("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$5\r\nWRITE\r\n"))?;

                                loop {
                                    let synced_replicas = self.state.synced_replicas();

                                    command = Command::Wait{args: args.clone(), replicas_count: synced_replicas };

                                    if synced_replicas >= expected_replicas {
                                        println!("> WAIT command - OK: {:?}", start.elapsed());
                                        break;
                                    }
                                    if start.elapsed() > timeout {
                                        println!("> WAIT command - timouted: {:?}", start.elapsed());
                                        break;
                                    }

                                    tokio::task::yield_now().await;
                                }

                                self.state.reset_sent_write_command_bytes();
                            }
                        }

                        let response = match command.response(Arc::clone(&self.storage)) {
                            Ok(r) => r,
                            Err(err) => {
                                self.handle_error(err).await?;
                                break;
                            }
                        };

                        // Master sends write (i.e. state changing) commands to replicas
                        if self.storage.is_master() && command.is_write() {
                            println!("Broadcasting command {:?} to replicas (if any)", command);
                            let command_bytes = command.clone().into_bytes();
                            let command_bytes_len = command_bytes.len();
                            self.broadcast_to_replicas(Bytes::from(command_bytes))?;

                            // Store the count of all sent write commands
                            self.state.add_sent_write_command_bytes(command_bytes_len);
                        }

                        if let ConnectionMode::ReplicaToMaster = self.mode { // Connection between replica and master
                            println!("Received command from master {:?}", command);

                            // Replica stores how many bytes received from master
                            let bytes_processed = bytes_read - buf.len();
                            self.storage.add_processed_bytes(bytes_processed);

                            if command.is_write() {
                                self.storage.add_processed_write_command_bytes(bytes_processed);
                            }

                            // Send response only to 'REPLCONF GETACK *' command
                            match command {
                                Command::Replconf(args) => {
                                    if args.len() != 2 || args[0].to_uppercase() != "GETACK" {
                                        continue;
                                    } // send response to REPLCONF GETACK
                                }
                                _ => { continue; } // do not send responses to master for all other commands
                            }
                        }

                        // Sending response
                        self.stream.write_all(&response).await?;
                        self.stream.flush().await?;
                    }
                },

                cmd = rx.recv() => {
                    // Sending command to connected replica
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

    pub fn add_replica(&mut self, port: String, channel: mpsc::UnboundedSender<Bytes>) -> String {
        let id = (self.state.replicas_count() + 1).to_string();
        let r = Replica::new(id.clone(), port, channel);
        self.state.add_replica(r);
        id
    }

    pub fn remove_replica(&mut self, id: String) {
        self.state.remove_replica(id)
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

    pub fn replicas_count(&self) -> usize {
        self.state
            .lock()
            .expect("should be able to lock the mutex")
            .replicas
            .len()
    }

    pub fn add_replica(&self, r: Replica) {
        self.state
            .lock()
            .expect("should be able to lock the mutex")
            .add_replica(r);
    }

    pub fn remove_replica(&self, id: String) {
        self.state
            .lock()
            .expect("should be able to lock the mutex")
            .remove_replica(id)
    }

    pub fn broadcast_to_replicas(&self, command: Bytes) -> Result<()> {
        let replicas = &self
            .state
            .lock()
            .expect("should be able to lock the mutex")
            .replicas;

        if !replicas.is_empty() {
            println!("Broadcasting command to {} replicas", replicas.len());
        }

        for replica in replicas.values() {
            println!("Broadcasting to replica {}", replica.id);
            replica.channel.send(command.clone())?;
        }
        Ok(())
    }

    pub fn add_sent_write_command_bytes(&self, count: usize) {
        self.state
            .lock()
            .expect("shoul be able to lock the mutex")
            .add_sent_write_command_bytes(count)
    }

    pub fn get_sent_write_command_bytes(&self) -> usize {
        self.state
            .lock()
            .expect("shoul be able to lock the mutex")
            .get_sent_write_command_bytes()
    }

    pub fn reset_sent_write_command_bytes(&self) {
        self.state
            .lock()
            .expect("shoul be able to lock the mutex")
            .reset_sent_write_command_bytes();
    }

    pub fn synced_replicas(&self) -> usize {
        self.state
            .lock()
            .expect("should be able to lock the mutex")
            .synced_replicas
    }

    pub fn incr_synced_replicas(&self) {
        self.state
            .lock()
            .expect("should be able to lock the mutex")
            .synced_replicas += 1;
    }

    pub fn reset_synced_replicas(&self) {
        self.state
            .lock()
            .expect("should be able to lock the mutex")
            .synced_replicas = 0;
    }
}

struct State {
    replicas: HashMap<String, Replica>,
    synced_replicas: usize,
    /// Number of bytes of write commands sent by master
    sent_write_command_bytes: usize,
}

impl State {
    pub fn new() -> Self {
        Self {
            replicas: HashMap::new(),
            synced_replicas: 0,
            sent_write_command_bytes: 0,
        }
    }

    pub fn add_replica(&mut self, r: Replica) {
        println!("Added replica {} listening on port: {}", &r.id, &r.port);
        self.replicas.insert(r.id.clone(), r);
    }

    pub fn remove_replica(&mut self, id: String) {
        let r = self.replicas.remove(&id);
        println!(
            "Removing replica listening on port: {}",
            r.expect("replica should be in the list").port
        );
    }

    pub fn add_sent_write_command_bytes(&mut self, count: usize) {
        self.sent_write_command_bytes += count;
    }

    pub fn get_sent_write_command_bytes(&self) -> usize {
        self.sent_write_command_bytes
    }

    pub fn reset_sent_write_command_bytes(&mut self) {
        self.sent_write_command_bytes = 0;
    }
}

#[derive(Debug)]
struct Replica {
    pub id: String,
    pub port: String,
    pub channel: mpsc::UnboundedSender<Bytes>,
}

impl Replica {
    pub fn new(id: String, port: String, channel: mpsc::UnboundedSender<Bytes>) -> Self {
        Self { id, port, channel }
    }
}
