use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};

use crate::config::Config;
use crate::connection::{ConnectionHandler, ConnectionMode};
use crate::resp::RESPType;
use crate::storage::Storage;

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
    connections_counter: u64,
}

impl Server {
    pub fn new(config: Config, storage: Arc<Storage>) -> Server {
        Self {
            config,
            storage,
            state: Arc::new(SharedState::new()),
            connections_counter: 0,
        }
    }

    pub async fn run(&mut self) -> std::io::Result<()> {
        if self.config.is_replica() {
            let master_conn = self.connect_to_master().await?;
            let storage = Arc::clone(&self.storage);
            let state = Arc::clone(&self.state);
            self.connections_counter += 1;
            let mut handler = ConnectionHandler::new(
                master_conn,
                ConnectionMode::ReplicaToMaster,
                storage,
                state,
                self.connections_counter,
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
            self.connections_counter += 1;
            let mut handler = ConnectionHandler::new(
                BufReader::new(stream),
                ConnectionMode::Main,
                storage,
                state,
                self.connections_counter,
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

pub struct SharedState {
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

    pub fn add_replica(&self, id: String, port: String, channel: mpsc::UnboundedSender<Bytes>) {
        let r = Replica::new(id, port, channel);

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
