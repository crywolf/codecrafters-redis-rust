use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::command::Command;
use crate::resp::RESPType;
use crate::server::SharedState;
use crate::storage::Storage;

pub struct ConnectionHandler {
    stream: BufReader<TcpStream>,
    storage: Arc<Storage>,
    state: Arc<SharedState>,
    mode: ConnectionMode,
}

#[derive(Debug, Clone)]
pub enum ConnectionMode {
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

    pub async fn handle_connection(&mut self) -> Result<()> {
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

                                    command = Command::Wait{ args: args.clone(), replicas_count: synced_replicas };

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

                        if let Command::Xread{ stream_keys, starts, block: Some(timeout) } = &command {
                            let (tx, mut rx) = mpsc::channel::<()>(1);
                            for key in stream_keys {
                                self.storage.db.subscribe_to_stream(key, tx.clone())?;
                            }

                            println!("> XREAD command - start waiting for {timeout} ms");
                            let timeout = tokio::time::sleep(Duration::from_millis(*timeout));

                            tokio::select! {
                                _ = rx.recv() => {
                                    command = Command::Xread{ stream_keys: stream_keys.to_vec(), starts: starts.to_vec(), block: None };
                                }
                                _ = timeout => {
                                    println!("> XREAD command - timouted");
                                    // Sending (nil) response
                                    let response =  Bytes::from("$-1\r\n");
                                    self.stream.write_all(&response).await?;
                                    self.stream.flush().await?;

                                    break;
                                }
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
        self.state.add_replica(id.clone(), port, channel);
        id
    }

    pub fn remove_replica(&mut self, id: String) {
        self.state.remove_replica(id)
    }

    pub fn broadcast_to_replicas(&self, command: Bytes) -> Result<()> {
        self.state.broadcast_to_replicas(command)
    }
}
