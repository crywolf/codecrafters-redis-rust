use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream)?;
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0; 64];
    let response = b"+PONG\r\n";
    loop {
        let _ = stream.read(&mut buf)?;

        stream.write_all(response)?;
        stream.flush()?;
    }
}
