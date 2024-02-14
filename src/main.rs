use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move { handle_connection(stream).await });
    }
}

async fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0; 64];
    let response = b"+PONG\r\n";
    loop {
        let _ = stream.read(&mut buf).await?;

        stream.write_all(response).await?;
        stream.flush().await?;
    }
}
