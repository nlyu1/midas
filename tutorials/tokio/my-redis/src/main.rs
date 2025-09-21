use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

// https://tokio.rs/tokio/tutorial/spawning
// Copy to `src/main.rs` to run.

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        // The second item contains the IP and port of the new connection.
        // This blocks until some socket connects to the listener's port
        let (socket, socket_addr) = listener.accept().await.unwrap();
        // This prints out e.g. `127.0.0.1:58646`. Last number is random for each connection.
        println!("Got connection from {:?}", socket_addr);

        // This implementation blocks the main loop for each incoming socket connection
        // process(socket).await;
        // Alternatively, spawn a new task for each inbound socket
        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    // The `Connection` lets us read/write redis **frames** instead of
    // byte streams. The `Connection` type is defined by mini-redis.
    let mut connection = Connection::new(socket);

    if let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);

        // Respond with an error
        let response = Frame::Error("unimplemented".to_string());
        connection.write_frame(&response).await.unwrap();
    }
}
