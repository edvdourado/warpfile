use std::error::Error;
use std::io;

use tokio::net::TcpListener;

use crate::protocol::frame::WFP_VERSION;
use crate::protocol::{Frame, MessageType, read_frame, write_frame};

pub async fn run_receiver(address: &str) -> Result<(), Box<dyn Error>> {
    println!("WarpFile Receiver");
    println!("Listening on {address}");

    let listener = TcpListener::bind(address).await?;

    let (mut stream, peer_address) = listener.accept().await?;

    println!("Connection from {peer_address}");

    let hello = read_frame(&mut stream).await?;

    if hello.message_type != MessageType::Hello {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected HELLO").into());
    }

    if hello.payload != vec![WFP_VERSION] {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid HELLO version").into());
    }

    println!("Received HELLO (WFP/0.1)");

    let response = Frame::new(MessageType::HelloAck, vec![WFP_VERSION]);

    write_frame(&mut stream, &response).await?;

    println!("Sent HELLO_ACK (WFP/0.1)");

    Ok(())
}
