use std::error::Error;
use std::io;

use tokio::net::TcpStream;

use crate::protocol::frame::WFP_VERSION;
use crate::protocol::{Frame, MessageType, read_frame, write_frame};

pub async fn run_sender(address: &str) -> Result<(), Box<dyn Error>> {
    println!("WarpFile Sender");
    println!("Connecting to {address}");

    let mut stream = TcpStream::connect(address).await?;

    println!("Connected");

    let hello = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

    write_frame(&mut stream, &hello).await?;

    println!("Sent HELLO (WFP/0.1)");

    let response = read_frame(&mut stream).await?;

    if response.message_type != MessageType::HelloAck {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected HELLO_ACK").into());
    }

    if response.payload != vec![WFP_VERSION] {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid HELLO_ACK version").into());
    }

    println!("Received HELLO_ACK (WFP/0.1)");

    println!("WFP handshake successful");

    Ok(())
}
