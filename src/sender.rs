use std::error::Error;
use std::io;
use std::path::Path;

use tokio::fs;
use tokio::net::TcpStream;

use crate::protocol::frame::WFP_VERSION;
use crate::protocol::{FileOffer, Frame, MessageType, encode_offer, read_frame, write_frame};

pub async fn run_sender(file_path: &str, address: &str) -> Result<(), Box<dyn Error>> {
    println!("WarpFile Sender");

    let path = Path::new(file_path);

    let metadata = fs::metadata(path).await?;

    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the supplied path is not a file",
        )
        .into());
    }

    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "filename is not valid UTF-8"))?
        .to_string();

    let file_size = metadata.len();

    println!("File: {filename}");
    println!("Size: {file_size} bytes");
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

    println!("WFP handshake successful");

    let offer = FileOffer {
        filename,
        file_size,
    };

    let offer_payload = encode_offer(&offer)?;

    let offer_frame = Frame::new(MessageType::Offer, offer_payload);

    write_frame(&mut stream, &offer_frame).await?;

    println!("Sent OFFER");

    let response = read_frame(&mut stream).await?;

    match response.message_type {
        MessageType::Accept => {
            println!("Receiver accepted the file");
        }

        MessageType::Reject => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "receiver rejected the file",
            )
            .into());
        }

        _ => {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "expected ACCEPT or REJECT").into(),
            );
        }
    }

    println!("File negotiation successful");

    Ok(())
}
