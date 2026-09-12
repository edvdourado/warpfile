use std::error::Error;
use std::io;
use std::path::Path;

use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::protocol::frame::{MAX_DATA_PAYLOAD_LENGTH, WFP_VERSION};

use crate::protocol::{
    FileOffer, Frame, MessageType, decode_reject, encode_offer, read_frame, write_frame,
};

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
            if !response.payload.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ACCEPT payload must be empty",
                )
                .into());
            }

            println!("Receiver accepted the file");
        }

        MessageType::Reject => {
            let reject = decode_reject(&response.payload)?;

            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "receiver rejected the file [{}]: {}",
                    reject.code, reject.message
                ),
            )
            .into());
        }

        _ => {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "expected ACCEPT or REJECT").into(),
            );
        }
    }

    let mut file = fs::File::open(path).await?;

    let mut buffer = vec![0u8; MAX_DATA_PAYLOAD_LENGTH];

    let mut hasher = blake3::Hasher::new();

    let mut bytes_sent: u64 = 0;

    loop {
        let bytes_read = file.read(&mut buffer).await?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);

        let data_frame = Frame::new(MessageType::Data, buffer[..bytes_read].to_vec());

        write_frame(&mut stream, &data_frame).await?;

        bytes_sent += bytes_read as u64;
    }

    if bytes_sent != file_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected to send {file_size} bytes, sent {bytes_sent}"),
        )
        .into());
    }

    println!("Sent {bytes_sent} bytes");

    let digest = hasher.finalize();

    let complete = Frame::new(MessageType::Complete, digest.as_bytes().to_vec());

    write_frame(&mut stream, &complete).await?;

    println!("Sent COMPLETE");

    let response = read_frame(&mut stream).await?;

    if response.message_type != MessageType::Verified {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected VERIFIED").into());
    }

    if !response.payload.is_empty() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "VERIFIED payload must be empty").into(),
        );
    }

    println!("Received VERIFIED");
    println!("Transfer successful");

    Ok(())
}
