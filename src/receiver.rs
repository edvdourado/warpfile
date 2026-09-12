use std::error::Error;
use std::io;
use std::path::{Component, Path};

use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::protocol::frame::WFP_VERSION;
use crate::protocol::{Frame, MessageType, decode_offer, read_frame, write_frame};

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

    let hello_ack = Frame::new(MessageType::HelloAck, vec![WFP_VERSION]);

    write_frame(&mut stream, &hello_ack).await?;

    println!("Sent HELLO_ACK (WFP/0.1)");

    let offer_frame = read_frame(&mut stream).await?;

    if offer_frame.message_type != MessageType::Offer {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected OFFER").into());
    }

    let offer = decode_offer(&offer_frame.payload)?;

    if !is_safe_filename(&offer.filename) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unsafe filename").into());
    }

    println!();
    println!("Incoming file:");
    println!("Name: {}", offer.filename);
    println!("Size: {} bytes", offer.file_size);
    println!();

    fs::create_dir_all("received").await?;

    let destination = Path::new("received").join(&offer.filename);

    let partial_name = format!("{}.part", offer.filename);

    let partial_destination = Path::new("received").join(partial_name);

    if fs::try_exists(&destination).await? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "destination file already exists",
        )
        .into());
    }

    if fs::try_exists(&partial_destination).await? {
        fs::remove_file(&partial_destination).await?;
    }

    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial_destination)
        .await?;

    let accept = Frame::new(MessageType::Accept, Vec::new());

    write_frame(&mut stream, &accept).await?;

    println!("Sent ACCEPT");

    let mut hasher = blake3::Hasher::new();

    let mut bytes_received: u64 = 0;

    let sender_hash = loop {
        let frame = read_frame(&mut stream).await?;

        match frame.message_type {
            MessageType::Data => {
                let chunk_size = frame.payload.len() as u64;

                let next_total = bytes_received.checked_add(chunk_size).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "received byte count overflow")
                })?;

                if next_total > offer.file_size {
                    let _ = fs::remove_file(&partial_destination).await;

                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received more bytes than announced",
                    )
                    .into());
                }

                output.write_all(&frame.payload).await?;

                hasher.update(&frame.payload);

                bytes_received = next_total;
            }

            MessageType::Complete => {
                if frame.payload.len() != 32 {
                    let _ = fs::remove_file(&partial_destination).await;

                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "COMPLETE must contain a 32-byte BLAKE3 hash",
                    )
                    .into());
                }

                break frame.payload;
            }

            _ => {
                let _ = fs::remove_file(&partial_destination).await;

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected DATA or COMPLETE",
                )
                .into());
            }
        }
    };

    output.flush().await?;
    drop(output);

    if bytes_received != offer.file_size {
        let _ = fs::remove_file(&partial_destination).await;

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected {} bytes, received {}",
                offer.file_size, bytes_received
            ),
        )
        .into());
    }

    let receiver_hash = hasher.finalize();

    if sender_hash.as_slice() != receiver_hash.as_bytes() {
        let _ = fs::remove_file(&partial_destination).await;

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file integrity verification failed",
        )
        .into());
    }

    fs::rename(&partial_destination, &destination).await?;

    println!("Received {bytes_received} bytes");

    println!("BLAKE3 verification successful");

    let verified = Frame::new(MessageType::Verified, Vec::new());

    write_frame(&mut stream, &verified).await?;

    println!("Sent VERIFIED");

    println!("Saved to {}", destination.display());

    Ok(())
}

fn is_safe_filename(filename: &str) -> bool {
    if filename.is_empty() || filename.contains('/') || filename.contains('\\') {
        return false;
    }

    let mut components = Path::new(filename).components();

    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    )
}
