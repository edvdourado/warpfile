use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::path::{Component, Path};

use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::discovery::{DISCOVERY_PORT, local_device_name, run_discovery_responder};
use crate::progress::ProgressTracker;
use crate::protocol::frame::WFP_VERSION;
use crate::protocol::{
    FileReject, Frame, MessageType, RejectCode, decode_offer, encode_reject, read_frame,
    write_frame,
};

pub async fn run_receiver(address: &str) -> Result<(), Box<dyn Error>> {
    println!("WarpFile Receiver");

    let listener = TcpListener::bind(address).await?;

    let tcp_port = listener.local_addr()?.port();

    println!("Listening on {address}");

    let discovery_socket = UdpSocket::bind(("0.0.0.0", DISCOVERY_PORT)).await?;

    let device_name = local_device_name();

    println!("Discovery: {device_name} on UDP 0.0.0.0:{DISCOVERY_PORT}");

    let receiver = receive_loop(listener, Path::new("received"));

    let discovery = run_discovery_responder(&discovery_socket, &device_name, tcp_port);

    tokio::pin!(receiver);
    tokio::pin!(discovery);

    tokio::select! {
        result = &mut receiver => {
            result
        }

        result = &mut discovery => {
            result
        }
    }
}

pub async fn receive_loop(
    listener: TcpListener,
    destination_directory: &Path,
) -> Result<(), Box<dyn Error>> {
    loop {
        let (stream, peer_address) = listener.accept().await?;

        if let Err(error) = receive_connection(stream, peer_address, destination_directory).await {
            eprintln!("Transfer from {peer_address} failed: {error}");
        }
    }
}

pub async fn receive_once(
    listener: TcpListener,
    destination_directory: &Path,
) -> Result<(), Box<dyn Error>> {
    let (stream, peer_address) = listener.accept().await?;

    receive_connection(stream, peer_address, destination_directory).await
}

async fn receive_connection(
    mut stream: TcpStream,
    peer_address: SocketAddr,
    destination_directory: &Path,
) -> Result<(), Box<dyn Error>> {
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
        send_reject(
            &mut stream,
            RejectCode::UnsafeFilename,
            "receiver rejected an unsafe filename",
        )
        .await?;

        return Err(io::Error::new(io::ErrorKind::InvalidData, "unsafe filename").into());
    }

    println!();
    println!("Incoming file:");
    println!("Name: {}", offer.filename);
    println!("Size: {} bytes", offer.file_size);
    println!();

    if let Err(error) = fs::create_dir_all(destination_directory).await {
        send_reject(
            &mut stream,
            RejectCode::CannotPrepareDestination,
            "receiver could not prepare the destination",
        )
        .await?;

        return Err(error.into());
    }

    let destination = destination_directory.join(&offer.filename);

    let partial_name = format!("{}.part", offer.filename);

    let partial_destination = destination_directory.join(partial_name);

    if fs::try_exists(&destination).await? {
        send_reject(
            &mut stream,
            RejectCode::FileExists,
            "destination file already exists",
        )
        .await?;

        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "destination file already exists",
        )
        .into());
    }

    if fs::try_exists(&partial_destination).await? {
        if let Err(error) = fs::remove_file(&partial_destination).await {
            send_reject(
                &mut stream,
                RejectCode::CannotPrepareDestination,
                "receiver could not remove an old partial file",
            )
            .await?;

            return Err(error.into());
        }
    }

    let output = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial_destination)
        .await
    {
        Ok(file) => file,

        Err(error) => {
            send_reject(
                &mut stream,
                RejectCode::CannotPrepareDestination,
                "receiver could not create the destination file",
            )
            .await?;

            return Err(error.into());
        }
    };

    let accept = Frame::new(MessageType::Accept, Vec::new());

    if let Err(error) = write_frame(&mut stream, &accept).await {
        drop(output);

        let _ = fs::remove_file(&partial_destination).await;

        return Err(error.into());
    }

    println!("Sent ACCEPT");

    let transfer_result = receive_file_data(&mut stream, output, offer.file_size).await;

    let bytes_received = match transfer_result {
        Ok(bytes_received) => bytes_received,

        Err(error) => {
            let _ = fs::remove_file(&partial_destination).await;

            return Err(error);
        }
    };

    if let Err(error) = fs::rename(&partial_destination, &destination).await {
        let _ = fs::remove_file(&partial_destination).await;

        return Err(error.into());
    }

    println!("Received {bytes_received} bytes");

    println!("BLAKE3 verification successful");

    let verified = Frame::new(MessageType::Verified, Vec::new());

    write_frame(&mut stream, &verified).await?;

    println!("Sent VERIFIED");

    println!("Saved to {}", destination.display());

    println!();

    println!("Waiting for the next transfer...");

    Ok(())
}

async fn receive_file_data(
    stream: &mut TcpStream,
    mut output: fs::File,
    expected_size: u64,
) -> Result<u64, Box<dyn Error>> {
    let mut hasher = blake3::Hasher::new();

    let mut bytes_received: u64 = 0;

    let mut progress = ProgressTracker::new("Receiving", expected_size);

    let sender_hash = loop {
        let frame = read_frame(stream).await?;

        match frame.message_type {
            MessageType::Data => {
                let chunk_size = frame.payload.len() as u64;

                let next_total = bytes_received.checked_add(chunk_size).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "received byte count overflow")
                })?;

                if next_total > expected_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received more bytes than announced",
                    )
                    .into());
                }

                output.write_all(&frame.payload).await?;

                hasher.update(&frame.payload);

                bytes_received = next_total;

                progress.add(frame.payload.len());
            }

            MessageType::Complete => {
                if frame.payload.len() != 32 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "COMPLETE must contain a 32-byte BLAKE3 hash",
                    )
                    .into());
                }

                break frame.payload;
            }

            MessageType::Cancel => {
                if !frame.payload.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "CANCEL payload must be empty",
                    )
                    .into());
                }

                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "transfer cancelled by sender",
                )
                .into());
            }

            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected DATA, COMPLETE, or CANCEL",
                )
                .into());
            }
        }
    };

    output.flush().await?;

    drop(output);

    if bytes_received != expected_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected {expected_size} bytes, received {bytes_received}"),
        )
        .into());
    }

    let receiver_hash = hasher.finalize();

    if sender_hash.as_slice() != receiver_hash.as_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file integrity verification failed",
        )
        .into());
    }

    progress.finish();

    Ok(bytes_received)
}

async fn send_reject(
    stream: &mut TcpStream,
    code: RejectCode,
    message: &str,
) -> Result<(), Box<dyn Error>> {
    let reject = FileReject {
        code,
        message: message.to_string(),
    };

    let payload = encode_reject(&reject)?;

    let frame = Frame::new(MessageType::Reject, payload);

    write_frame(stream, &frame).await?;

    println!("Sent REJECT ({code})");

    Ok(())
}

fn is_safe_filename(filename: &str) -> bool {
    if filename.is_empty() || filename.contains('/') || filename.contains('\\') {
        return false;
    }

    let mut components = Path::new(filename).components();

    matches!(
        (components.next(), components.next(),),
        (Some(Component::Normal(_)), None,)
    )
}
