use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::path::Path;

use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::completion_receipt::{
    CompletionReceipt, completion_receipt_path, read_completion_receipt, write_completion_receipt,
};
use crate::discovery::{DISCOVERY_PORT, local_device_name, run_discovery_responder};
use crate::progress::ProgressTracker;
use crate::protocol::frame::{MAX_DATA_PAYLOAD_LENGTH, WFP_VERSION};
use crate::protocol::{
    FileOffer, FileReject, Frame, MessageType, ProtocolIoError, RejectCode, ResumeRequest,
    decode_offer, encode_reject, encode_resume, read_frame, write_frame,
};
use crate::receiver_paths::{final_path, is_safe_filename, partial_path, partials_directory};
use crate::transfer_metadata::{
    TransferMetadata, TransferState, read_transfer_metadata, remove_transfer_metadata,
    write_transfer_metadata,
};

struct PreparedTransfer {
    output: fs::File,
    bytes_received: u64,
    hasher: blake3::Hasher,
}

struct CompletedTransfer {
    bytes_received: u64,
    blake3: [u8; 32],
}

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

    println!("Received HELLO (WFP/0.2)");

    let hello_ack = Frame::new(MessageType::HelloAck, vec![WFP_VERSION]);

    write_frame(&mut stream, &hello_ack).await?;

    println!("Sent HELLO_ACK (WFP/0.2)");

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
    println!("Transfer ID: {}", offer.transfer_id);
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

    let destination = final_path(destination_directory, &offer.filename);
    let partial_destination = partial_path(destination_directory, &offer.filename);

    if reconcile_completed_transfer(
        &mut stream,
        destination_directory,
        &destination,
        &partial_destination,
        &offer,
    )
    .await?
    {
        return Ok(());
    }

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

    if let Err(error) = fs::create_dir_all(partials_directory(destination_directory)).await {
        send_reject(
            &mut stream,
            RejectCode::CannotPrepareDestination,
            "receiver could not prepare the partial directory",
        )
        .await?;
        return Err(error.into());
    }

    let prepared = prepare_transfer(&mut stream, &partial_destination, &offer).await?;

    let transfer_result = receive_file_data(
        &mut stream,
        prepared.output,
        offer.file_size,
        prepared.bytes_received,
        prepared.hasher,
    )
    .await;

    let completed = match transfer_result {
        Ok(completed) => completed,

        Err(error) => {
            if should_preserve_partial(error.as_ref()) {
                println!();
                println!(
                    "Transfer interrupted by connection loss; partial state preserved at {}",
                    partial_destination.display()
                );
            } else {
                let _ = discard_partial_state(&partial_destination).await;
            }

            return Err(error);
        }
    };

    let completion_receipt = CompletionReceipt {
        transfer_id: offer.transfer_id,
        filename: offer.filename.clone(),
        file_size: offer.file_size,
        blake3: completed.blake3,
    };

    if let Err(error) = write_completion_receipt(destination_directory, &completion_receipt).await {
        println!();
        println!(
            "Transfer data verified, but completion receipt could not be persisted; partial state preserved"
        );

        return Err(error.into());
    }

    println!("Persisted completion receipt");

    /*
     * Once the completion receipt exists, the partial file must
     * not be discarded if rename fails.
     *
     * A later connection can verify the receipt against this
     * complete .part and finish the commit.
     */
    if let Err(error) = fs::rename(&partial_destination, &destination).await {
        println!();
        println!(
            "Completion receipt persisted, but final rename failed; completed partial state preserved"
        );

        return Err(error.into());
    }

    if let Err(error) = remove_transfer_metadata(&partial_destination).await {
        eprintln!(
            "Warning: file was committed but transfer metadata could not be removed: {error}"
        );
    }

    println!("Received {} bytes", completed.bytes_received);

    println!("BLAKE3 verification successful");

    send_verified(&mut stream).await?;

    println!("Saved to {}", destination.display());

    println!();

    println!("Waiting for the next transfer...");

    Ok(())
}

async fn reconcile_completed_transfer(
    stream: &mut TcpStream,
    destination_directory: &Path,
    destination: &Path,
    partial_destination: &Path,
    offer: &FileOffer,
) -> Result<bool, Box<dyn Error>> {
    let receipt_path = completion_receipt_path(destination_directory, offer.transfer_id);

    if !fs::try_exists(&receipt_path).await? {
        return Ok(false);
    }

    let receipt = match read_completion_receipt(destination_directory, offer.transfer_id).await {
        Ok(receipt) => receipt,

        Err(error) => {
            send_reject(
                stream,
                RejectCode::CannotPrepareDestination,
                "receiver could not read the completion receipt",
            )
            .await?;

            return Err(error.into());
        }
    };

    if receipt.filename != offer.filename || receipt.file_size != offer.file_size {
        send_reject(
            stream,
            RejectCode::CannotPrepareDestination,
            "transfer identity conflicts with the completion receipt",
        )
        .await?;

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "transfer identity conflicts with the completion receipt",
        )
        .into());
    }

    if fs::try_exists(destination).await? {
        if let Err(error) = verify_completed_data(destination, &receipt).await {
            send_reject(
                stream,
                RejectCode::CannotPrepareDestination,
                "completed destination does not match the completion receipt",
            )
            .await?;

            return Err(error);
        }

        if let Err(error) = discard_partial_state(partial_destination).await {
            eprintln!(
                "Warning: completed transfer was verified but stale partial state could not be removed: {error}"
            );
        }

        println!(
            "Reconciled completed transfer {} from final file",
            offer.transfer_id
        );

        send_verified(stream).await?;

        println!("Saved to {}", destination.display());

        return Ok(true);
    }

    if fs::try_exists(partial_destination).await? {
        if let Err(error) = verify_completed_data(partial_destination, &receipt).await {
            send_reject(
                stream,
                RejectCode::CannotPrepareDestination,
                "completed partial file does not match the completion receipt",
            )
            .await?;

            return Err(error);
        }

        if let Err(error) = fs::rename(partial_destination, destination).await {
            return Err(error.into());
        }

        if let Err(error) = remove_transfer_metadata(partial_destination).await {
            eprintln!(
                "Warning: completed partial was committed but transfer metadata could not be removed: {error}"
            );
        }

        println!(
            "Reconciled completed transfer {} from completed partial file",
            offer.transfer_id
        );

        send_verified(stream).await?;

        println!("Saved to {}", destination.display());

        return Ok(true);
    }

    send_reject(
        stream,
        RejectCode::CannotPrepareDestination,
        "completion receipt exists but completed transfer data is missing",
    )
    .await?;

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "completion receipt exists but completed transfer data is missing",
    )
    .into())
}

async fn verify_completed_data(
    path: &Path,
    receipt: &CompletionReceipt,
) -> Result<(), Box<dyn Error>> {
    let metadata = fs::metadata(path).await?;

    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "completed transfer data is not a regular file",
        )
        .into());
    }

    if metadata.len() != receipt.file_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "completed transfer size does not match the completion receipt",
        )
        .into());
    }

    let hasher = hash_file_exact(path, receipt.file_size).await?;

    let digest = hasher.finalize();

    if digest.as_bytes() != &receipt.blake3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "completed transfer hash does not match the completion receipt",
        )
        .into());
    }

    Ok(())
}

async fn prepare_transfer(
    stream: &mut TcpStream,
    partial_destination: &Path,
    offer: &FileOffer,
) -> Result<PreparedTransfer, Box<dyn Error>> {
    let expected_metadata = transfer_metadata_for_offer(offer);

    if !fs::try_exists(partial_destination).await? {
        return prepare_fresh_transfer(stream, partial_destination, &expected_metadata).await;
    }

    let partial_metadata = match fs::metadata(partial_destination).await {
        Ok(metadata) => metadata,

        Err(error) => {
            send_reject(
                stream,
                RejectCode::CannotPrepareDestination,
                "receiver could not inspect the partial file",
            )
            .await?;

            return Err(error.into());
        }
    };

    if !partial_metadata.is_file() {
        send_reject(
            stream,
            RejectCode::CannotPrepareDestination,
            "partial destination is not a regular file",
        )
        .await?;

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "partial destination is not a regular file",
        )
        .into());
    }

    let partial_size = partial_metadata.len();

    if partial_size == 0 || partial_size > offer.file_size {
        if let Err(error) = discard_partial_state(partial_destination).await {
            send_reject(
                stream,
                RejectCode::CannotPrepareDestination,
                "receiver could not replace an unusable partial file",
            )
            .await?;

            return Err(error);
        }

        return prepare_fresh_transfer(stream, partial_destination, &expected_metadata).await;
    }

    let stored_metadata_matches = match read_transfer_metadata(partial_destination).await {
        Ok(stored_metadata) => {
            if stored_metadata == expected_metadata {
                println!("Existing partial belongs to transfer {}", offer.transfer_id);

                true
            } else {
                println!(
                    "Existing partial metadata does not match transfer {}; prefix proof required",
                    offer.transfer_id
                );

                false
            }
        }

        Err(error) => {
            println!(
                "Existing partial metadata is unavailable or invalid ({error}); prefix proof required"
            );

            false
        }
    };

    let hasher = match hash_file_exact(partial_destination, partial_size).await {
        Ok(hasher) => hasher,

        Err(error) => {
            send_reject(
                stream,
                RejectCode::CannotPrepareDestination,
                "receiver could not verify the existing partial file",
            )
            .await?;

            return Err(error);
        }
    };

    let prefix_digest = hasher.clone().finalize();

    let resume_request = ResumeRequest {
        offset: partial_size,
        prefix_hash: *prefix_digest.as_bytes(),
    };

    let resume_payload = encode_resume(&resume_request);

    let resume = Frame::new(MessageType::Resume, resume_payload);

    if let Err(error) = write_frame(stream, &resume).await {
        if !should_preserve_partial(&error) {
            let _ = discard_partial_state(partial_destination).await;
        }

        return Err(error.into());
    }

    println!("Sent RESUME (offset: {partial_size} bytes)");

    let response = match read_frame(stream).await {
        Ok(response) => response,

        Err(error) => {
            if !should_preserve_partial(&error) {
                let _ = discard_partial_state(partial_destination).await;
            }

            return Err(error.into());
        }
    };

    match response.message_type {
        MessageType::Accept => {
            if !response.payload.is_empty() {
                let _ = discard_partial_state(partial_destination).await;

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ACCEPT payload must be empty",
                )
                .into());
            }

            let current_size = fs::metadata(partial_destination).await?.len();

            if current_size != partial_size {
                let _ = discard_partial_state(partial_destination).await;

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "partial file changed while resume was being negotiated",
                )
                .into());
            }

            if !stored_metadata_matches {
                write_transfer_metadata(partial_destination, &expected_metadata).await?;

                println!(
                    "Adopted verified partial for transfer {}",
                    offer.transfer_id
                );
            }

            let output = OpenOptions::new()
                .append(true)
                .open(partial_destination)
                .await?;

            println!("Resume accepted at byte {partial_size}");

            Ok(PreparedTransfer {
                output,
                bytes_received: partial_size,
                hasher,
            })
        }

        MessageType::Restart => {
            if !response.payload.is_empty() {
                let _ = discard_partial_state(partial_destination).await;

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESTART payload must be empty",
                )
                .into());
            }

            println!("Sender rejected the partial file; restarting from byte 0");

            if let Err(error) = discard_partial_state(partial_destination).await {
                send_reject(
                    stream,
                    RejectCode::CannotPrepareDestination,
                    "receiver could not discard the rejected partial file",
                )
                .await?;

                return Err(error);
            }

            prepare_fresh_transfer(stream, partial_destination, &expected_metadata).await
        }

        MessageType::Cancel => {
            if !response.payload.is_empty() {
                let _ = discard_partial_state(partial_destination).await;

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CANCEL payload must be empty",
                )
                .into());
            }

            let _ = discard_partial_state(partial_destination).await;

            Err(io::Error::new(io::ErrorKind::Interrupted, "transfer cancelled by sender").into())
        }

        _ => {
            let _ = discard_partial_state(partial_destination).await;

            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected ACCEPT, RESTART, or CANCEL after RESUME",
            )
            .into())
        }
    }
}

async fn prepare_fresh_transfer(
    stream: &mut TcpStream,
    partial_destination: &Path,
    metadata: &TransferMetadata,
) -> Result<PreparedTransfer, Box<dyn Error>> {
    if let Err(error) = remove_transfer_metadata(partial_destination).await {
        send_reject(
            stream,
            RejectCode::CannotPrepareDestination,
            "receiver could not clear stale transfer metadata",
        )
        .await?;

        return Err(error.into());
    }

    let output = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(partial_destination)
        .await
    {
        Ok(file) => file,

        Err(error) => {
            send_reject(
                stream,
                RejectCode::CannotPrepareDestination,
                "receiver could not create the destination file",
            )
            .await?;

            return Err(error.into());
        }
    };

    if let Err(error) = write_transfer_metadata(partial_destination, metadata).await {
        drop(output);

        let _ = discard_partial_state(partial_destination).await;

        send_reject(
            stream,
            RejectCode::CannotPrepareDestination,
            "receiver could not persist transfer metadata",
        )
        .await?;

        return Err(error.into());
    }

    let accept = Frame::new(MessageType::Accept, Vec::new());

    if let Err(error) = write_frame(stream, &accept).await {
        drop(output);

        let _ = discard_partial_state(partial_destination).await;

        return Err(error.into());
    }

    println!("Persisted transfer metadata");
    println!("Sent ACCEPT");

    Ok(PreparedTransfer {
        output,
        bytes_received: 0,
        hasher: blake3::Hasher::new(),
    })
}

fn transfer_metadata_for_offer(offer: &FileOffer) -> TransferMetadata {
    TransferMetadata {
        transfer_id: offer.transfer_id,
        filename: offer.filename.clone(),
        file_size: offer.file_size,
        state: TransferState::Partial,
    }
}

async fn discard_partial_state(partial_destination: &Path) -> Result<(), Box<dyn Error>> {
    match fs::remove_file(partial_destination).await {
        Ok(()) => {}

        Err(error) if error.kind() == io::ErrorKind::NotFound => {}

        Err(error) => {
            return Err(error.into());
        }
    }

    remove_transfer_metadata(partial_destination).await?;

    Ok(())
}

async fn hash_file_exact(
    path: &Path,
    expected_size: u64,
) -> Result<blake3::Hasher, Box<dyn Error>> {
    let mut file = fs::File::open(path).await?;

    let mut hasher = blake3::Hasher::new();

    let mut buffer = vec![0u8; MAX_DATA_PAYLOAD_LENGTH];

    let mut bytes_hashed: u64 = 0;

    loop {
        let bytes_read = file.read(&mut buffer).await?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);

        bytes_hashed = bytes_hashed.checked_add(bytes_read as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "file byte count overflow")
        })?;
    }

    if bytes_hashed != expected_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file changed while it was being hashed",
        )
        .into());
    }

    Ok(hasher)
}

async fn receive_file_data(
    stream: &mut TcpStream,
    mut output: fs::File,
    expected_size: u64,
    initial_bytes_received: u64,
    mut hasher: blake3::Hasher,
) -> Result<CompletedTransfer, Box<dyn Error>> {
    if initial_bytes_received > expected_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "resume offset exceeds the announced file size",
        )
        .into());
    }

    let mut bytes_received = initial_bytes_received;

    let remaining_size = expected_size - initial_bytes_received;

    let mut progress = ProgressTracker::new("Receiving", remaining_size);

    let sender_hash = loop {
        let frame = match read_frame(stream).await {
            Ok(frame) => frame,
            Err(error) => {
                if should_preserve_partial(&error) {
                    output.flush().await?;
                }
                return Err(error.into());
            }
        };

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

    /*
     * The complete partial file must be persisted before the
     * completion receipt is allowed to become durable.
     */
    output.sync_all().await?;

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

    Ok(CompletedTransfer {
        bytes_received,
        blake3: *receiver_hash.as_bytes(),
    })
}

async fn send_verified(stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let verified = Frame::new(MessageType::Verified, Vec::new());

    write_frame(stream, &verified).await?;

    println!("Sent VERIFIED");

    Ok(())
}

fn should_preserve_partial(error: &(dyn Error + 'static)) -> bool {
    let Some(protocol_error) = error.downcast_ref::<ProtocolIoError>() else {
        return false;
    };

    let ProtocolIoError::Io(io_error) = protocol_error else {
        return false;
    };

    matches!(
        io_error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
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
