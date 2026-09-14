use std::error::Error;
use std::io;
use std::path::Path;

use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::progress::ProgressTracker;
use crate::protocol::frame::{MAX_DATA_PAYLOAD_LENGTH, WFP_VERSION};
use crate::protocol::{
    FileOffer, Frame, MessageType, ResumeRequest, decode_reject, decode_resume, encode_offer,
    read_frame, write_frame,
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

    let mut file = fs::File::open(path).await?;

    let mut hasher = blake3::Hasher::new();

    let resume_offset = match response.message_type {
        MessageType::Accept => {
            validate_empty_payload(&response, "ACCEPT")?;

            println!("Receiver accepted the file");

            0
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

        MessageType::Resume => {
            let resume = decode_resume(&response.payload)?;

            negotiate_resume(&mut stream, path, file_size, &mut file, &mut hasher, resume).await?
        }

        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected ACCEPT, REJECT, or RESUME",
            )
            .into());
        }
    };

    let remaining_size = file_size.checked_sub(resume_offset).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "resume offset exceeds file size",
        )
    })?;

    let mut buffer = vec![0u8; MAX_DATA_PAYLOAD_LENGTH];

    let mut bytes_processed = resume_offset;

    let mut network_bytes_sent: u64 = 0;

    let mut progress = ProgressTracker::new("Sending", remaining_size);

    let cancel_signal = tokio::signal::ctrl_c();

    tokio::pin!(cancel_signal);

    loop {
        let read_result = tokio::select! {
            signal_result =
                &mut cancel_signal =>
            {
                signal_result?;

                let cancel =
                    Frame::new(
                        MessageType::Cancel,
                        Vec::new(),
                    );

                write_frame(
                    &mut stream,
                    &cancel,
                )
                .await?;

                println!();
                println!("Sent CANCEL");

                return Err(
                    io::Error::new(
                        io::ErrorKind::Interrupted,
                        "transfer cancelled by user",
                    )
                    .into(),
                );
            }

            read_result =
                file.read(
                    &mut buffer,
                ) =>
            {
                read_result
            }
        };

        let bytes_read = read_result?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);

        let data_frame = Frame::new(MessageType::Data, buffer[..bytes_read].to_vec());

        write_frame(&mut stream, &data_frame).await?;

        bytes_processed = bytes_processed
            .checked_add(bytes_read as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "sent byte count overflow")
            })?;

        network_bytes_sent = network_bytes_sent
            .checked_add(bytes_read as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "network byte count overflow")
            })?;

        progress.add(bytes_read);
    }

    if bytes_processed != file_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected to process {file_size} bytes, processed {bytes_processed}"),
        )
        .into());
    }

    progress.finish();

    if resume_offset > 0 {
        println!("Resumed from byte {resume_offset}");
    }

    println!("Sent {network_bytes_sent} bytes over the network");

    let digest = hasher.finalize();

    let complete = Frame::new(MessageType::Complete, digest.as_bytes().to_vec());

    write_frame(&mut stream, &complete).await?;

    println!("Sent COMPLETE");

    let response = read_frame(&mut stream).await?;

    if response.message_type != MessageType::Verified {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected VERIFIED").into());
    }

    validate_empty_payload(&response, "VERIFIED")?;

    println!("Received VERIFIED");
    println!("Transfer successful");

    Ok(())
}

async fn negotiate_resume(
    stream: &mut TcpStream,
    path: &Path,
    file_size: u64,
    file: &mut fs::File,
    hasher: &mut blake3::Hasher,
    resume: ResumeRequest,
) -> Result<u64, Box<dyn Error>> {
    if resume.offset > file_size {
        let restart = Frame::new(MessageType::Restart, Vec::new());

        write_frame(stream, &restart).await?;

        println!("Receiver requested an invalid resume offset; requested restart");

        wait_for_restart_accept(stream).await?;

        *file = fs::File::open(path).await?;

        *hasher = blake3::Hasher::new();

        return Ok(0);
    }

    println!("Receiver requested resume from byte {}", resume.offset);

    hash_prefix(file, hasher, resume.offset).await?;

    let local_prefix_hash = hasher.clone().finalize();

    if local_prefix_hash.as_bytes() == &resume.prefix_hash {
        let accept = Frame::new(MessageType::Accept, Vec::new());

        write_frame(stream, &accept).await?;

        println!("Resume prefix verified");

        println!("Sent ACCEPT for resume");

        return Ok(resume.offset);
    }

    println!("Resume prefix does not match the source file");

    let restart = Frame::new(MessageType::Restart, Vec::new());

    write_frame(stream, &restart).await?;

    println!("Sent RESTART");

    wait_for_restart_accept(stream).await?;

    *file = fs::File::open(path).await?;

    *hasher = blake3::Hasher::new();

    println!("Restart accepted; sending from byte 0");

    Ok(0)
}

async fn hash_prefix(
    file: &mut fs::File,
    hasher: &mut blake3::Hasher,
    prefix_length: u64,
) -> Result<(), Box<dyn Error>> {
    let mut remaining = prefix_length;

    let mut buffer = vec![0u8; MAX_DATA_PAYLOAD_LENGTH];

    while remaining > 0 {
        let bytes_to_read = remaining.min(MAX_DATA_PAYLOAD_LENGTH as u64) as usize;

        let bytes_read = file.read(&mut buffer[..bytes_to_read]).await?;

        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source file ended before the requested resume offset",
            )
            .into());
        }

        hasher.update(&buffer[..bytes_read]);

        remaining -= bytes_read as u64;
    }

    Ok(())
}

async fn wait_for_restart_accept(stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let response = read_frame(stream).await?;

    match response.message_type {
        MessageType::Accept => {
            validate_empty_payload(&response, "ACCEPT")?;

            Ok(())
        }

        MessageType::Reject => {
            let reject = decode_reject(&response.payload)?;

            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "receiver rejected restart [{}]: {}",
                    reject.code, reject.message
                ),
            )
            .into())
        }

        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected ACCEPT or REJECT after RESTART",
        )
        .into()),
    }
}

fn validate_empty_payload(frame: &Frame, message_name: &str) -> Result<(), Box<dyn Error>> {
    if !frame.payload.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{message_name} payload must be empty"),
        )
        .into());
    }

    Ok(())
}
