use std::error::Error;
use std::fmt;
use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::sleep;

use crate::progress::ProgressTracker;
use crate::protocol::frame::{MAX_DATA_PAYLOAD_LENGTH, WFP_VERSION};
use crate::protocol::{
    FileOffer, Frame, MessageType, ProtocolIoError, ResumeRequest, decode_reject, decode_resume,
    encode_offer, read_frame, write_frame,
};

const MAX_SEND_ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
enum SendAttemptError {
    RetryableConnect(io::Error),
    RetryableProtocol(ProtocolIoError),
    Permanent(Box<dyn Error>),
}

impl SendAttemptError {
    fn permanent<E>(error: E) -> Self
    where
        E: Error + 'static,
    {
        Self::Permanent(Box::new(error))
    }

    fn is_retryable(&self) -> bool {
        matches!(
            self,
            SendAttemptError::RetryableConnect(_) | SendAttemptError::RetryableProtocol(_)
        )
    }
}

impl fmt::Display for SendAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendAttemptError::RetryableConnect(error) => {
                write!(formatter, "{error}")
            }

            SendAttemptError::RetryableProtocol(error) => {
                write!(formatter, "{error}")
            }

            SendAttemptError::Permanent(error) => {
                write!(formatter, "{error}")
            }
        }
    }
}

impl Error for SendAttemptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SendAttemptError::RetryableConnect(error) => Some(error),
            SendAttemptError::RetryableProtocol(error) => Some(error),
            SendAttemptError::Permanent(error) => Some(error.as_ref()),
        }
    }
}

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

    let mut attempt = 1usize;

    loop {
        match send_once(path, &filename, file_size, address).await {
            Ok(()) => {
                return Ok(());
            }

            Err(error) if error.is_retryable() && attempt < MAX_SEND_ATTEMPTS => {
                println!();
                println!(
                    "Transfer attempt {attempt} failed with a recoverable network error: {error}"
                );
                println!("Retrying in {} second(s)...", RETRY_DELAY.as_secs());

                wait_before_retry().await?;

                attempt += 1;

                println!();
                println!("Retry attempt {attempt} of {MAX_SEND_ATTEMPTS}");
            }

            Err(error) => {
                if error.is_retryable() {
                    println!();
                    println!(
                        "Transfer failed after {attempt} attempts due to a recoverable network error"
                    );
                }

                return Err(Box::new(error));
            }
        }
    }
}

async fn wait_before_retry() -> Result<(), Box<dyn Error>> {
    tokio::select! {
        _ = sleep(RETRY_DELAY) => {
            Ok(())
        }

        signal_result = tokio::signal::ctrl_c() => {
            signal_result?;

            Err(
                io::Error::new(
                    io::ErrorKind::Interrupted,
                    "transfer cancelled by user",
                )
                .into(),
            )
        }
    }
}

async fn send_once(
    path: &Path,
    filename: &str,
    file_size: u64,
    address: &str,
) -> Result<(), SendAttemptError> {
    println!("Connecting to {address}");

    let mut stream = TcpStream::connect(address)
        .await
        .map_err(classify_connect_error)?;

    println!("Connected");

    let hello = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

    write_frame(&mut stream, &hello)
        .await
        .map_err(classify_protocol_error)?;

    println!("Sent HELLO (WFP/0.1)");

    let response = read_frame(&mut stream)
        .await
        .map_err(classify_protocol_error)?;

    if response.message_type != MessageType::HelloAck {
        return Err(SendAttemptError::permanent(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected HELLO_ACK",
        )));
    }

    if response.payload != vec![WFP_VERSION] {
        return Err(SendAttemptError::permanent(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid HELLO_ACK version",
        )));
    }

    println!("WFP handshake successful");

    let offer = FileOffer {
        filename: filename.to_string(),
        file_size,
    };

    let offer_payload = encode_offer(&offer).map_err(SendAttemptError::permanent)?;

    let offer_frame = Frame::new(MessageType::Offer, offer_payload);

    write_frame(&mut stream, &offer_frame)
        .await
        .map_err(classify_protocol_error)?;

    println!("Sent OFFER");

    let response = read_frame(&mut stream)
        .await
        .map_err(classify_protocol_error)?;

    let mut file = fs::File::open(path)
        .await
        .map_err(SendAttemptError::permanent)?;

    let mut hasher = blake3::Hasher::new();

    let resume_offset = match response.message_type {
        MessageType::Accept => {
            validate_empty_payload(&response, "ACCEPT")?;

            println!("Receiver accepted the file");

            0
        }

        MessageType::Reject => {
            let reject = decode_reject(&response.payload).map_err(SendAttemptError::permanent)?;

            return Err(SendAttemptError::permanent(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "receiver rejected the file [{}]: {}",
                    reject.code, reject.message
                ),
            )));
        }

        MessageType::Resume => {
            let resume = decode_resume(&response.payload).map_err(SendAttemptError::permanent)?;

            negotiate_resume(&mut stream, path, file_size, &mut file, &mut hasher, resume).await?
        }

        _ => {
            return Err(SendAttemptError::permanent(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected ACCEPT, REJECT, or RESUME",
            )));
        }
    };

    let remaining_size = file_size.checked_sub(resume_offset).ok_or_else(|| {
        SendAttemptError::permanent(io::Error::new(
            io::ErrorKind::InvalidData,
            "resume offset exceeds file size",
        ))
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
                signal_result.map_err(SendAttemptError::permanent)?;

                let cancel =
                    Frame::new(
                        MessageType::Cancel,
                        Vec::new(),
                    );

                write_frame(
                    &mut stream,
                    &cancel,
                )
                .await
                .map_err(SendAttemptError::permanent)?;

                println!();
                println!("Sent CANCEL");

                return Err(
                    SendAttemptError::permanent(
                        io::Error::new(
                            io::ErrorKind::Interrupted,
                            "transfer cancelled by user",
                        ),
                    ),
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

        let bytes_read = read_result.map_err(SendAttemptError::permanent)?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buffer[..bytes_read]);

        let data_frame = Frame::new(MessageType::Data, buffer[..bytes_read].to_vec());

        write_frame(&mut stream, &data_frame)
            .await
            .map_err(classify_protocol_error)?;

        bytes_processed = bytes_processed
            .checked_add(bytes_read as u64)
            .ok_or_else(|| {
                SendAttemptError::permanent(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sent byte count overflow",
                ))
            })?;

        network_bytes_sent = network_bytes_sent
            .checked_add(bytes_read as u64)
            .ok_or_else(|| {
                SendAttemptError::permanent(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "network byte count overflow",
                ))
            })?;

        progress.add(bytes_read);
    }

    if bytes_processed != file_size {
        return Err(SendAttemptError::permanent(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected to process {file_size} bytes, processed {bytes_processed}"),
        )));
    }

    progress.finish();

    if resume_offset > 0 {
        println!("Resumed from byte {resume_offset}");
    }

    println!("Sent {network_bytes_sent} bytes over the network");

    let digest = hasher.finalize();

    let complete = Frame::new(MessageType::Complete, digest.as_bytes().to_vec());

    write_frame(&mut stream, &complete)
        .await
        .map_err(classify_protocol_error)?;

    println!("Sent COMPLETE");

    let response = read_frame(&mut stream)
        .await
        .map_err(classify_protocol_error)?;

    if response.message_type != MessageType::Verified {
        return Err(SendAttemptError::permanent(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected VERIFIED",
        )));
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
) -> Result<u64, SendAttemptError> {
    if resume.offset > file_size {
        let restart = Frame::new(MessageType::Restart, Vec::new());

        write_frame(stream, &restart)
            .await
            .map_err(classify_protocol_error)?;

        println!("Receiver requested an invalid resume offset; requested restart");

        wait_for_restart_accept(stream).await?;

        *file = fs::File::open(path)
            .await
            .map_err(SendAttemptError::permanent)?;

        *hasher = blake3::Hasher::new();

        return Ok(0);
    }

    println!("Receiver requested resume from byte {}", resume.offset);

    hash_prefix(file, hasher, resume.offset).await?;

    let local_prefix_hash = hasher.clone().finalize();

    if local_prefix_hash.as_bytes() == &resume.prefix_hash {
        let accept = Frame::new(MessageType::Accept, Vec::new());

        write_frame(stream, &accept)
            .await
            .map_err(classify_protocol_error)?;

        println!("Resume prefix verified");
        println!("Sent ACCEPT for resume");

        return Ok(resume.offset);
    }

    println!("Resume prefix does not match the source file");

    let restart = Frame::new(MessageType::Restart, Vec::new());

    write_frame(stream, &restart)
        .await
        .map_err(classify_protocol_error)?;

    println!("Sent RESTART");

    wait_for_restart_accept(stream).await?;

    *file = fs::File::open(path)
        .await
        .map_err(SendAttemptError::permanent)?;

    *hasher = blake3::Hasher::new();

    println!("Restart accepted; sending from byte 0");

    Ok(0)
}

async fn hash_prefix(
    file: &mut fs::File,
    hasher: &mut blake3::Hasher,
    prefix_length: u64,
) -> Result<(), SendAttemptError> {
    let mut remaining = prefix_length;

    let mut buffer = vec![0u8; MAX_DATA_PAYLOAD_LENGTH];

    while remaining > 0 {
        let bytes_to_read = remaining.min(MAX_DATA_PAYLOAD_LENGTH as u64) as usize;

        let bytes_read = file
            .read(&mut buffer[..bytes_to_read])
            .await
            .map_err(SendAttemptError::permanent)?;

        if bytes_read == 0 {
            return Err(SendAttemptError::permanent(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source file ended before the requested resume offset",
            )));
        }

        hasher.update(&buffer[..bytes_read]);

        remaining -= bytes_read as u64;
    }

    Ok(())
}

async fn wait_for_restart_accept(stream: &mut TcpStream) -> Result<(), SendAttemptError> {
    let response = read_frame(stream).await.map_err(classify_protocol_error)?;

    match response.message_type {
        MessageType::Accept => {
            validate_empty_payload(&response, "ACCEPT")?;

            Ok(())
        }

        MessageType::Reject => {
            let reject = decode_reject(&response.payload).map_err(SendAttemptError::permanent)?;

            Err(SendAttemptError::permanent(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "receiver rejected restart [{}]: {}",
                    reject.code, reject.message
                ),
            )))
        }

        _ => Err(SendAttemptError::permanent(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected ACCEPT or REJECT after RESTART",
        ))),
    }
}

fn validate_empty_payload(frame: &Frame, message_name: &str) -> Result<(), SendAttemptError> {
    if !frame.payload.is_empty() {
        return Err(SendAttemptError::permanent(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{message_name} payload must be empty"),
        )));
    }

    Ok(())
}

fn classify_connect_error(error: io::Error) -> SendAttemptError {
    if is_retryable_network_kind(error.kind()) {
        SendAttemptError::RetryableConnect(error)
    } else {
        SendAttemptError::Permanent(Box::new(error))
    }
}

fn classify_protocol_error(error: ProtocolIoError) -> SendAttemptError {
    let retryable = match &error {
        ProtocolIoError::Io(io_error) => is_retryable_network_kind(io_error.kind()),
        ProtocolIoError::Decode(_) | ProtocolIoError::Encode(_) => false,
    };

    if retryable {
        SendAttemptError::RetryableProtocol(error)
    } else {
        SendAttemptError::Permanent(Box::new(error))
    }
}

fn is_retryable_network_kind(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WriteZero
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_expected_connect_failures_as_retryable() {
        let kinds = [
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::NotConnected,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::TimedOut,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::Interrupted,
            io::ErrorKind::WriteZero,
        ];

        for kind in kinds {
            let error = classify_connect_error(io::Error::from(kind));

            assert!(matches!(error, SendAttemptError::RetryableConnect(_)));
        }
    }

    #[test]
    fn keeps_non_transient_connect_failures_permanent() {
        let kinds = [
            io::ErrorKind::InvalidInput,
            io::ErrorKind::InvalidData,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists,
        ];

        for kind in kinds {
            let error = classify_connect_error(io::Error::from(kind));

            assert!(matches!(error, SendAttemptError::Permanent(_)));
        }
    }

    #[test]
    fn classifies_protocol_connection_loss_as_retryable() {
        let protocol_error = ProtocolIoError::Io(io::Error::from(io::ErrorKind::ConnectionReset));

        let error = classify_protocol_error(protocol_error);

        assert!(matches!(error, SendAttemptError::RetryableProtocol(_)));
    }

    #[test]
    fn keeps_non_transient_protocol_io_failure_permanent() {
        let protocol_error = ProtocolIoError::Io(io::Error::from(io::ErrorKind::PermissionDenied));

        let error = classify_protocol_error(protocol_error);

        assert!(matches!(error, SendAttemptError::Permanent(_)));
    }
}
