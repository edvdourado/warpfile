use std::error::Error;
use std::fmt;
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::decoder::{DecodeError, decode_frame};

use super::encoder::{EncodeError, encode_frame};

use super::frame::{Frame, HEADER_LENGTH, MAX_DATA_PAYLOAD_LENGTH, MAX_PAYLOAD_LENGTH};

use super::message::MessageType;

#[derive(Debug)]
pub enum ProtocolIoError {
    Io(io::Error),
    Decode(DecodeError),
    Encode(EncodeError),
}

impl fmt::Display for ProtocolIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolIoError::Io(error) => {
                write!(f, "I/O error: {error}")
            }

            ProtocolIoError::Decode(error) => {
                write!(f, "decode error: {error}")
            }

            ProtocolIoError::Encode(error) => {
                write!(f, "encode error: {error}")
            }
        }
    }
}

impl Error for ProtocolIoError {}

impl From<io::Error> for ProtocolIoError {
    fn from(error: io::Error) -> Self {
        ProtocolIoError::Io(error)
    }
}

impl From<DecodeError> for ProtocolIoError {
    fn from(error: DecodeError) -> Self {
        ProtocolIoError::Decode(error)
    }
}

impl From<EncodeError> for ProtocolIoError {
    fn from(error: EncodeError) -> Self {
        ProtocolIoError::Encode(error)
    }
}

pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> Result<(), ProtocolIoError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = encode_frame(frame)?;

    writer.write_all(&bytes).await?;
    writer.flush().await?;

    Ok(())
}

pub async fn read_frame<R>(reader: &mut R) -> Result<Frame, ProtocolIoError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; HEADER_LENGTH];

    reader.read_exact(&mut header).await?;

    let payload_length =
        u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;

    if payload_length > MAX_PAYLOAD_LENGTH {
        return Err(DecodeError::PayloadTooLarge(payload_length).into());
    }

    let message_type = MessageType::try_from(header[5]).map_err(DecodeError::UnknownMessageType)?;

    if message_type == MessageType::Data && payload_length > MAX_DATA_PAYLOAD_LENGTH {
        return Err(DecodeError::DataPayloadTooLarge(payload_length).into());
    }

    let mut bytes = Vec::with_capacity(HEADER_LENGTH + payload_length);

    bytes.extend_from_slice(&header);

    if payload_length > 0 {
        let mut payload = vec![0u8; payload_length];

        reader.read_exact(&mut payload).await?;

        bytes.extend_from_slice(&payload);
    }

    Ok(decode_frame(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::protocol::frame::WFP_VERSION;

    use tokio::io::duplex;

    #[tokio::test]
    async fn writes_and_reads_one_frame() {
        let (mut side_a, mut side_b) = duplex(1024);

        let original = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

        write_frame(&mut side_a, &original).await.unwrap();

        let received = read_frame(&mut side_b).await.unwrap();

        assert_eq!(received, original);
    }

    #[tokio::test]
    async fn keeps_two_frames_separate() {
        let (mut side_a, mut side_b) = duplex(1024);

        let first = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

        let second = Frame::new(MessageType::Accept, Vec::new());

        write_frame(&mut side_a, &first).await.unwrap();

        write_frame(&mut side_a, &second).await.unwrap();

        let received_first = read_frame(&mut side_b).await.unwrap();

        let received_second = read_frame(&mut side_b).await.unwrap();

        assert_eq!(received_first, first);

        assert_eq!(received_second, second);
    }
}
