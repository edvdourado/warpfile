use std::error::Error;
use std::fmt;
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::decoder::{DecodeError, decode_frame_for_version, validate_frame_header};

use super::encoder::{EncodeError, encode_frame};

use super::frame::{ACTIVE_WFP_VERSION, Frame, HEADER_LENGTH};

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
    read_frame_for_version(reader, ACTIVE_WFP_VERSION).await
}

pub async fn read_frame_for_version<R>(
    reader: &mut R,
    version: u8,
) -> Result<Frame, ProtocolIoError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; HEADER_LENGTH];

    reader.read_exact(&mut header).await?;

    let frame_header = validate_frame_header(&header, version)?;

    let mut bytes = Vec::with_capacity(HEADER_LENGTH + frame_header.payload_length);

    bytes.extend_from_slice(&header);

    if frame_header.payload_length > 0 {
        let mut payload = vec![0u8; frame_header.payload_length];

        reader.read_exact(&mut payload).await?;

        bytes.extend_from_slice(&payload);
    }

    Ok(decode_frame_for_version(&bytes, version)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::protocol::frame::{WFP_VERSION, WFP_VERSION_V02, WFP_VERSION_V03};
    use crate::protocol::message::MessageType;

    use tokio::io::{AsyncWriteExt, duplex};

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

    #[tokio::test]
    async fn reads_wfp_v03_only_when_explicitly_requested() {
        let (mut side_a, mut side_b) = duplex(1024);
        let frame =
            Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkHashes, Vec::new()).unwrap();

        write_frame(&mut side_a, &frame).await.unwrap();
        assert_eq!(
            read_frame_for_version(&mut side_b, WFP_VERSION_V03)
                .await
                .unwrap(),
            frame
        );

        let (mut side_a, mut side_b) = duplex(1024);
        write_frame(&mut side_a, &frame).await.unwrap();
        assert!(matches!(
            read_frame(&mut side_b).await,
            Err(ProtocolIoError::Decode(DecodeError::UnsupportedVersion(
                WFP_VERSION_V03
            )))
        ));
    }

    #[tokio::test]
    async fn rejects_invalid_headers_before_reading_the_declared_payload() {
        for (version, message_type, expected_error) in [
            (0x04, 0x01, DecodeError::UnsupportedVersion(0x04)),
            (WFP_VERSION_V02, 0x15, DecodeError::UnknownMessageType(0x15)),
        ] {
            let (mut writer, mut reader) = duplex(HEADER_LENGTH);
            let header = [b'W', b'F', b'P', 0, version, message_type, 0, 0, 0, 0, 0, 1];
            writer.write_all(&header).await.unwrap();
            drop(writer);

            let result = read_frame(&mut reader).await;
            assert!(
                matches!(result, Err(ProtocolIoError::Decode(error)) if error == expected_error)
            );
        }
    }
}
