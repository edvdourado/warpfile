use std::error::Error;
use std::fmt;

use super::frame::{
    Frame, FrameError, HEADER_LENGTH, MAX_DATA_PAYLOAD_LENGTH, MAX_PAYLOAD_LENGTH, WFP_MAGIC,
    validate_version_and_message_type,
};
use super::message::MessageType;

#[derive(Debug, PartialEq, Eq)]
pub enum EncodeError {
    InvalidFrame(FrameError),
    PayloadTooLarge(usize),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::InvalidFrame(error) => write!(f, "invalid frame: {error}"),
            EncodeError::PayloadTooLarge(size) => {
                write!(f, "payload is too large: {size} bytes")
            }
        }
    }
}

impl Error for EncodeError {}

pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, EncodeError> {
    validate_version_and_message_type(frame.version, frame.message_type)
        .map_err(EncodeError::InvalidFrame)?;

    let payload_length = frame.payload.len();

    if frame.message_type == MessageType::Data && payload_length > MAX_DATA_PAYLOAD_LENGTH {
        return Err(EncodeError::PayloadTooLarge(payload_length));
    }

    if payload_length > MAX_PAYLOAD_LENGTH {
        return Err(EncodeError::PayloadTooLarge(payload_length));
    }

    let payload_length_u32 =
        u32::try_from(payload_length).map_err(|_| EncodeError::PayloadTooLarge(payload_length))?;

    let mut bytes = Vec::with_capacity(HEADER_LENGTH + payload_length);

    bytes.extend_from_slice(&WFP_MAGIC);
    bytes.push(frame.version);
    bytes.push(frame.message_type as u8);
    bytes.extend_from_slice(&frame.flags.to_be_bytes());
    bytes.extend_from_slice(&payload_length_u32.to_be_bytes());
    bytes.extend_from_slice(&frame.payload);

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::{FrameError, WFP_VERSION, WFP_VERSION_V02, WFP_VERSION_V03};
    use crate::protocol::message::MessageType;

    #[test]
    fn encodes_hello_frame_exactly_as_specified() {
        let frame = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

        let encoded = encode_frame(&frame).unwrap();

        let expected = vec![
            0x57, 0x46, 0x50, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
        ];

        assert_eq!(encoded, expected);
    }

    #[test]
    fn encodes_empty_payload() {
        let frame = Frame::new(MessageType::Accept, Vec::new());

        let encoded = encode_frame(&frame).unwrap();

        let expected = vec![
            0x57, 0x46, 0x50, 0x00, 0x02, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(encoded, expected);
    }

    #[test]
    fn rejects_payload_larger_than_protocol_limit() {
        let payload = vec![0u8; MAX_PAYLOAD_LENGTH + 1];

        let frame = Frame::new(MessageType::Offer, payload);

        let result = encode_frame(&frame);

        assert_eq!(
            result,
            Err(EncodeError::PayloadTooLarge(MAX_PAYLOAD_LENGTH + 1))
        );
    }

    #[test]
    fn rejects_data_payload_larger_than_data_limit() {
        let payload = vec![0u8; MAX_DATA_PAYLOAD_LENGTH + 1];

        let frame = Frame::new(MessageType::Data, payload);

        let result = encode_frame(&frame);

        assert_eq!(
            result,
            Err(EncodeError::PayloadTooLarge(MAX_DATA_PAYLOAD_LENGTH + 1))
        );
    }

    #[test]
    fn rejects_chunk_hashes_for_wfp_v02_and_encodes_it_for_wfp_v03() {
        let invalid = Frame {
            version: WFP_VERSION_V02,
            message_type: MessageType::ChunkHashes,
            flags: 0,
            payload: Vec::new(),
        };
        assert_eq!(
            encode_frame(&invalid),
            Err(EncodeError::InvalidFrame(
                FrameError::MessageTypeNotAllowed {
                    version: WFP_VERSION_V02,
                    message_type: MessageType::ChunkHashes,
                }
            ))
        );

        let encoded = encode_frame(
            &Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkHashes, Vec::new()).unwrap(),
        )
        .unwrap();
        assert_eq!(encoded[4], WFP_VERSION_V03);
        assert_eq!(encoded[5], 0x15);
    }

    #[test]
    fn rejects_chunk_start_for_wfp_v02_and_encodes_it_for_wfp_v03() {
        let invalid = Frame {
            version: WFP_VERSION_V02,
            message_type: MessageType::ChunkStart,
            flags: 0,
            payload: Vec::new(),
        };
        assert_eq!(
            encode_frame(&invalid),
            Err(EncodeError::InvalidFrame(
                FrameError::MessageTypeNotAllowed {
                    version: WFP_VERSION_V02,
                    message_type: MessageType::ChunkStart,
                }
            ))
        );

        let encoded = encode_frame(
            &Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkStart, Vec::new()).unwrap(),
        )
        .unwrap();
        assert_eq!(encoded[4], WFP_VERSION_V03);
        assert_eq!(encoded[5], 0x16);
    }
}
