use std::error::Error;
use std::fmt;

use super::frame::{
    Frame, HEADER_LENGTH, MAX_DATA_PAYLOAD_LENGTH, MAX_PAYLOAD_LENGTH, WFP_MAGIC, WFP_VERSION,
};
use super::message::MessageType;

#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    HeaderTooShort(usize),
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    UnknownMessageType(u8),
    UnsupportedFlags(u16),
    PayloadTooLarge(usize),
    DataPayloadTooLarge(usize),
    IncompleteFrame { expected: usize, actual: usize },
    TrailingBytes { expected: usize, actual: usize },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::HeaderTooShort(size) => {
                write!(f, "frame header is too short: {size} bytes")
            }

            DecodeError::InvalidMagic(magic) => {
                write!(f, "invalid WFP magic: {magic:02X?}")
            }

            DecodeError::UnsupportedVersion(version) => {
                write!(f, "unsupported WFP version: {version}")
            }

            DecodeError::UnknownMessageType(message_type) => {
                write!(f, "unknown WFP message type: 0x{message_type:02X}")
            }

            DecodeError::UnsupportedFlags(flags) => {
                write!(f, "unsupported WFP flags: 0x{flags:04X}")
            }

            DecodeError::PayloadTooLarge(size) => {
                write!(f, "payload is too large: {size} bytes")
            }

            DecodeError::DataPayloadTooLarge(size) => {
                write!(f, "DATA payload is too large: {size} bytes")
            }

            DecodeError::IncompleteFrame { expected, actual } => {
                write!(
                    f,
                    "incomplete frame: expected {expected} bytes, got {actual}"
                )
            }

            DecodeError::TrailingBytes { expected, actual } => {
                write!(
                    f,
                    "frame contains trailing bytes: expected {expected} bytes, got {actual}"
                )
            }
        }
    }
}

impl Error for DecodeError {}

pub fn decode_frame(bytes: &[u8]) -> Result<Frame, DecodeError> {
    if bytes.len() < HEADER_LENGTH {
        return Err(DecodeError::HeaderTooShort(bytes.len()));
    }

    let magic = [bytes[0], bytes[1], bytes[2], bytes[3]];

    if magic != WFP_MAGIC {
        return Err(DecodeError::InvalidMagic(magic));
    }

    let version = bytes[4];

    if version != WFP_VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }

    let message_type_byte = bytes[5];

    let message_type =
        MessageType::try_from(message_type_byte).map_err(DecodeError::UnknownMessageType)?;

    let flags = u16::from_be_bytes([bytes[6], bytes[7]]);

    if flags != 0 {
        return Err(DecodeError::UnsupportedFlags(flags));
    }

    let payload_length = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;

    if payload_length > MAX_PAYLOAD_LENGTH {
        return Err(DecodeError::PayloadTooLarge(payload_length));
    }

    if message_type == MessageType::Data && payload_length > MAX_DATA_PAYLOAD_LENGTH {
        return Err(DecodeError::DataPayloadTooLarge(payload_length));
    }

    let expected_length = HEADER_LENGTH + payload_length;

    if bytes.len() < expected_length {
        return Err(DecodeError::IncompleteFrame {
            expected: expected_length,
            actual: bytes.len(),
        });
    }

    if bytes.len() > expected_length {
        return Err(DecodeError::TrailingBytes {
            expected: expected_length,
            actual: bytes.len(),
        });
    }

    let payload = bytes[HEADER_LENGTH..expected_length].to_vec();

    Ok(Frame {
        version,
        message_type,
        flags,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::encoder::encode_frame;

    #[test]
    fn decodes_valid_hello_frame() {
        let bytes = vec![
            0x57, 0x46, 0x50, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
        ];

        let frame = decode_frame(&bytes).unwrap();

        assert_eq!(frame.version, WFP_VERSION);
        assert_eq!(frame.message_type, MessageType::Hello);
        assert_eq!(frame.flags, 0);
        assert_eq!(frame.payload, vec![WFP_VERSION]);
    }

    #[test]
    fn encoder_and_decoder_round_trip() {
        let original = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

        let bytes = encode_frame(&original).unwrap();

        let decoded = decode_frame(&bytes).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_invalid_magic() {
        let bytes = vec![
            0x42, 0x41, 0x44, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
        ];

        let result = decode_frame(&bytes);

        assert_eq!(
            result,
            Err(DecodeError::InvalidMagic([0x42, 0x41, 0x44, 0x00,]))
        );
    }

    #[test]
    fn rejects_unknown_message_type() {
        let bytes = vec![
            0x57, 0x46, 0x50, 0x00, 0x02, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let result = decode_frame(&bytes);

        assert_eq!(result, Err(DecodeError::UnknownMessageType(0x73)));
    }

    #[test]
    fn rejects_chunk_hashes_message_type_under_wfp_v02() {
        let bytes = vec![
            0x57,
            0x46,
            0x50,
            0x00,
            WFP_VERSION,
            0x15,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];

        assert_eq!(
            decode_frame(&bytes),
            Err(DecodeError::UnknownMessageType(0x15))
        );
    }

    #[test]
    fn rejects_incomplete_frame() {
        let bytes = vec![
            0x57, 0x46, 0x50, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x02, 0x02,
        ];

        let result = decode_frame(&bytes);

        assert_eq!(
            result,
            Err(DecodeError::IncompleteFrame {
                expected: 17,
                actual: 14,
            })
        );
    }
}
