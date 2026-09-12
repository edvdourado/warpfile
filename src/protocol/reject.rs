use std::error::Error;
use std::fmt;
use std::str;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RejectCode {
    FileExists = 0x0001,
    UnsafeFilename = 0x0002,
    CannotPrepareDestination = 0x0003,
}

impl TryFrom<u16> for RejectCode {
    type Error = u16;

    fn try_from(value: u16) -> Result<Self, u16> {
        match value {
            0x0001 => Ok(Self::FileExists),
            0x0002 => Ok(Self::UnsafeFilename),
            0x0003 => Ok(Self::CannotPrepareDestination),
            _ => Err(value),
        }
    }
}

impl fmt::Display for RejectCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::FileExists => "FILE_EXISTS",
            Self::UnsafeFilename => "UNSAFE_FILENAME",
            Self::CannotPrepareDestination => "CANNOT_PREPARE_DESTINATION",
        };

        formatter.write_str(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReject {
    pub code: RejectCode,
    pub message: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RejectError {
    MessageTooLong(usize),
    InvalidPayloadLength,
    InvalidUtf8,
    UnknownCode(u16),
}

impl fmt::Display for RejectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MessageTooLong(length) => {
                write!(formatter, "reject message is too long: {length} bytes")
            }

            Self::InvalidPayloadLength => formatter.write_str("invalid REJECT payload length"),

            Self::InvalidUtf8 => formatter.write_str("REJECT message is not valid UTF-8"),

            Self::UnknownCode(code) => {
                write!(formatter, "unknown REJECT code: {code}")
            }
        }
    }
}

impl Error for RejectError {}

pub fn encode_reject(reject: &FileReject) -> Result<Vec<u8>, RejectError> {
    let message_bytes = reject.message.as_bytes();

    if message_bytes.len() > u16::MAX as usize {
        return Err(RejectError::MessageTooLong(message_bytes.len()));
    }

    let message_length = message_bytes.len() as u16;

    let mut payload = Vec::with_capacity(4 + message_bytes.len());

    payload.extend_from_slice(&(reject.code as u16).to_be_bytes());

    payload.extend_from_slice(&message_length.to_be_bytes());

    payload.extend_from_slice(message_bytes);

    Ok(payload)
}

pub fn decode_reject(payload: &[u8]) -> Result<FileReject, RejectError> {
    if payload.len() < 4 {
        return Err(RejectError::InvalidPayloadLength);
    }

    let raw_code = u16::from_be_bytes([payload[0], payload[1]]);

    let code = RejectCode::try_from(raw_code).map_err(RejectError::UnknownCode)?;

    let message_length = u16::from_be_bytes([payload[2], payload[3]]) as usize;

    let expected_length = 4 + message_length;

    if payload.len() != expected_length {
        return Err(RejectError::InvalidPayloadLength);
    }

    let message = str::from_utf8(&payload[4..])
        .map_err(|_| RejectError::InvalidUtf8)?
        .to_string();

    Ok(FileReject { code, message })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_reject_exactly() {
        let reject = FileReject {
            code: RejectCode::FileExists,
            message: "exists".to_string(),
        };

        let encoded = encode_reject(&reject).unwrap();

        assert_eq!(
            encoded,
            vec![0x00, 0x01, 0x00, 0x06, b'e', b'x', b'i', b's', b't', b's',]
        );
    }

    #[test]
    fn reject_round_trip() {
        let original = FileReject {
            code: RejectCode::UnsafeFilename,
            message: "unsafe filename".to_string(),
        };

        let encoded = encode_reject(&original).unwrap();

        let decoded = decode_reject(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_unknown_reject_code() {
        let payload = [0x12, 0x34, 0x00, 0x00];

        let result = decode_reject(&payload);

        assert_eq!(result, Err(RejectError::UnknownCode(0x1234)));
    }

    #[test]
    fn rejects_invalid_payload_length() {
        let payload = [0x00, 0x01, 0x00, 0x05, b'a'];

        let result = decode_reject(&payload);

        assert_eq!(result, Err(RejectError::InvalidPayloadLength));
    }

    #[test]
    fn rejects_invalid_utf8_message() {
        let payload = [0x00, 0x01, 0x00, 0x01, 0xFF];

        let result = decode_reject(&payload);

        assert_eq!(result, Err(RejectError::InvalidUtf8));
    }
}
