use std::error::Error;
use std::fmt;

use serde_json::{Value, json};

use crate::protocol::{TRANSFER_ID_LENGTH, TransferId};

pub const TRANSFER_METADATA_FORMAT_VERSION: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferState {
    Partial,
}

impl TransferState {
    const fn as_str(self) -> &'static str {
        match self {
            TransferState::Partial => "partial",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferMetadata {
    pub transfer_id: TransferId,
    pub filename: String,
    pub file_size: u64,
    pub state: TransferState,
}

#[derive(Debug)]
pub enum TransferMetadataError {
    Json(serde_json::Error),
    RootNotObject,
    MissingField(&'static str),
    InvalidField(&'static str),
    UnsupportedFormatVersion(u64),
    UnsupportedState(String),
    InvalidTransferId,
}

impl fmt::Display for TransferMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransferMetadataError::Json(error) => {
                write!(formatter, "invalid transfer metadata JSON: {error}")
            }

            TransferMetadataError::RootNotObject => {
                write!(formatter, "transfer metadata root must be a JSON object")
            }

            TransferMetadataError::MissingField(field) => {
                write!(formatter, "transfer metadata is missing field: {field}")
            }

            TransferMetadataError::InvalidField(field) => {
                write!(
                    formatter,
                    "transfer metadata contains invalid field: {field}"
                )
            }

            TransferMetadataError::UnsupportedFormatVersion(version) => {
                write!(
                    formatter,
                    "unsupported transfer metadata format version: {version}"
                )
            }

            TransferMetadataError::UnsupportedState(state) => {
                write!(formatter, "unsupported transfer metadata state: {state}")
            }

            TransferMetadataError::InvalidTransferId => {
                write!(
                    formatter,
                    "transfer metadata contains an invalid transfer ID"
                )
            }
        }
    }
}

impl Error for TransferMetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TransferMetadataError::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for TransferMetadataError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub fn encode_transfer_metadata(
    metadata: &TransferMetadata,
) -> Result<Vec<u8>, TransferMetadataError> {
    let value = json!({
        "format_version": TRANSFER_METADATA_FORMAT_VERSION,
        "transfer_id": metadata.transfer_id.to_string(),
        "filename": metadata.filename.as_str(),
        "file_size": metadata.file_size,
        "state": metadata.state.as_str(),
    });

    Ok(serde_json::to_vec_pretty(&value)?)
}

pub fn decode_transfer_metadata(bytes: &[u8]) -> Result<TransferMetadata, TransferMetadataError> {
    let value: Value = serde_json::from_slice(bytes)?;

    let object = value
        .as_object()
        .ok_or(TransferMetadataError::RootNotObject)?;

    let format_version = object
        .get("format_version")
        .ok_or(TransferMetadataError::MissingField("format_version"))?
        .as_u64()
        .ok_or(TransferMetadataError::InvalidField("format_version"))?;

    if format_version != TRANSFER_METADATA_FORMAT_VERSION {
        return Err(TransferMetadataError::UnsupportedFormatVersion(
            format_version,
        ));
    }

    let transfer_id_text = object
        .get("transfer_id")
        .ok_or(TransferMetadataError::MissingField("transfer_id"))?
        .as_str()
        .ok_or(TransferMetadataError::InvalidField("transfer_id"))?;

    let transfer_id = parse_transfer_id(transfer_id_text)?;

    let filename = object
        .get("filename")
        .ok_or(TransferMetadataError::MissingField("filename"))?
        .as_str()
        .ok_or(TransferMetadataError::InvalidField("filename"))?
        .to_string();

    if filename.is_empty() {
        return Err(TransferMetadataError::InvalidField("filename"));
    }

    let file_size = object
        .get("file_size")
        .ok_or(TransferMetadataError::MissingField("file_size"))?
        .as_u64()
        .ok_or(TransferMetadataError::InvalidField("file_size"))?;

    let state_text = object
        .get("state")
        .ok_or(TransferMetadataError::MissingField("state"))?
        .as_str()
        .ok_or(TransferMetadataError::InvalidField("state"))?;

    let state = match state_text {
        "partial" => TransferState::Partial,
        other => {
            return Err(TransferMetadataError::UnsupportedState(other.to_string()));
        }
    };

    Ok(TransferMetadata {
        transfer_id,
        filename,
        file_size,
        state,
    })
}

fn parse_transfer_id(text: &str) -> Result<TransferId, TransferMetadataError> {
    if text.len() != TRANSFER_ID_LENGTH * 2 {
        return Err(TransferMetadataError::InvalidTransferId);
    }

    let mut bytes = [0u8; TRANSFER_ID_LENGTH];

    for (index, byte) in bytes.iter_mut().enumerate() {
        let high_index = index * 2;
        let low_index = high_index + 1;

        let high = decode_hex_digit(text.as_bytes()[high_index])
            .ok_or(TransferMetadataError::InvalidTransferId)?;

        let low = decode_hex_digit(text.as_bytes()[low_index])
            .ok_or(TransferMetadataError::InvalidTransferId)?;

        *byte = (high << 4) | low;
    }

    Ok(TransferId::from_bytes(bytes))
}

const fn decode_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metadata() -> TransferMetadata {
        TransferMetadata {
            transfer_id: TransferId::from_bytes([
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
                0xEE, 0xFF,
            ]),
            filename: "arquivo.bin".to_string(),
            file_size: 123_456_789,
            state: TransferState::Partial,
        }
    }

    #[test]
    fn transfer_metadata_round_trip() {
        let original = test_metadata();

        let encoded = encode_transfer_metadata(&original).unwrap();

        let decoded = decode_transfer_metadata(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn encodes_transfer_id_as_readable_hex() {
        let metadata = test_metadata();

        let encoded = encode_transfer_metadata(&metadata).unwrap();

        let value: Value = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(value["transfer_id"], "00112233445566778899aabbccddeeff");

        assert_eq!(value["format_version"], TRANSFER_METADATA_FORMAT_VERSION);

        assert_eq!(value["state"], "partial");
    }

    #[test]
    fn rejects_unsupported_format_version() {
        let bytes = br#"
        {
            "format_version": 99,
            "transfer_id": "00112233445566778899aabbccddeeff",
            "filename": "arquivo.bin",
            "file_size": 100,
            "state": "partial"
        }
        "#;

        let error = decode_transfer_metadata(bytes).unwrap_err();

        assert!(matches!(
            error,
            TransferMetadataError::UnsupportedFormatVersion(99)
        ));
    }

    #[test]
    fn rejects_invalid_transfer_id() {
        let bytes = br#"
        {
            "format_version": 1,
            "transfer_id": "isso-nao-e-um-transfer-id",
            "filename": "arquivo.bin",
            "file_size": 100,
            "state": "partial"
        }
        "#;

        let error = decode_transfer_metadata(bytes).unwrap_err();

        assert!(matches!(error, TransferMetadataError::InvalidTransferId));
    }

    #[test]
    fn rejects_unknown_state() {
        let bytes = br#"
        {
            "format_version": 1,
            "transfer_id": "00112233445566778899aabbccddeeff",
            "filename": "arquivo.bin",
            "file_size": 100,
            "state": "teleported"
        }
        "#;

        let error = decode_transfer_metadata(bytes).unwrap_err();

        assert!(matches!(
            error,
            TransferMetadataError::UnsupportedState(ref state)
                if state == "teleported"
        ));
    }
}
