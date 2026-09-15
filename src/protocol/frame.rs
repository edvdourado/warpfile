use std::error::Error;
use std::fmt;

use super::message::MessageType;

pub const WFP_MAGIC: [u8; 4] = *b"WFP\0";
pub const WFP_VERSION_V02: u8 = 0x02;
pub const WFP_VERSION_V03: u8 = 0x03;
pub const ACTIVE_WFP_VERSION: u8 = WFP_VERSION_V02;
pub const WFP_VERSION: u8 = ACTIVE_WFP_VERSION;

pub const HEADER_LENGTH: usize = 12;

pub const MAX_PAYLOAD_LENGTH: usize = 1024 * 1024;
pub const MAX_DATA_PAYLOAD_LENGTH: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub version: u8,
    pub message_type: MessageType,
    pub flags: u16,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    UnsupportedVersion(u8),
    MessageTypeNotAllowed {
        version: u8,
        message_type: MessageType,
    },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => write!(f, "unsupported WFP version: {version}"),
            Self::MessageTypeNotAllowed {
                version,
                message_type,
            } => write!(
                f,
                "WFP version {version} does not allow message type 0x{:02X}",
                *message_type as u8
            ),
        }
    }
}

impl Error for FrameError {}

impl Frame {
    pub fn new(message_type: MessageType, payload: Vec<u8>) -> Self {
        Self {
            version: WFP_VERSION,
            message_type,
            flags: 0,
            payload,
        }
    }

    pub fn new_for_version(
        version: u8,
        message_type: MessageType,
        payload: Vec<u8>,
    ) -> Result<Self, FrameError> {
        validate_version_and_message_type(version, message_type)?;

        Ok(Self {
            version,
            message_type,
            flags: 0,
            payload,
        })
    }
}

pub(crate) fn is_supported_version(version: u8) -> bool {
    matches!(version, WFP_VERSION_V02 | WFP_VERSION_V03)
}

pub(crate) fn validate_version_and_message_type(
    version: u8,
    message_type: MessageType,
) -> Result<(), FrameError> {
    if !is_supported_version(version) {
        return Err(FrameError::UnsupportedVersion(version));
    }

    if !message_type.is_allowed_in_version(version) {
        return Err(FrameError::MessageTypeNotAllowed {
            version,
            message_type,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_the_active_wfp_v02_version() {
        assert_eq!(
            Frame::new(MessageType::Hello, Vec::new()).version,
            WFP_VERSION_V02
        );
        assert_eq!(ACTIVE_WFP_VERSION, WFP_VERSION_V02);
    }

    #[test]
    fn constructs_chunk_hashes_only_for_wfp_v03() {
        assert_eq!(
            Frame::new_for_version(WFP_VERSION_V02, MessageType::ChunkHashes, Vec::new()),
            Err(FrameError::MessageTypeNotAllowed {
                version: WFP_VERSION_V02,
                message_type: MessageType::ChunkHashes,
            })
        );
        assert_eq!(
            Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkHashes, Vec::new())
                .unwrap()
                .version,
            WFP_VERSION_V03
        );
    }
}
