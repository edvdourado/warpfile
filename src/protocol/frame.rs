use super::message::MessageType;

pub const WFP_MAGIC: [u8; 4] = *b"WFP\0";
pub const WFP_VERSION: u8 = 0x02;

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

impl Frame {
    pub fn new(message_type: MessageType, payload: Vec<u8>) -> Self {
        Self {
            version: WFP_VERSION,
            message_type,
            flags: 0,
            payload,
        }
    }
}
