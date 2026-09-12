#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Hello = 0x01,
    HelloAck = 0x02,

    Offer = 0x10,
    Accept = 0x11,
    Reject = 0x12,

    Data = 0x20,

    Complete = 0x30,
    Verified = 0x31,

    Cancel = 0x40,

    Error = 0xFF,
}

impl TryFrom<u8> for MessageType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0x01 => Ok(MessageType::Hello),
            0x02 => Ok(MessageType::HelloAck),

            0x10 => Ok(MessageType::Offer),
            0x11 => Ok(MessageType::Accept),
            0x12 => Ok(MessageType::Reject),

            0x20 => Ok(MessageType::Data),

            0x30 => Ok(MessageType::Complete),
            0x31 => Ok(MessageType::Verified),

            0x40 => Ok(MessageType::Cancel),

            0xFF => Ok(MessageType::Error),

            unknown => Err(unknown),
        }
    }
}
