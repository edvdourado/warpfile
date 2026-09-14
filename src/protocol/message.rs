#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Hello = 0x01,
    HelloAck = 0x02,

    Offer = 0x10,
    Accept = 0x11,
    Reject = 0x12,
    Resume = 0x13,

    Data = 0x20,

    Complete = 0x30,
    Verified = 0x31,

    Cancel = 0x40,

    Discover = 0x50,
    Announce = 0x51,

    Error = 0xFF,
}

impl TryFrom<u8> for MessageType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0x01 => Ok(Self::Hello),
            0x02 => Ok(Self::HelloAck),

            0x10 => Ok(Self::Offer),
            0x11 => Ok(Self::Accept),
            0x12 => Ok(Self::Reject),
            0x13 => Ok(Self::Resume),

            0x20 => Ok(Self::Data),

            0x30 => Ok(Self::Complete),
            0x31 => Ok(Self::Verified),

            0x40 => Ok(Self::Cancel),

            0x50 => Ok(Self::Discover),
            0x51 => Ok(Self::Announce),

            0xFF => Ok(Self::Error),

            unknown => Err(unknown),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_discover_message_type() {
        assert_eq!(MessageType::try_from(0x50), Ok(MessageType::Discover));
    }

    #[test]
    fn decodes_announce_message_type() {
        assert_eq!(MessageType::try_from(0x51), Ok(MessageType::Announce));
    }

    #[test]
    fn decodes_resume_message_type() {
        assert_eq!(MessageType::try_from(0x13), Ok(MessageType::Resume));
    }

    #[test]
    fn rejects_unknown_message_type() {
        assert_eq!(MessageType::try_from(0x7E), Err(0x7E));
    }
}
