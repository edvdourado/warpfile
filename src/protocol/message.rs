use super::frame::{WFP_VERSION_V02, WFP_VERSION_V03};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Hello = 0x01,
    HelloAck = 0x02,

    Offer = 0x10,
    Accept = 0x11,
    Reject = 0x12,
    Resume = 0x13,
    Restart = 0x14,
    ChunkHashes = 0x15,

    Data = 0x20,

    Complete = 0x30,
    Verified = 0x31,

    Cancel = 0x40,

    Discover = 0x50,
    Announce = 0x51,

    Error = 0xFF,
}

impl MessageType {
    pub(crate) fn from_wire(version: u8, value: u8) -> Result<Self, u8> {
        let message_type = match value {
            0x01 => Ok(Self::Hello),
            0x02 => Ok(Self::HelloAck),

            0x10 => Ok(Self::Offer),
            0x11 => Ok(Self::Accept),
            0x12 => Ok(Self::Reject),
            0x13 => Ok(Self::Resume),
            0x14 => Ok(Self::Restart),
            0x15 => Ok(Self::ChunkHashes),

            0x20 => Ok(Self::Data),

            0x30 => Ok(Self::Complete),
            0x31 => Ok(Self::Verified),

            0x40 => Ok(Self::Cancel),

            0x50 => Ok(Self::Discover),
            0x51 => Ok(Self::Announce),

            0xFF => Ok(Self::Error),

            unknown => Err(unknown),
        }?;

        if message_type.is_allowed_in_version(version) {
            Ok(message_type)
        } else {
            Err(value)
        }
    }

    pub(crate) fn is_allowed_in_version(self, version: u8) -> bool {
        match version {
            WFP_VERSION_V02 => self != Self::ChunkHashes,
            WFP_VERSION_V03 => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_discover_message_type() {
        assert_eq!(
            MessageType::from_wire(WFP_VERSION_V02, 0x50),
            Ok(MessageType::Discover)
        );
    }

    #[test]
    fn decodes_announce_message_type() {
        assert_eq!(
            MessageType::from_wire(WFP_VERSION_V03, 0x51),
            Ok(MessageType::Announce)
        );
    }

    #[test]
    fn decodes_resume_message_type() {
        for version in [WFP_VERSION_V02, WFP_VERSION_V03] {
            assert_eq!(
                MessageType::from_wire(version, 0x13),
                Ok(MessageType::Resume)
            );
        }
    }

    #[test]
    fn decodes_restart_message_type() {
        assert_eq!(
            MessageType::from_wire(WFP_VERSION_V03, 0x14),
            Ok(MessageType::Restart)
        );
    }

    #[test]
    fn chunk_hashes_is_allocated_only_for_wfp_v03() {
        assert_eq!(MessageType::ChunkHashes as u8, 0x15);
        assert_eq!(MessageType::from_wire(WFP_VERSION_V02, 0x15), Err(0x15));
        assert_eq!(
            MessageType::from_wire(WFP_VERSION_V03, 0x15),
            Ok(MessageType::ChunkHashes)
        );
    }

    #[test]
    fn rejects_unknown_message_type() {
        assert_eq!(MessageType::from_wire(WFP_VERSION_V02, 0x7E), Err(0x7E));
        assert_eq!(MessageType::from_wire(WFP_VERSION_V03, 0x7E), Err(0x7E));
    }
}
