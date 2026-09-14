use std::fmt;

pub const TRANSFER_ID_LENGTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransferId([u8; TRANSFER_ID_LENGTH]);

impl TransferId {
    pub const fn from_bytes(bytes: [u8; TRANSFER_ID_LENGTH]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; TRANSFER_ID_LENGTH] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; TRANSFER_ID_LENGTH] {
        self.0
    }
}

impl fmt::Display for TransferId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_transfer_id_bytes() {
        let bytes = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ];

        let transfer_id = TransferId::from_bytes(bytes);

        assert_eq!(transfer_id.as_bytes(), &bytes);
        assert_eq!(transfer_id.into_bytes(), bytes);
    }

    #[test]
    fn displays_transfer_id_as_lowercase_hex() {
        let transfer_id = TransferId::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ]);

        assert_eq!(transfer_id.to_string(), "00112233445566778899aabbccddeeff");
    }
}
