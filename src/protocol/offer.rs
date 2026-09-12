use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileOffer {
    pub filename: String,
    pub file_size: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OfferError {
    FilenameEmpty,
    FilenameTooLong(usize),
    InvalidUtf8,
    InvalidPayloadLength,
}

impl fmt::Display for OfferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OfferError::FilenameEmpty => {
                write!(f, "filename cannot be empty")
            }

            OfferError::FilenameTooLong(length) => {
                write!(f, "filename is too long: {length} bytes")
            }

            OfferError::InvalidUtf8 => {
                write!(f, "filename is not valid UTF-8")
            }

            OfferError::InvalidPayloadLength => {
                write!(f, "invalid OFFER payload length")
            }
        }
    }
}

impl Error for OfferError {}

pub fn encode_offer(offer: &FileOffer) -> Result<Vec<u8>, OfferError> {
    let filename_bytes = offer.filename.as_bytes();

    if filename_bytes.is_empty() {
        return Err(OfferError::FilenameEmpty);
    }

    let filename_length = filename_bytes.len();

    let filename_length_u16 =
        u16::try_from(filename_length).map_err(|_| OfferError::FilenameTooLong(filename_length))?;

    let mut payload = Vec::with_capacity(2 + filename_length + 8);

    payload.extend_from_slice(&filename_length_u16.to_be_bytes());

    payload.extend_from_slice(filename_bytes);

    payload.extend_from_slice(&offer.file_size.to_be_bytes());

    Ok(payload)
}

pub fn decode_offer(payload: &[u8]) -> Result<FileOffer, OfferError> {
    const MINIMUM_PAYLOAD_LENGTH: usize = 2 + 8;

    if payload.len() < MINIMUM_PAYLOAD_LENGTH {
        return Err(OfferError::InvalidPayloadLength);
    }

    let filename_length = u16::from_be_bytes([payload[0], payload[1]]) as usize;

    if filename_length == 0 {
        return Err(OfferError::FilenameEmpty);
    }

    let expected_length = 2 + filename_length + 8;

    if payload.len() != expected_length {
        return Err(OfferError::InvalidPayloadLength);
    }

    let filename_start = 2;
    let filename_end = filename_start + filename_length;

    let filename_bytes = &payload[filename_start..filename_end];

    let filename = std::str::from_utf8(filename_bytes)
        .map_err(|_| OfferError::InvalidUtf8)?
        .to_owned();

    let file_size_start = filename_end;

    let file_size = u64::from_be_bytes([
        payload[file_size_start],
        payload[file_size_start + 1],
        payload[file_size_start + 2],
        payload[file_size_start + 3],
        payload[file_size_start + 4],
        payload[file_size_start + 5],
        payload[file_size_start + 6],
        payload[file_size_start + 7],
    ]);

    Ok(FileOffer {
        filename,
        file_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_offer_exactly() {
        let offer = FileOffer {
            filename: "foto.jpg".to_string(),
            file_size: 1000,
        };

        let encoded = encode_offer(&offer).unwrap();

        let expected = vec![
            0x00, 0x08, 0x66, 0x6F, 0x74, 0x6F, 0x2E, 0x6A, 0x70, 0x67, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x03, 0xE8,
        ];

        assert_eq!(encoded, expected);
    }

    #[test]
    fn offer_round_trip() {
        let original = FileOffer {
            filename: "arquivo grande.bin".to_string(),
            file_size: 12_345_678_901,
        };

        let encoded = encode_offer(&original).unwrap();

        let decoded = decode_offer(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn supports_utf8_filename() {
        let original = FileOffer {
            filename: "férias-ação.txt".to_string(),
            file_size: 42,
        };

        let encoded = encode_offer(&original).unwrap();

        let decoded = decode_offer(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_empty_filename() {
        let offer = FileOffer {
            filename: String::new(),
            file_size: 10,
        };

        assert_eq!(encode_offer(&offer), Err(OfferError::FilenameEmpty));
    }

    #[test]
    fn rejects_invalid_payload_length() {
        let payload = vec![
            0x00, 0x05, 0x61, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        ];

        assert_eq!(
            decode_offer(&payload),
            Err(OfferError::InvalidPayloadLength)
        );
    }
}
