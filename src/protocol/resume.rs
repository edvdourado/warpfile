use std::error::Error;
use std::fmt;

pub const RESUME_PAYLOAD_LENGTH: usize = 40;
pub const BLAKE3_HASH_LENGTH: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeRequest {
    pub offset: u64,
    pub prefix_hash: [u8; BLAKE3_HASH_LENGTH],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeError {
    InvalidPayloadLength(usize),
}

impl fmt::Display for ResumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayloadLength(length) => {
                write!(
                    formatter,
                    "RESUME payload must contain exactly 40 bytes, received {length}"
                )
            }
        }
    }
}

impl Error for ResumeError {}

pub fn encode_resume(request: &ResumeRequest) -> Vec<u8> {
    let mut payload = Vec::with_capacity(RESUME_PAYLOAD_LENGTH);

    payload.extend_from_slice(&request.offset.to_be_bytes());

    payload.extend_from_slice(&request.prefix_hash);

    payload
}

pub fn decode_resume(payload: &[u8]) -> Result<ResumeRequest, ResumeError> {
    if payload.len() != RESUME_PAYLOAD_LENGTH {
        return Err(ResumeError::InvalidPayloadLength(payload.len()));
    }

    let offset = u64::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);

    let mut prefix_hash = [0u8; BLAKE3_HASH_LENGTH];

    prefix_hash.copy_from_slice(&payload[8..40]);

    Ok(ResumeRequest {
        offset,
        prefix_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_resume_exactly() {
        let request = ResumeRequest {
            offset: 65_536,
            prefix_hash: [0xAB; 32],
        };

        let encoded = encode_resume(&request);

        assert_eq!(encoded.len(), 40);

        assert_eq!(
            &encoded[..8],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,]
        );

        assert_eq!(&encoded[8..], &[0xAB; 32]);
    }

    #[test]
    fn resume_round_trip() {
        let original = ResumeRequest {
            offset: 12_345_678_901,
            prefix_hash: [0x42; 32],
        };

        let encoded = encode_resume(&original);

        let decoded = decode_resume(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn supports_zero_offset() {
        let request = ResumeRequest {
            offset: 0,
            prefix_hash: [0; 32],
        };

        let encoded = encode_resume(&request);

        let decoded = decode_resume(&encoded).unwrap();

        assert_eq!(decoded.offset, 0);

        assert_eq!(decoded.prefix_hash, [0; 32]);
    }

    #[test]
    fn rejects_invalid_resume_payload_length() {
        let payload = vec![0; 39];

        assert_eq!(
            decode_resume(&payload),
            Err(ResumeError::InvalidPayloadLength(39))
        );
    }
}
