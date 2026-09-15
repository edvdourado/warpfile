use std::error::Error;
use std::fmt;

use crate::chunk_manifest::ChunkHash;

use super::frame::{MAX_DATA_PAYLOAD_LENGTH, MAX_PAYLOAD_LENGTH};
use super::transfer_id::{TRANSFER_ID_LENGTH, TransferId};

pub const V03_RESUME_PAYLOAD_LENGTH: usize = 8;
pub const CHUNK_HASH_RECORD_LENGTH: usize = 40;
pub const V03_DATA_OFFSET_LENGTH: usize = 8;
pub const V03_MAX_DATA_BYTES: usize = MAX_DATA_PAYLOAD_LENGTH - V03_DATA_OFFSET_LENGTH;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileOfferV03 {
    pub transfer_id: TransferId,
    pub filename: String,
    pub file_size: u64,
    pub chunk_size: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OfferV03Error {
    FilenameEmpty,
    FilenameTooLong(usize),
    InvalidUtf8,
    InvalidPayloadLength,
    ZeroChunkSize,
}

impl fmt::Display for OfferV03Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FilenameEmpty => formatter.write_str("filename cannot be empty"),
            Self::FilenameTooLong(length) => {
                write!(formatter, "filename is too long: {length} bytes")
            }
            Self::InvalidUtf8 => formatter.write_str("filename is not valid UTF-8"),
            Self::InvalidPayloadLength => {
                formatter.write_str("invalid WFP/0.3 OFFER payload length")
            }
            Self::ZeroChunkSize => formatter.write_str("chunk size must be greater than zero"),
        }
    }
}

impl Error for OfferV03Error {}

pub fn encode_offer_v03(offer: &FileOfferV03) -> Result<Vec<u8>, OfferV03Error> {
    let filename = offer.filename.as_bytes();

    if filename.is_empty() {
        return Err(OfferV03Error::FilenameEmpty);
    }

    let filename_length = u16::try_from(filename.len())
        .map_err(|_| OfferV03Error::FilenameTooLong(filename.len()))?;

    if offer.chunk_size == 0 {
        return Err(OfferV03Error::ZeroChunkSize);
    }

    let mut payload = Vec::with_capacity(TRANSFER_ID_LENGTH + 2 + filename.len() + 16);
    payload.extend_from_slice(offer.transfer_id.as_bytes());
    payload.extend_from_slice(&filename_length.to_be_bytes());
    payload.extend_from_slice(filename);
    payload.extend_from_slice(&offer.file_size.to_be_bytes());
    payload.extend_from_slice(&offer.chunk_size.to_be_bytes());

    Ok(payload)
}

pub fn decode_offer_v03(payload: &[u8]) -> Result<FileOfferV03, OfferV03Error> {
    const MINIMUM_PAYLOAD_LENGTH: usize = TRANSFER_ID_LENGTH + 2 + 16;

    if payload.len() < MINIMUM_PAYLOAD_LENGTH {
        return Err(OfferV03Error::InvalidPayloadLength);
    }

    let mut transfer_id_bytes = [0u8; TRANSFER_ID_LENGTH];
    transfer_id_bytes.copy_from_slice(&payload[..TRANSFER_ID_LENGTH]);

    let filename_length =
        u16::from_be_bytes([payload[TRANSFER_ID_LENGTH], payload[TRANSFER_ID_LENGTH + 1]]) as usize;
    if filename_length == 0 {
        return Err(OfferV03Error::FilenameEmpty);
    }

    let filename_start = TRANSFER_ID_LENGTH + 2;
    let filename_end = filename_start
        .checked_add(filename_length)
        .ok_or(OfferV03Error::InvalidPayloadLength)?;
    let expected_length = filename_end
        .checked_add(16)
        .ok_or(OfferV03Error::InvalidPayloadLength)?;

    if payload.len() != expected_length {
        return Err(OfferV03Error::InvalidPayloadLength);
    }

    let filename = std::str::from_utf8(&payload[filename_start..filename_end])
        .map_err(|_| OfferV03Error::InvalidUtf8)?
        .to_owned();
    let file_size_start = filename_end;
    let chunk_size_start = file_size_start + 8;
    let file_size = u64::from_be_bytes(
        payload[file_size_start..chunk_size_start]
            .try_into()
            .expect("validated file size range"),
    );
    let chunk_size = u64::from_be_bytes(
        payload[chunk_size_start..]
            .try_into()
            .expect("validated chunk size range"),
    );

    if chunk_size == 0 {
        return Err(OfferV03Error::ZeroChunkSize);
    }

    Ok(FileOfferV03 {
        transfer_id: TransferId::from_bytes(transfer_id_bytes),
        filename,
        file_size,
        chunk_size,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeRequestV03 {
    pub chunk_record_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeV03Error {
    InvalidPayloadLength(usize),
}

impl fmt::Display for ResumeV03Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayloadLength(length) => write!(
                formatter,
                "WFP/0.3 RESUME payload must contain exactly 8 bytes, received {length}"
            ),
        }
    }
}

impl Error for ResumeV03Error {}

pub fn encode_resume_v03(request: &ResumeRequestV03) -> Vec<u8> {
    request.chunk_record_count.to_be_bytes().to_vec()
}

pub fn decode_resume_v03(payload: &[u8]) -> Result<ResumeRequestV03, ResumeV03Error> {
    let bytes: [u8; V03_RESUME_PAYLOAD_LENGTH] = payload
        .try_into()
        .map_err(|_| ResumeV03Error::InvalidPayloadLength(payload.len()))?;

    Ok(ResumeRequestV03 {
        chunk_record_count: u64::from_be_bytes(bytes),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkHashRecord {
    pub chunk_index: u64,
    pub hash: ChunkHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkHashesBatch {
    pub records: Vec<ChunkHashRecord>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChunkHashesError {
    RecordCountTooLarge(usize),
    PayloadTooLarge(usize),
    InvalidPayloadLength,
}

impl fmt::Display for ChunkHashesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecordCountTooLarge(count) => {
                write!(formatter, "too many chunk hash records: {count}")
            }
            Self::PayloadTooLarge(length) => write!(
                formatter,
                "CHUNK_HASHES payload is too large: {length} bytes"
            ),
            Self::InvalidPayloadLength => {
                formatter.write_str("invalid CHUNK_HASHES payload length")
            }
        }
    }
}

impl Error for ChunkHashesError {}

pub fn encode_chunk_hashes(batch: &ChunkHashesBatch) -> Result<Vec<u8>, ChunkHashesError> {
    let record_count = u32::try_from(batch.records.len())
        .map_err(|_| ChunkHashesError::RecordCountTooLarge(batch.records.len()))?;
    let payload_length = CHUNK_HASH_RECORD_LENGTH
        .checked_mul(batch.records.len())
        .and_then(|length| length.checked_add(4))
        .ok_or(ChunkHashesError::InvalidPayloadLength)?;

    if payload_length > MAX_PAYLOAD_LENGTH {
        return Err(ChunkHashesError::PayloadTooLarge(payload_length));
    }

    let mut payload = Vec::with_capacity(payload_length);
    payload.extend_from_slice(&record_count.to_be_bytes());
    for record in &batch.records {
        payload.extend_from_slice(&record.chunk_index.to_be_bytes());
        payload.extend_from_slice(record.hash.as_bytes());
    }

    Ok(payload)
}

pub fn decode_chunk_hashes(payload: &[u8]) -> Result<ChunkHashesBatch, ChunkHashesError> {
    if payload.len() < 4 {
        return Err(ChunkHashesError::InvalidPayloadLength);
    }
    if payload.len() > MAX_PAYLOAD_LENGTH {
        return Err(ChunkHashesError::PayloadTooLarge(payload.len()));
    }

    let record_count = u32::from_be_bytes(
        payload[..4]
            .try_into()
            .expect("validated record count range"),
    );
    let record_count =
        usize::try_from(record_count).map_err(|_| ChunkHashesError::InvalidPayloadLength)?;
    let expected_length = CHUNK_HASH_RECORD_LENGTH
        .checked_mul(record_count)
        .and_then(|length| length.checked_add(4))
        .ok_or(ChunkHashesError::InvalidPayloadLength)?;

    if payload.len() != expected_length {
        return Err(ChunkHashesError::InvalidPayloadLength);
    }

    let (record_bytes, remainder) = payload[4..].as_chunks::<CHUNK_HASH_RECORD_LENGTH>();
    debug_assert!(remainder.is_empty());

    let mut records = Vec::with_capacity(record_count);
    for bytes in record_bytes {
        let chunk_index =
            u64::from_be_bytes(bytes[..8].try_into().expect("fixed-size record index"));
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[8..]);
        records.push(ChunkHashRecord {
            chunk_index,
            hash: ChunkHash::from_bytes(hash),
        });
    }

    Ok(ChunkHashesBatch { records })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataV03 {
    pub absolute_offset: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DataV03Error {
    EmptyData,
    InvalidPayloadLength,
    PayloadTooLarge(usize),
}

impl fmt::Display for DataV03Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyData => formatter.write_str("WFP/0.3 DATA bytes cannot be empty"),
            Self::InvalidPayloadLength => {
                formatter.write_str("WFP/0.3 DATA payload is missing its offset")
            }
            Self::PayloadTooLarge(length) => write!(
                formatter,
                "WFP/0.3 DATA payload is too large: {length} bytes"
            ),
        }
    }
}

impl Error for DataV03Error {}

pub fn encode_data_v03(data: &DataV03) -> Result<Vec<u8>, DataV03Error> {
    if data.data.is_empty() {
        return Err(DataV03Error::EmptyData);
    }
    if data.data.len() > V03_MAX_DATA_BYTES {
        return Err(DataV03Error::PayloadTooLarge(
            V03_DATA_OFFSET_LENGTH + data.data.len(),
        ));
    }

    let mut payload = Vec::with_capacity(V03_DATA_OFFSET_LENGTH + data.data.len());
    payload.extend_from_slice(&data.absolute_offset.to_be_bytes());
    payload.extend_from_slice(&data.data);
    Ok(payload)
}

pub fn decode_data_v03(payload: &[u8]) -> Result<DataV03, DataV03Error> {
    if payload.len() > MAX_DATA_PAYLOAD_LENGTH {
        return Err(DataV03Error::PayloadTooLarge(payload.len()));
    }
    if payload.len() < V03_DATA_OFFSET_LENGTH {
        return Err(DataV03Error::InvalidPayloadLength);
    }
    if payload.len() == V03_DATA_OFFSET_LENGTH {
        return Err(DataV03Error::EmptyData);
    }

    let absolute_offset = u64::from_be_bytes(
        payload[..V03_DATA_OFFSET_LENGTH]
            .try_into()
            .expect("validated DATA offset range"),
    );
    Ok(DataV03 {
        absolute_offset,
        data: payload[V03_DATA_OFFSET_LENGTH..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{FileOffer, decode_offer, encode_offer};

    fn transfer_id() -> TransferId {
        TransferId::from_bytes([0x10; TRANSFER_ID_LENGTH])
    }

    #[test]
    fn offer_v03_round_trips_with_chunk_size() {
        let offer = FileOfferV03 {
            transfer_id: transfer_id(),
            filename: "archive.bin".to_string(),
            file_size: 123,
            chunk_size: 64,
        };

        assert_eq!(
            decode_offer_v03(&encode_offer_v03(&offer).unwrap()).unwrap(),
            offer
        );
    }

    #[test]
    fn offer_v03_rejects_zero_chunk_size() {
        let offer = FileOfferV03 {
            transfer_id: transfer_id(),
            filename: "archive.bin".to_string(),
            file_size: 123,
            chunk_size: 0,
        };

        assert_eq!(encode_offer_v03(&offer), Err(OfferV03Error::ZeroChunkSize));
        let mut payload = encode_offer_v03(&FileOfferV03 {
            chunk_size: 1,
            ..offer
        })
        .unwrap();
        payload.truncate(payload.len() - 8);
        payload.extend_from_slice(&0u64.to_be_bytes());
        assert_eq!(
            decode_offer_v03(&payload),
            Err(OfferV03Error::ZeroChunkSize)
        );
    }

    #[test]
    fn offer_v03_rejects_malformed_or_truncated_payload() {
        assert_eq!(
            decode_offer_v03(&[0; 33]),
            Err(OfferV03Error::InvalidPayloadLength)
        );
        let mut payload = encode_offer_v03(&FileOfferV03 {
            transfer_id: transfer_id(),
            filename: "a".to_string(),
            file_size: 1,
            chunk_size: 1,
        })
        .unwrap();
        payload.pop();
        assert_eq!(
            decode_offer_v03(&payload),
            Err(OfferV03Error::InvalidPayloadLength)
        );
    }

    #[test]
    fn offer_v02_behavior_remains_available() {
        let offer = FileOffer {
            transfer_id: transfer_id(),
            filename: "old.bin".to_string(),
            file_size: 12,
        };
        assert_eq!(decode_offer(&encode_offer(&offer).unwrap()).unwrap(), offer);
    }

    #[test]
    fn resume_v03_round_trips_chunk_record_count() {
        let request = ResumeRequestV03 {
            chunk_record_count: 42,
        };
        assert_eq!(
            decode_resume_v03(&encode_resume_v03(&request)).unwrap(),
            request
        );
    }

    #[test]
    fn resume_v03_requires_exact_payload_length() {
        assert_eq!(
            decode_resume_v03(&[0; 7]),
            Err(ResumeV03Error::InvalidPayloadLength(7))
        );
        assert_eq!(
            decode_resume_v03(&[0; 9]),
            Err(ResumeV03Error::InvalidPayloadLength(9))
        );
    }

    fn record(index: u64, byte: u8) -> ChunkHashRecord {
        ChunkHashRecord {
            chunk_index: index,
            hash: ChunkHash::from_bytes([byte; 32]),
        }
    }

    #[test]
    fn chunk_hashes_round_trip_one_and_multiple_records() {
        for records in [
            vec![record(4, 0xA4)],
            vec![record(1, 0xA1), record(7, 0xA7)],
        ] {
            let batch = ChunkHashesBatch { records };
            assert_eq!(
                decode_chunk_hashes(&encode_chunk_hashes(&batch).unwrap()).unwrap(),
                batch
            );
        }
    }

    #[test]
    fn chunk_hashes_allows_an_empty_batch() {
        let batch = ChunkHashesBatch {
            records: Vec::new(),
        };
        assert_eq!(encode_chunk_hashes(&batch).unwrap(), vec![0, 0, 0, 0]);
        assert_eq!(decode_chunk_hashes(&[0, 0, 0, 0]).unwrap(), batch);
    }

    #[test]
    fn chunk_hashes_rejects_count_mismatch_truncation_and_trailing_bytes() {
        assert_eq!(
            decode_chunk_hashes(&[0, 0, 0, 1]),
            Err(ChunkHashesError::InvalidPayloadLength)
        );
        let mut payload = encode_chunk_hashes(&ChunkHashesBatch {
            records: vec![record(1, 0xAA)],
        })
        .unwrap();
        payload.pop();
        assert_eq!(
            decode_chunk_hashes(&payload),
            Err(ChunkHashesError::InvalidPayloadLength)
        );
        let mut payload = encode_chunk_hashes(&ChunkHashesBatch {
            records: vec![record(1, 0xAA)],
        })
        .unwrap();
        payload.push(0);
        assert_eq!(
            decode_chunk_hashes(&payload),
            Err(ChunkHashesError::InvalidPayloadLength)
        );
    }

    #[test]
    fn chunk_hashes_handles_maximum_record_count_without_overflow() {
        assert_eq!(
            decode_chunk_hashes(&u32::MAX.to_be_bytes()),
            Err(ChunkHashesError::InvalidPayloadLength)
        );
    }

    #[test]
    fn data_v03_round_trips_offset_and_data_including_zero_offset() {
        for absolute_offset in [0, 99] {
            let data = DataV03 {
                absolute_offset,
                data: vec![1, 2, 3],
            };
            assert_eq!(
                decode_data_v03(&encode_data_v03(&data).unwrap()).unwrap(),
                data
            );
        }
    }

    #[test]
    fn data_v03_accepts_the_maximum_legal_raw_payload() {
        let data = DataV03 {
            absolute_offset: 1,
            data: vec![0xAA; V03_MAX_DATA_BYTES],
        };
        assert_eq!(
            encode_data_v03(&data).unwrap().len(),
            MAX_DATA_PAYLOAD_LENGTH
        );
    }

    #[test]
    fn data_v03_rejects_empty_data_truncated_offset_and_oversized_payload() {
        assert_eq!(
            encode_data_v03(&DataV03 {
                absolute_offset: 0,
                data: Vec::new()
            }),
            Err(DataV03Error::EmptyData)
        );
        assert_eq!(
            decode_data_v03(&[0; 7]),
            Err(DataV03Error::InvalidPayloadLength)
        );
        assert_eq!(
            decode_data_v03(&vec![0; MAX_DATA_PAYLOAD_LENGTH + 1]),
            Err(DataV03Error::PayloadTooLarge(MAX_DATA_PAYLOAD_LENGTH + 1))
        );
    }
}
