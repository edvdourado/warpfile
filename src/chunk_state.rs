use crate::chunk::ChunkLayout;
use crate::chunk_manifest::ChunkHash;
use serde_json::{Value, json};
use std::error::Error;
use std::fmt;

pub const CHUNK_STATE_FORMAT_VERSION: u64 = 1;

const CHUNK_HASH_LENGTH: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedChunk {
    pub index: u64,
    pub hash: ChunkHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkState {
    layout: ChunkLayout,
    chunks: Vec<RecordedChunk>,
}

#[derive(Debug)]
pub enum ChunkStateError {
    Json(serde_json::Error),
    RootNotObject,
    MissingField(&'static str),
    InvalidField(&'static str),
    UnsupportedFormatVersion(u64),
    InvalidChunkHash(u64),
    ChunkIndexOutOfRange(u64),
    DuplicateChunkIndex(u64),
    UnorderedChunkIndices { previous: u64, current: u64 },
}

impl ChunkState {
    pub fn new(layout: ChunkLayout, chunks: Vec<RecordedChunk>) -> Result<Self, ChunkStateError> {
        validate_recorded_chunks(layout, &chunks)?;

        Ok(Self { layout, chunks })
    }

    pub const fn layout(&self) -> ChunkLayout {
        self.layout
    }

    pub fn recorded_chunks(&self) -> &[RecordedChunk] {
        &self.chunks
    }

    pub fn hash(&self, index: u64) -> Option<ChunkHash> {
        let position = self
            .chunks
            .binary_search_by_key(&index, |chunk| chunk.index)
            .ok()?;

        Some(self.chunks[position].hash)
    }
}

impl fmt::Display for ChunkStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => {
                write!(formatter, "invalid chunk state JSON: {error}")
            }
            Self::RootNotObject => {
                write!(formatter, "chunk state root must be a JSON object")
            }
            Self::MissingField(field) => {
                write!(formatter, "chunk state is missing field: {field}")
            }
            Self::InvalidField(field) => {
                write!(formatter, "chunk state contains invalid field: {field}")
            }
            Self::UnsupportedFormatVersion(version) => {
                write!(
                    formatter,
                    "unsupported chunk state format version: {version}"
                )
            }
            Self::InvalidChunkHash(index) => {
                write!(
                    formatter,
                    "chunk state contains an invalid BLAKE3 hash for chunk {index}"
                )
            }
            Self::ChunkIndexOutOfRange(index) => {
                write!(
                    formatter,
                    "chunk state contains out-of-range chunk index: {index}"
                )
            }
            Self::DuplicateChunkIndex(index) => {
                write!(
                    formatter,
                    "chunk state contains duplicate chunk index: {index}"
                )
            }
            Self::UnorderedChunkIndices { previous, current } => {
                write!(
                    formatter,
                    "chunk state chunk indices are not ordered: {previous} before {current}"
                )
            }
        }
    }
}

impl Error for ChunkStateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for ChunkStateError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub fn encode_chunk_state(state: &ChunkState) -> Result<Vec<u8>, ChunkStateError> {
    let chunks: Vec<Value> = state
        .recorded_chunks()
        .iter()
        .map(|chunk| {
            json!({
                "index": chunk.index,
                "blake3": encode_chunk_hash(chunk.hash),
            })
        })
        .collect();

    let value = json!({
        "format_version": CHUNK_STATE_FORMAT_VERSION,
        "file_size": state.layout().file_size(),
        "chunk_size": state.layout().chunk_size(),
        "chunks": chunks,
    });

    Ok(serde_json::to_vec_pretty(&value)?)
}

pub fn decode_chunk_state(bytes: &[u8]) -> Result<ChunkState, ChunkStateError> {
    let value: Value = serde_json::from_slice(bytes)?;

    let object = value.as_object().ok_or(ChunkStateError::RootNotObject)?;

    let format_version = object
        .get("format_version")
        .ok_or(ChunkStateError::MissingField("format_version"))?
        .as_u64()
        .ok_or(ChunkStateError::InvalidField("format_version"))?;

    if format_version != CHUNK_STATE_FORMAT_VERSION {
        return Err(ChunkStateError::UnsupportedFormatVersion(format_version));
    }

    let file_size = object
        .get("file_size")
        .ok_or(ChunkStateError::MissingField("file_size"))?
        .as_u64()
        .ok_or(ChunkStateError::InvalidField("file_size"))?;

    let chunk_size = object
        .get("chunk_size")
        .ok_or(ChunkStateError::MissingField("chunk_size"))?
        .as_u64()
        .ok_or(ChunkStateError::InvalidField("chunk_size"))?;

    let layout = ChunkLayout::new(file_size, chunk_size)
        .map_err(|_| ChunkStateError::InvalidField("chunk_size"))?;

    let chunk_values = object
        .get("chunks")
        .ok_or(ChunkStateError::MissingField("chunks"))?
        .as_array()
        .ok_or(ChunkStateError::InvalidField("chunks"))?;

    let mut chunks = Vec::with_capacity(chunk_values.len());

    for chunk_value in chunk_values {
        let chunk_object = chunk_value
            .as_object()
            .ok_or(ChunkStateError::InvalidField("chunks"))?;

        let index = chunk_object
            .get("index")
            .ok_or(ChunkStateError::MissingField("chunks[].index"))?
            .as_u64()
            .ok_or(ChunkStateError::InvalidField("chunks[].index"))?;

        let hash_text = chunk_object
            .get("blake3")
            .ok_or(ChunkStateError::MissingField("chunks[].blake3"))?
            .as_str()
            .ok_or(ChunkStateError::InvalidField("chunks[].blake3"))?;

        let hash = parse_chunk_hash(hash_text).ok_or(ChunkStateError::InvalidChunkHash(index))?;

        chunks.push(RecordedChunk { index, hash });
    }

    ChunkState::new(layout, chunks)
}

fn validate_recorded_chunks(
    layout: ChunkLayout,
    chunks: &[RecordedChunk],
) -> Result<(), ChunkStateError> {
    let mut previous_index = None;

    for chunk in chunks {
        if layout.range(chunk.index).is_none() {
            return Err(ChunkStateError::ChunkIndexOutOfRange(chunk.index));
        }

        if let Some(previous) = previous_index {
            if chunk.index == previous {
                return Err(ChunkStateError::DuplicateChunkIndex(chunk.index));
            }

            if chunk.index < previous {
                return Err(ChunkStateError::UnorderedChunkIndices {
                    previous,
                    current: chunk.index,
                });
            }
        }

        previous_index = Some(chunk.index);
    }

    Ok(())
}

fn encode_chunk_hash(hash: ChunkHash) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(CHUNK_HASH_LENGTH * 2);

    for byte in hash.as_bytes() {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0F) as usize] as char);
    }

    encoded
}

fn parse_chunk_hash(text: &str) -> Option<ChunkHash> {
    if text.len() != CHUNK_HASH_LENGTH * 2 {
        return None;
    }

    let mut bytes = [0u8; CHUNK_HASH_LENGTH];

    for (index, byte) in bytes.iter_mut().enumerate() {
        let high_index = index * 2;
        let low_index = high_index + 1;

        let high = decode_hex_digit(text.as_bytes()[high_index])?;
        let low = decode_hex_digit(text.as_bytes()[low_index])?;

        *byte = (high << 4) | low;
    }

    Some(ChunkHash::from_bytes(bytes))
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

    fn hash(byte: u8) -> ChunkHash {
        ChunkHash::from_bytes([byte; CHUNK_HASH_LENGTH])
    }

    fn sparse_state() -> ChunkState {
        let layout = ChunkLayout::new(20, 4).unwrap();

        ChunkState::new(
            layout,
            vec![
                RecordedChunk {
                    index: 0,
                    hash: hash(0x11),
                },
                RecordedChunk {
                    index: 2,
                    hash: hash(0x22),
                },
                RecordedChunk {
                    index: 4,
                    hash: hash(0xAB),
                },
            ],
        )
        .unwrap()
    }

    #[test]
    fn chunk_state_round_trip() {
        let original = sparse_state();

        let encoded = encode_chunk_state(&original).unwrap();
        let decoded = decode_chunk_state(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn encodes_chunk_hashes_as_lowercase_hex() {
        let state = sparse_state();
        let encoded = encode_chunk_state(&state).unwrap();
        let value: Value = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(
            value["chunks"][2]["blake3"],
            "abababababababababababababababababababababababababababababababab"
        );
        assert_eq!(value["format_version"], CHUNK_STATE_FORMAT_VERSION);
        assert_eq!(value["file_size"], 20);
        assert_eq!(value["chunk_size"], 4);
    }

    #[test]
    fn decoder_accepts_uppercase_hex() {
        let bytes = br#"
        {
            "format_version": 1,
            "file_size": 4,
            "chunk_size": 4,
            "chunks": [
                {
                    "index": 0,
                    "blake3": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                }
            ]
        }
        "#;

        let state = decode_chunk_state(bytes).unwrap();

        assert_eq!(state.hash(0), Some(hash(0xAA)));
    }

    #[test]
    fn preserves_sparse_chunk_indices() {
        let state = sparse_state();

        assert_eq!(state.recorded_chunks().len(), 3);
        assert_eq!(state.hash(0), Some(hash(0x11)));
        assert_eq!(state.hash(1), None);
        assert_eq!(state.hash(2), Some(hash(0x22)));
        assert_eq!(state.hash(3), None);
        assert_eq!(state.hash(4), Some(hash(0xAB)));
    }

    #[test]
    fn empty_file_accepts_empty_chunk_state() {
        let layout = ChunkLayout::new(0, 4).unwrap();
        let state = ChunkState::new(layout, Vec::new()).unwrap();

        let encoded = encode_chunk_state(&state).unwrap();
        let decoded = decode_chunk_state(&encoded).unwrap();

        assert_eq!(decoded.layout(), layout);
        assert!(decoded.recorded_chunks().is_empty());
    }

    #[test]
    fn rejects_zero_chunk_size() {
        let bytes = br#"
        {
            "format_version": 1,
            "file_size": 100,
            "chunk_size": 0,
            "chunks": []
        }
        "#;

        let error = decode_chunk_state(bytes).unwrap_err();

        assert!(matches!(error, ChunkStateError::InvalidField("chunk_size")));
    }

    #[test]
    fn rejects_unsupported_format_version() {
        let bytes = br#"
        {
            "format_version": 99,
            "file_size": 100,
            "chunk_size": 4,
            "chunks": []
        }
        "#;

        let error = decode_chunk_state(bytes).unwrap_err();

        assert!(matches!(
            error,
            ChunkStateError::UnsupportedFormatVersion(99)
        ));
    }

    #[test]
    fn rejects_invalid_chunk_hash() {
        let bytes = br#"
        {
            "format_version": 1,
            "file_size": 4,
            "chunk_size": 4,
            "chunks": [
                {
                    "index": 0,
                    "blake3": "not-a-blake3-hash"
                }
            ]
        }
        "#;

        let error = decode_chunk_state(bytes).unwrap_err();

        assert!(matches!(error, ChunkStateError::InvalidChunkHash(0)));
    }

    #[test]
    fn rejects_out_of_range_chunk_index() {
        let layout = ChunkLayout::new(8, 4).unwrap();

        let error = ChunkState::new(
            layout,
            vec![RecordedChunk {
                index: 2,
                hash: hash(0x11),
            }],
        )
        .unwrap_err();

        assert!(matches!(error, ChunkStateError::ChunkIndexOutOfRange(2)));
    }

    #[test]
    fn empty_file_rejects_recorded_chunk() {
        let layout = ChunkLayout::new(0, 4).unwrap();

        let error = ChunkState::new(
            layout,
            vec![RecordedChunk {
                index: 0,
                hash: hash(0x11),
            }],
        )
        .unwrap_err();

        assert!(matches!(error, ChunkStateError::ChunkIndexOutOfRange(0)));
    }

    #[test]
    fn rejects_duplicate_chunk_indices() {
        let layout = ChunkLayout::new(12, 4).unwrap();

        let error = ChunkState::new(
            layout,
            vec![
                RecordedChunk {
                    index: 1,
                    hash: hash(0x11),
                },
                RecordedChunk {
                    index: 1,
                    hash: hash(0x22),
                },
            ],
        )
        .unwrap_err();

        assert!(matches!(error, ChunkStateError::DuplicateChunkIndex(1)));
    }

    #[test]
    fn rejects_unordered_chunk_indices() {
        let layout = ChunkLayout::new(12, 4).unwrap();

        let error = ChunkState::new(
            layout,
            vec![
                RecordedChunk {
                    index: 2,
                    hash: hash(0x22),
                },
                RecordedChunk {
                    index: 1,
                    hash: hash(0x11),
                },
            ],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ChunkStateError::UnorderedChunkIndices {
                previous: 2,
                current: 1,
            }
        ));
    }
}
