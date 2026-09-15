use crate::chunk::ChunkLayout;
use crate::chunk_manifest::ChunkHash;
use serde_json::{Value, json};
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const CHUNK_STATE_FORMAT_VERSION: u64 = 1;

const CHUNK_HASH_LENGTH: usize = 32;
const CHUNK_STATE_SUFFIX: &str = ".warpchunks";
const CHUNK_STATE_TEMP_SUFFIX: &str = ".tmp";
const REVALIDATION_BUFFER_SIZE: usize = 64 * 1024;

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
    Io(io::Error),
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
            Self::Io(error) => {
                write!(formatter, "chunk state I/O error: {error}")
            }
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
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ChunkStateError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for ChunkStateError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub fn chunk_state_path(partial_path: &Path) -> PathBuf {
    append_suffix(partial_path, CHUNK_STATE_SUFFIX)
}

pub async fn write_chunk_state(
    partial_path: &Path,
    state: &ChunkState,
) -> Result<(), ChunkStateError> {
    let state_path = chunk_state_path(partial_path);
    let temporary_path = chunk_state_temporary_path(partial_path);
    let encoded = encode_chunk_state(state)?;

    /*
     * A previous crash may have left a stale
     * .warpchunks.tmp file behind.
     *
     * Temporary files are never considered valid
     * snapshots, so it is safe to remove one before
     * beginning a new write.
     */
    remove_if_exists(&temporary_path).await?;

    let write_result = async {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .await?;

        file.write_all(&encoded).await?;
        file.flush().await?;

        /*
         * sync_all() asks the operating system to
         * persist the temporary snapshot contents
         * before we expose the snapshot under its
         * final filename.
         *
         * This does not by itself guarantee directory
         * entry durability across sudden machine or
         * power failure.
         */
        file.sync_all().await?;

        drop(file);

        /*
         * Windows does not reliably allow rename()
         * to replace an existing target.
         *
         * Removing the previous snapshot first creates
         * a small crash window where the snapshot can
         * be absent. That is safe for correctness:
         * the .part remains the source of physical data,
         * and missing snapshot information only means
         * that some reusable chunks may need to be sent
         * again.
         */
        remove_if_exists_io(&state_path).await?;
        fs::rename(&temporary_path, &state_path).await?;

        Ok::<(), io::Error>(())
    }
    .await;

    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary_path).await;
        return Err(ChunkStateError::Io(error));
    }

    Ok(())
}

pub async fn read_chunk_state(partial_path: &Path) -> Result<ChunkState, ChunkStateError> {
    let state_path = chunk_state_path(partial_path);
    let bytes = fs::read(state_path).await?;

    decode_chunk_state(&bytes)
}

pub async fn revalidate_chunk_state(
    partial_path: &Path,
    state: &ChunkState,
) -> Result<ChunkState, ChunkStateError> {
    if state.recorded_chunks().is_empty() {
        return ChunkState::new(state.layout(), Vec::new());
    }

    let mut file = match fs::File::open(partial_path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return ChunkState::new(state.layout(), Vec::new());
        }
        Err(error) => return Err(error.into()),
    };
    let mut buffer = [0u8; REVALIDATION_BUFFER_SIZE];
    let mut verified_chunks = Vec::with_capacity(state.recorded_chunks().len());

    for recorded_chunk in state.recorded_chunks() {
        let Some(range) = state.layout().range(recorded_chunk.index) else {
            continue;
        };

        file.seek(io::SeekFrom::Start(range.offset)).await?;

        let mut hasher = blake3::Hasher::new();
        let mut remaining = range.length;

        while remaining > 0 {
            let read_length = usize::try_from(remaining)
                .unwrap_or(buffer.len())
                .min(buffer.len());
            let bytes_read = file.read(&mut buffer[..read_length]).await?;

            if bytes_read == 0 {
                break;
            }

            hasher.update(&buffer[..bytes_read]);
            remaining -= u64::try_from(bytes_read)
                .expect("read length must fit into u64 on supported targets");
        }

        if remaining == 0
            && ChunkHash::from_bytes(*hasher.finalize().as_bytes()) == recorded_chunk.hash
        {
            verified_chunks.push(*recorded_chunk);
        }
    }

    ChunkState::new(state.layout(), verified_chunks)
}

pub async fn remove_chunk_state(partial_path: &Path) -> Result<(), ChunkStateError> {
    let state_path = chunk_state_path(partial_path);
    let temporary_path = chunk_state_temporary_path(partial_path);

    remove_if_exists(&state_path).await?;
    remove_if_exists(&temporary_path).await?;

    Ok(())
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

fn chunk_state_temporary_path(partial_path: &Path) -> PathBuf {
    let state_path = chunk_state_path(partial_path);

    append_suffix(&state_path, CHUNK_STATE_TEMP_SUFFIX)
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_os_string();
    value.push(suffix);

    PathBuf::from(value)
}

async fn remove_if_exists(path: &Path) -> Result<(), ChunkStateError> {
    remove_if_exists_io(path).await?;

    Ok(())
}

async fn remove_if_exists_io(path: &Path) -> Result<(), io::Error> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
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
    use tempfile::tempdir;

    fn hash(byte: u8) -> ChunkHash {
        ChunkHash::from_bytes([byte; CHUNK_HASH_LENGTH])
    }

    fn chunk_hash(data: &[u8]) -> ChunkHash {
        ChunkHash::from_bytes(*blake3::hash(data).as_bytes())
    }

    fn state_for_chunks(data: &[u8], chunk_size: u64, indices: &[u64]) -> ChunkState {
        let layout = ChunkLayout::new(u64::try_from(data.len()).unwrap(), chunk_size).unwrap();
        let chunks = indices
            .iter()
            .map(|&index| {
                let range = layout.range(index).unwrap();
                let start = usize::try_from(range.offset).unwrap();
                let end = usize::try_from(range.offset + range.length).unwrap();

                RecordedChunk {
                    index,
                    hash: chunk_hash(&data[start..end]),
                }
            })
            .collect();

        ChunkState::new(layout, chunks).unwrap()
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

    fn replacement_state() -> ChunkState {
        let layout = ChunkLayout::new(20, 4).unwrap();

        ChunkState::new(
            layout,
            vec![
                RecordedChunk {
                    index: 1,
                    hash: hash(0x33),
                },
                RecordedChunk {
                    index: 3,
                    hash: hash(0x44),
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

    #[test]
    fn derives_chunk_state_path_from_partial_path() {
        let partial_path = Path::new("received").join("video.mkv.part");
        let state_path = chunk_state_path(&partial_path);

        assert_eq!(
            state_path,
            Path::new("received").join("video.mkv.part.warpchunks")
        );
    }

    #[tokio::test]
    async fn writes_and_reads_chunk_state_file() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let original = sparse_state();

        write_chunk_state(&partial_path, &original).await.unwrap();

        let state_path = chunk_state_path(&partial_path);

        assert!(state_path.exists(), "chunk state file was not created");

        let loaded = read_chunk_state(&partial_path).await.unwrap();

        assert_eq!(loaded, original);
    }

    #[tokio::test]
    async fn replaces_existing_chunk_state_file() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let first = sparse_state();
        let replacement = replacement_state();

        write_chunk_state(&partial_path, &first).await.unwrap();
        write_chunk_state(&partial_path, &replacement)
            .await
            .unwrap();

        let loaded = read_chunk_state(&partial_path).await.unwrap();

        assert_eq!(loaded, replacement);
    }

    #[tokio::test]
    async fn write_discards_stale_temporary_chunk_state_file() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let temporary_path = chunk_state_temporary_path(&partial_path);
        let state = sparse_state();

        fs::write(&temporary_path, b"stale temporary chunk state")
            .await
            .unwrap();

        assert!(temporary_path.exists());

        write_chunk_state(&partial_path, &state).await.unwrap();

        assert!(!temporary_path.exists());

        let loaded = read_chunk_state(&partial_path).await.unwrap();

        assert_eq!(loaded, state);
    }

    #[tokio::test]
    async fn removes_chunk_state_file_and_stale_temporary_file() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let state = sparse_state();

        write_chunk_state(&partial_path, &state).await.unwrap();

        let state_path = chunk_state_path(&partial_path);
        let temporary_path = chunk_state_temporary_path(&partial_path);

        fs::write(&temporary_path, b"stale temporary chunk state")
            .await
            .unwrap();

        assert!(state_path.exists());
        assert!(temporary_path.exists());

        remove_chunk_state(&partial_path).await.unwrap();

        assert!(
            !state_path.exists(),
            "chunk state file remained after removal"
        );
        assert!(
            !temporary_path.exists(),
            "temporary chunk state file remained after removal"
        );
    }

    #[tokio::test]
    async fn rejects_corrupted_chunk_state_file() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let state_path = chunk_state_path(&partial_path);

        fs::write(&state_path, b"{ this is not valid JSON")
            .await
            .unwrap();

        let error = read_chunk_state(&partial_path).await.unwrap_err();

        assert!(matches!(error, ChunkStateError::Json(_)));
    }

    #[tokio::test]
    async fn revalidation_keeps_matching_recorded_chunks() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let data = b"abcdefghijkl";
        let state = state_for_chunks(data, 4, &[0, 1, 2]);

        fs::write(&partial_path, data).await.unwrap();

        let revalidated = revalidate_chunk_state(&partial_path, &state).await.unwrap();

        assert_eq!(revalidated, state);
    }

    #[tokio::test]
    async fn revalidation_removes_changed_recorded_chunks() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let state = state_for_chunks(b"abcdefghijkl", 4, &[0, 1, 2]);

        fs::write(&partial_path, b"abcdWXYZijkl").await.unwrap();

        let revalidated = revalidate_chunk_state(&partial_path, &state).await.unwrap();
        let expected = state_for_chunks(b"abcdefghijkl", 4, &[0, 2]);

        assert_eq!(revalidated, expected);
    }

    #[tokio::test]
    async fn revalidation_keeps_complete_chunks_when_partial_file_is_truncated() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let state = state_for_chunks(b"abcdefghijkl", 4, &[0, 1, 2]);

        fs::write(&partial_path, b"abcdefghij").await.unwrap();

        let revalidated = revalidate_chunk_state(&partial_path, &state).await.unwrap();

        assert_eq!(
            revalidated.recorded_chunks(),
            &state.recorded_chunks()[0..2]
        );
    }

    #[tokio::test]
    async fn revalidation_returns_empty_state_when_partial_file_is_missing() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let state = state_for_chunks(b"abcdefghijkl", 4, &[0, 2]);

        let revalidated = revalidate_chunk_state(&partial_path, &state).await.unwrap();

        assert_eq!(revalidated.layout(), state.layout());
        assert!(revalidated.recorded_chunks().is_empty());
    }

    #[tokio::test]
    async fn revalidation_preserves_sparse_ordered_records() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let data = b"abcdefghijklmnopqrst";
        let state = state_for_chunks(data, 4, &[0, 2, 4]);

        fs::write(&partial_path, data).await.unwrap();

        let revalidated = revalidate_chunk_state(&partial_path, &state).await.unwrap();

        assert_eq!(revalidated.recorded_chunks(), state.recorded_chunks());
    }

    #[tokio::test]
    async fn revalidation_ignores_corruption_in_unrecorded_chunks() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let state = state_for_chunks(b"abcdefghijkl", 4, &[0, 2]);

        fs::write(&partial_path, b"abcdWXYZijkl").await.unwrap();

        let revalidated = revalidate_chunk_state(&partial_path, &state).await.unwrap();

        assert_eq!(revalidated, state);
    }

    #[tokio::test]
    async fn revalidation_of_empty_state_does_not_require_partial_file() {
        let temp = tempdir().unwrap();
        let partial_path = temp.path().join("arquivo.bin.part");
        let layout = ChunkLayout::new(12, 4).unwrap();
        let state = ChunkState::new(layout, Vec::new()).unwrap();

        let revalidated = revalidate_chunk_state(&partial_path, &state).await.unwrap();

        assert_eq!(revalidated, state);
    }
}
