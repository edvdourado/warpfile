use std::error::Error;
use std::fmt;
use std::io;
use std::path::Path;

use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::chunk::ChunkRange;
use crate::chunk_manifest::ChunkHash;
use crate::chunk_state::{ChunkState, ChunkStateError, write_chunk_state};
use crate::protocol::{ChunkStartV03, DataV03};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedChunk {
    pub index: u64,
    pub hash: ChunkHash,
}

#[derive(Debug)]
pub enum ReceiverV03Error {
    Io(io::Error),
    PartialNotRegularFile,
    ChunkIndexOutOfRange(u64),
    ChunkAlreadyActive,
    ChunkPendingPersistence { index: u64 },
    NonIncreasingChunkIndex { previous: u64, current: u64 },
    AlreadyVerifiedChunk(u64),
    NoPendingVerifiedChunk,
    DataWithoutActiveChunk,
    UnexpectedDataOffset { expected: u64, actual: u64 },
    DataLengthOverflow,
    DataRangeOverflow,
    DataExceedsFileSize { end: u64, file_size: u64 },
    DataCrossesChunkBoundary { end: u64, chunk_end: u64 },
    ChunkHashMismatch { index: u64 },
    ChunkState(ChunkStateError),
}

impl fmt::Display for ReceiverV03Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "WFP/0.3 receiver I/O error: {error}"),
            Self::PartialNotRegularFile => {
                formatter.write_str("partial path is not a regular file")
            }
            Self::ChunkIndexOutOfRange(index) => {
                write!(formatter, "chunk index is out of range: {index}")
            }
            Self::ChunkAlreadyActive => formatter.write_str("a chunk is already active"),
            Self::ChunkPendingPersistence { index } => {
                write!(
                    formatter,
                    "chunk {index} is waiting for durable persistence"
                )
            }
            Self::NonIncreasingChunkIndex { previous, current } => {
                write!(
                    formatter,
                    "chunk index {current} does not follow {previous}"
                )
            }
            Self::AlreadyVerifiedChunk(index) => {
                write!(
                    formatter,
                    "chunk {index} already matches the receiver inventory"
                )
            }
            Self::NoPendingVerifiedChunk => {
                formatter.write_str("no verified chunk is waiting for persistence")
            }
            Self::DataWithoutActiveChunk => {
                formatter.write_str("DATA arrived without an active chunk")
            }
            Self::UnexpectedDataOffset { expected, actual } => {
                write!(
                    formatter,
                    "DATA offset {actual} does not match expected {expected}"
                )
            }
            Self::DataLengthOverflow => formatter.write_str("DATA length does not fit in u64"),
            Self::DataRangeOverflow => formatter.write_str("DATA range overflows u64"),
            Self::DataExceedsFileSize { end, file_size } => {
                write!(formatter, "DATA end {end} exceeds file size {file_size}")
            }
            Self::DataCrossesChunkBoundary { end, chunk_end } => {
                write!(formatter, "DATA end {end} exceeds chunk end {chunk_end}")
            }
            Self::ChunkHashMismatch { index } => {
                write!(formatter, "chunk {index} does not match its declared hash")
            }
            Self::ChunkState(error) => write!(formatter, "WFP/0.3 chunk state error: {error}"),
        }
    }
}

impl Error for ReceiverV03Error {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::ChunkState(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ReceiverV03Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ChunkStateError> for ReceiverV03Error {
    fn from(error: ChunkStateError) -> Self {
        Self::ChunkState(error)
    }
}

pub async fn prepare_v03_partial_file(
    partial_path: &Path,
    file_size: u64,
) -> Result<fs::File, ReceiverV03Error> {
    let existed = fs::try_exists(partial_path).await?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(partial_path)
        .await?;

    let metadata = file.metadata().await?;
    if !metadata.is_file() {
        return Err(ReceiverV03Error::PartialNotRegularFile);
    }
    if !existed || metadata.len() != file_size {
        file.set_len(file_size).await?;
    }

    Ok(file)
}

struct IncomingChunk {
    range: ChunkRange,
    expected_hash: ChunkHash,
    next_offset: u64,
    hasher: blake3::Hasher,
}

pub struct ChunkReceiverV03 {
    chunk_state: ChunkState,
    last_chunk_index: Option<u64>,
    active: Option<IncomingChunk>,
    pending_verified: Option<VerifiedChunk>,
}

impl ChunkReceiverV03 {
    pub fn new(chunk_state: ChunkState) -> Self {
        Self {
            chunk_state,
            last_chunk_index: None,
            active: None,
            pending_verified: None,
        }
    }

    pub fn begin_chunk(&mut self, chunk_start: ChunkStartV03) -> Result<(), ReceiverV03Error> {
        if let Some(pending) = self.pending_verified {
            return Err(ReceiverV03Error::ChunkPendingPersistence {
                index: pending.index,
            });
        }

        if self.active.is_some() {
            return Err(ReceiverV03Error::ChunkAlreadyActive);
        }

        let range = self
            .chunk_state
            .layout()
            .range(chunk_start.chunk_index)
            .ok_or(ReceiverV03Error::ChunkIndexOutOfRange(
                chunk_start.chunk_index,
            ))?;

        if let Some(previous) = self.last_chunk_index
            && chunk_start.chunk_index <= previous
        {
            return Err(ReceiverV03Error::NonIncreasingChunkIndex {
                previous,
                current: chunk_start.chunk_index,
            });
        }

        if self.chunk_state.hash(chunk_start.chunk_index) == Some(chunk_start.expected_hash) {
            return Err(ReceiverV03Error::AlreadyVerifiedChunk(
                chunk_start.chunk_index,
            ));
        }

        self.last_chunk_index = Some(chunk_start.chunk_index);
        self.active = Some(IncomingChunk {
            range,
            expected_hash: chunk_start.expected_hash,
            next_offset: range.offset,
            hasher: blake3::Hasher::new(),
        });

        Ok(())
    }

    pub async fn write_data(
        &mut self,
        file: &mut fs::File,
        data: &DataV03,
    ) -> Result<Option<VerifiedChunk>, ReceiverV03Error> {
        let active = self
            .active
            .as_mut()
            .ok_or(ReceiverV03Error::DataWithoutActiveChunk)?;

        if data.absolute_offset != active.next_offset {
            return Err(ReceiverV03Error::UnexpectedDataOffset {
                expected: active.next_offset,
                actual: data.absolute_offset,
            });
        }

        let data_length =
            u64::try_from(data.data.len()).map_err(|_| ReceiverV03Error::DataLengthOverflow)?;
        let end = data
            .absolute_offset
            .checked_add(data_length)
            .ok_or(ReceiverV03Error::DataRangeOverflow)?;
        let chunk_end = active
            .range
            .offset
            .checked_add(active.range.length)
            .ok_or(ReceiverV03Error::DataRangeOverflow)?;
        let file_size = self.chunk_state.layout().file_size();

        if end > file_size {
            return Err(ReceiverV03Error::DataExceedsFileSize { end, file_size });
        }
        if end > chunk_end {
            return Err(ReceiverV03Error::DataCrossesChunkBoundary { end, chunk_end });
        }

        file.seek(io::SeekFrom::Start(data.absolute_offset)).await?;
        file.write_all(&data.data).await?;

        active.hasher.update(&data.data);
        active.next_offset = end;

        if end != chunk_end {
            return Ok(None);
        }

        let hash = ChunkHash::from_bytes(*active.hasher.finalize().as_bytes());

        if hash != active.expected_hash {
            return Err(ReceiverV03Error::ChunkHashMismatch {
                index: active.range.index,
            });
        }

        let verified = VerifiedChunk {
            index: active.range.index,
            hash,
        };
        let _ = self.active.take();
        self.pending_verified = Some(verified);

        Ok(Some(verified))
    }

    pub async fn persist_verified_chunk(
        &mut self,
        file: &mut fs::File,
        partial_path: &Path,
    ) -> Result<(), ReceiverV03Error> {
        let verified = self
            .pending_verified
            .ok_or(ReceiverV03Error::NoPendingVerifiedChunk)?;
        let mut candidate = self.chunk_state.clone();
        candidate.record_verified_chunk(verified.index, verified.hash)?;

        file.flush().await?;
        file.sync_data().await?;
        write_chunk_state(partial_path, &candidate).await?;

        self.chunk_state = candidate;
        self.pending_verified = None;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    use crate::chunk::ChunkLayout;
    use crate::chunk_state::{ChunkStateError, RecordedChunk, chunk_state_path, read_chunk_state};

    fn hash(data: &[u8]) -> ChunkHash {
        ChunkHash::from_bytes(*blake3::hash(data).as_bytes())
    }

    fn state(file_size: u64, chunk_size: u64, chunks: Vec<RecordedChunk>) -> ChunkState {
        ChunkState::new(ChunkLayout::new(file_size, chunk_size).unwrap(), chunks).unwrap()
    }

    fn chunk_start(index: u64, bytes: &[u8]) -> ChunkStartV03 {
        ChunkStartV03 {
            chunk_index: index,
            expected_hash: hash(bytes),
        }
    }

    fn data(offset: u64, bytes: &[u8]) -> DataV03 {
        DataV03 {
            absolute_offset: offset,
            data: bytes.to_vec(),
        }
    }

    #[tokio::test]
    async fn prepares_missing_partial_at_the_offered_size_without_touching_chunk_state() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let state_path = chunk_state_path(&partial);
        fs::write(&state_path, b"unchanged").await.unwrap();

        let file = prepare_v03_partial_file(&partial, 7).await.unwrap();

        assert_eq!(file.metadata().await.unwrap().len(), 7);
        assert_eq!(fs::read(&state_path).await.unwrap(), b"unchanged");
    }

    #[tokio::test]
    async fn extends_short_partial_without_losing_its_prefix() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        fs::write(&partial, b"prefix").await.unwrap();

        let file = prepare_v03_partial_file(&partial, 10).await.unwrap();

        assert_eq!(file.metadata().await.unwrap().len(), 10);
        assert_eq!(&fs::read(&partial).await.unwrap()[..6], b"prefix");
    }

    #[tokio::test]
    async fn keeps_an_exact_size_partial_unchanged() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        fs::write(&partial, b"unchanged").await.unwrap();

        let file = prepare_v03_partial_file(&partial, 9).await.unwrap();

        assert_eq!(file.metadata().await.unwrap().len(), 9);
        assert_eq!(fs::read(&partial).await.unwrap(), b"unchanged");
    }

    #[tokio::test]
    async fn truncates_long_partial_without_losing_the_retained_range() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        fs::write(&partial, b"retain-drop").await.unwrap();

        let file = prepare_v03_partial_file(&partial, 6).await.unwrap();

        assert_eq!(file.metadata().await.unwrap().len(), 6);
        assert_eq!(fs::read(&partial).await.unwrap(), b"retain");
    }

    #[tokio::test]
    async fn prepares_an_empty_file() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("empty.part");

        let file = prepare_v03_partial_file(&partial, 0).await.unwrap();

        assert_eq!(file.metadata().await.unwrap().len(), 0);
    }

    #[test]
    fn starts_only_existing_increasing_chunks_that_are_not_already_verified() {
        let mut receiver = ChunkReceiverV03::new(state(8, 4, Vec::new()));

        receiver.begin_chunk(chunk_start(1, b"efgh")).unwrap();
        assert_eq!(receiver.active.as_ref().unwrap().range.offset, 4);
        assert_eq!(receiver.active.as_ref().unwrap().next_offset, 4);
        assert!(matches!(
            receiver.begin_chunk(chunk_start(2, b"ijkl")),
            Err(ReceiverV03Error::ChunkAlreadyActive)
        ));

        let mut out_of_range = ChunkReceiverV03::new(state(8, 4, Vec::new()));
        assert!(matches!(
            out_of_range.begin_chunk(chunk_start(2, b"ijkl")),
            Err(ReceiverV03Error::ChunkIndexOutOfRange(2))
        ));

        let matching = hash(b"abcd");
        let mut already_verified = ChunkReceiverV03::new(state(
            8,
            4,
            vec![RecordedChunk {
                index: 0,
                hash: matching,
            }],
        ));
        assert!(matches!(
            already_verified.begin_chunk(ChunkStartV03 {
                chunk_index: 0,
                expected_hash: matching,
            }),
            Err(ReceiverV03Error::AlreadyVerifiedChunk(0))
        ));
    }

    #[tokio::test]
    async fn allows_a_different_candidate_hash_and_rejects_non_increasing_indices() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 8).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(
            8,
            4,
            vec![RecordedChunk {
                index: 1,
                hash: hash(b"old!"),
            }],
        ));

        receiver.begin_chunk(chunk_start(1, b"efgh")).unwrap();
        assert!(
            receiver
                .write_data(&mut file, &data(4, b"efgh"))
                .await
                .unwrap()
                .is_some()
        );
        receiver
            .persist_verified_chunk(&mut file, &partial)
            .await
            .unwrap();
        assert!(matches!(
            receiver.begin_chunk(chunk_start(0, b"abcd")),
            Err(ReceiverV03Error::NonIncreasingChunkIndex {
                previous: 1,
                current: 0,
            })
        ));
    }

    #[tokio::test]
    async fn requires_data_to_be_contiguous_and_within_the_active_chunk() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 8).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(8, 4, Vec::new()));

        assert!(matches!(
            receiver.write_data(&mut file, &data(0, b"a")).await,
            Err(ReceiverV03Error::DataWithoutActiveChunk)
        ));
        receiver.begin_chunk(chunk_start(0, b"abcd")).unwrap();
        assert!(matches!(
            receiver.write_data(&mut file, &data(1, b"a")).await,
            Err(ReceiverV03Error::UnexpectedDataOffset {
                expected: 0,
                actual: 1,
            })
        ));
        assert!(matches!(
            receiver.write_data(&mut file, &data(0, b"abcde")).await,
            Err(ReceiverV03Error::DataCrossesChunkBoundary {
                end: 5,
                chunk_end: 4,
            })
        ));
        assert!(
            receiver
                .write_data(&mut file, &data(0, b"ab"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            receiver.write_data(&mut file, &data(1, b"c")).await,
            Err(ReceiverV03Error::UnexpectedDataOffset {
                expected: 2,
                actual: 1,
            })
        ));

        let mut final_chunk = ChunkReceiverV03::new(state(8, 4, Vec::new()));
        final_chunk.begin_chunk(chunk_start(1, b"efgh")).unwrap();
        assert!(matches!(
            final_chunk.write_data(&mut file, &data(4, b"efghi")).await,
            Err(ReceiverV03Error::DataExceedsFileSize {
                end: 9,
                file_size: 8,
            })
        ));
    }

    #[tokio::test]
    async fn detects_data_range_overflow_before_writing() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 0).await.unwrap();
        let layout = ChunkLayout::new(u64::MAX, 2).unwrap();
        let last_index = layout.chunk_count() - 1;
        let mut receiver = ChunkReceiverV03::new(ChunkState::new(layout, Vec::new()).unwrap());
        receiver.begin_chunk(chunk_start(last_index, b"x")).unwrap();

        assert!(matches!(
            receiver
                .write_data(&mut file, &data(u64::MAX - 1, b"xy"))
                .await,
            Err(ReceiverV03Error::DataRangeOverflow)
        ));
    }

    #[tokio::test]
    async fn verifies_multi_frame_chunks_and_writes_at_absolute_offsets() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 8).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(8, 4, Vec::new()));
        receiver.begin_chunk(chunk_start(1, b"efgh")).unwrap();

        assert_eq!(
            receiver
                .write_data(&mut file, &data(4, b"ef"))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            receiver
                .write_data(&mut file, &data(6, b"gh"))
                .await
                .unwrap(),
            Some(VerifiedChunk {
                index: 1,
                hash: hash(b"efgh"),
            })
        );
        assert!(receiver.active.is_none());
        assert_eq!(
            receiver.pending_verified,
            Some(VerifiedChunk {
                index: 1,
                hash: hash(b"efgh"),
            })
        );
        assert!(receiver.chunk_state.recorded_chunks().is_empty());
        assert!(!chunk_state_path(&partial).exists());

        file.seek(io::SeekFrom::Start(4)).await.unwrap();
        let mut bytes = [0u8; 4];
        file.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, *b"efgh");
    }

    #[tokio::test]
    async fn rejects_a_completed_chunk_with_the_wrong_declared_hash() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 8).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(8, 4, Vec::new()));
        receiver.begin_chunk(chunk_start(0, b"wxyz")).unwrap();

        let completion = receiver.write_data(&mut file, &data(0, b"abcd")).await;

        assert!(matches!(
            completion,
            Err(ReceiverV03Error::ChunkHashMismatch { index: 0 })
        ));
        assert!(matches!(
            receiver.begin_chunk(chunk_start(1, b"efgh")),
            Err(ReceiverV03Error::ChunkAlreadyActive)
        ));
        assert!(receiver.pending_verified.is_none());
        assert!(receiver.chunk_state.recorded_chunks().is_empty());
        assert!(!chunk_state_path(&partial).exists());
    }

    #[tokio::test]
    async fn persists_verified_chunk_before_starting_the_next_chunk() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 12).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(
            12,
            4,
            vec![RecordedChunk {
                index: 0,
                hash: hash(b"abcd"),
            }],
        ));

        receiver.begin_chunk(chunk_start(1, b"efgh")).unwrap();
        assert_eq!(
            receiver
                .write_data(&mut file, &data(4, b"efgh"))
                .await
                .unwrap(),
            Some(VerifiedChunk {
                index: 1,
                hash: hash(b"efgh"),
            })
        );
        assert!(matches!(
            receiver.begin_chunk(chunk_start(2, b"ijkl")),
            Err(ReceiverV03Error::ChunkPendingPersistence { index: 1 })
        ));
        assert!(receiver.active.is_none());
        assert_eq!(receiver.chunk_state.hash(1), None);
        assert!(!chunk_state_path(&partial).exists());

        receiver
            .persist_verified_chunk(&mut file, &partial)
            .await
            .unwrap();

        assert!(receiver.pending_verified.is_none());
        assert_eq!(receiver.chunk_state.hash(1), Some(hash(b"efgh")));
        assert_eq!(
            read_chunk_state(&partial).await.unwrap(),
            receiver.chunk_state
        );
        receiver.begin_chunk(chunk_start(2, b"ijkl")).unwrap();
    }

    #[tokio::test]
    async fn persistence_replaces_divergent_hash_in_strict_sparse_order() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 12).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(
            12,
            4,
            vec![
                RecordedChunk {
                    index: 0,
                    hash: hash(b"abcd"),
                },
                RecordedChunk {
                    index: 1,
                    hash: hash(b"old!"),
                },
                RecordedChunk {
                    index: 2,
                    hash: hash(b"ijkl"),
                },
            ],
        ));

        receiver.begin_chunk(chunk_start(1, b"efgh")).unwrap();
        receiver
            .write_data(&mut file, &data(4, b"efgh"))
            .await
            .unwrap();
        receiver
            .persist_verified_chunk(&mut file, &partial)
            .await
            .unwrap();

        let expected = [
            RecordedChunk {
                index: 0,
                hash: hash(b"abcd"),
            },
            RecordedChunk {
                index: 1,
                hash: hash(b"efgh"),
            },
            RecordedChunk {
                index: 2,
                hash: hash(b"ijkl"),
            },
        ];
        assert_eq!(receiver.chunk_state.recorded_chunks(), expected);
        assert_eq!(
            read_chunk_state(&partial).await.unwrap().recorded_chunks(),
            expected
        );
    }

    #[tokio::test]
    async fn persistence_requires_pending_verified_chunk() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let mut file = prepare_v03_partial_file(&partial, 4).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(4, 4, Vec::new()));

        assert!(matches!(
            receiver.persist_verified_chunk(&mut file, &partial).await,
            Err(ReceiverV03Error::NoPendingVerifiedChunk)
        ));
        assert!(receiver.chunk_state.recorded_chunks().is_empty());
        assert!(!chunk_state_path(&partial).exists());
    }

    #[tokio::test]
    async fn failed_snapshot_persistence_keeps_durable_progress_pending() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let state_path = chunk_state_path(&partial);
        let mut file = prepare_v03_partial_file(&partial, 8).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(8, 4, Vec::new()));
        receiver.begin_chunk(chunk_start(0, b"abcd")).unwrap();
        receiver
            .write_data(&mut file, &data(0, b"abcd"))
            .await
            .unwrap();
        fs::create_dir(&state_path).await.unwrap();

        assert!(matches!(
            receiver.persist_verified_chunk(&mut file, &partial).await,
            Err(ReceiverV03Error::ChunkState(ChunkStateError::Io(_)))
        ));
        assert_eq!(receiver.chunk_state.hash(0), None);
        assert_eq!(
            receiver.pending_verified,
            Some(VerifiedChunk {
                index: 0,
                hash: hash(b"abcd"),
            })
        );
        assert!(matches!(
            receiver.begin_chunk(chunk_start(1, b"efgh")),
            Err(ReceiverV03Error::ChunkPendingPersistence { index: 0 })
        ));
        assert!(state_path.is_dir());
    }

    #[tokio::test]
    async fn supports_final_short_and_single_byte_chunks_without_altering_retained_ranges() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        fs::write(&partial, b"keep????tail").await.unwrap();
        let mut file = prepare_v03_partial_file(&partial, 12).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(12, 4, Vec::new()));
        receiver.begin_chunk(chunk_start(1, b"MID!")).unwrap();
        assert!(
            receiver
                .write_data(&mut file, &data(4, b"MID!"))
                .await
                .unwrap()
                .is_some()
        );

        assert_eq!(fs::read(&partial).await.unwrap(), b"keepMID!tail");

        let mut one_byte = ChunkReceiverV03::new(state(1, 1, Vec::new()));
        let single = temp.path().join("single.part");
        let mut single_file = prepare_v03_partial_file(&single, 1).await.unwrap();
        one_byte.begin_chunk(chunk_start(0, b"z")).unwrap();
        assert_eq!(
            one_byte
                .write_data(&mut single_file, &data(0, b"z"))
                .await
                .unwrap(),
            Some(VerifiedChunk {
                index: 0,
                hash: hash(b"z"),
            })
        );
    }
}
