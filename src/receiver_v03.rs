use std::error::Error;
use std::fmt;
use std::io;
use std::mem;
use std::path::{Path, PathBuf};

use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::chunk::{ChunkLayout, ChunkLayoutError, ChunkRange};
use crate::chunk_manifest::ChunkHash;
use crate::chunk_state::{ChunkState, ChunkStateError, prepare_chunk_inventory, write_chunk_state};
use crate::protocol::frame::{FrameError, MAX_PAYLOAD_LENGTH, WFP_VERSION_V03};
use crate::protocol::{
    CHUNK_HASH_RECORD_LENGTH, ChunkHashRecord, ChunkHashesBatch, ChunkHashesError, ChunkStartV03,
    ChunkStartV03Error, DataV03, DataV03Error, FileOfferV03, Frame, MessageType, ProtocolIoError,
    ResumeRequestV03, decode_chunk_start_v03, decode_data_v03, encode_chunk_hashes,
    encode_resume_v03, read_frame_for_version, write_frame,
};

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

#[derive(Debug)]
pub enum ReceiverV03PreparationError {
    ChunkLayout(ChunkLayoutError),
    ChunkState(ChunkStateError),
    Receiver(ReceiverV03Error),
    ChunkRecordCountOverflow(usize),
}

impl fmt::Display for ReceiverV03PreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChunkLayout(error) => write!(formatter, "invalid WFP/0.3 chunk layout: {error}"),
            Self::ChunkState(error) => write!(formatter, "WFP/0.3 chunk inventory error: {error}"),
            Self::Receiver(error) => {
                write!(formatter, "WFP/0.3 receiver preparation error: {error}")
            }
            Self::ChunkRecordCountOverflow(count) => {
                write!(formatter, "chunk record count does not fit in u64: {count}")
            }
        }
    }
}

impl Error for ReceiverV03PreparationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ChunkLayout(error) => Some(error),
            Self::ChunkState(error) => Some(error),
            Self::Receiver(error) => Some(error),
            Self::ChunkRecordCountOverflow(_) => None,
        }
    }
}

impl From<ChunkLayoutError> for ReceiverV03PreparationError {
    fn from(error: ChunkLayoutError) -> Self {
        Self::ChunkLayout(error)
    }
}

impl From<ChunkStateError> for ReceiverV03PreparationError {
    fn from(error: ChunkStateError) -> Self {
        Self::ChunkState(error)
    }
}

impl From<ReceiverV03Error> for ReceiverV03PreparationError {
    fn from(error: ReceiverV03Error) -> Self {
        Self::Receiver(error)
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

pub struct PreparedReceiverV03 {
    partial_path: PathBuf,
    file: fs::File,
    receiver: ChunkReceiverV03,
    resume: ResumeRequestV03,
    chunk_hash_batches: Vec<ChunkHashesBatch>,
}

pub struct AcceptedReceiverV03 {
    partial_path: PathBuf,
    file: fs::File,
    receiver: ChunkReceiverV03,
}

#[allow(
    dead_code,
    reason = "the next WFP/0.3 final-verification phase will consume this state"
)]
pub struct ReceivedCompleteV03 {
    file: fs::File,
    partial_path: PathBuf,
    receiver: ChunkReceiverV03,
    sender_file_hash: ChunkHash,
}

#[derive(Debug)]
pub enum ReceiverV03TransferError {
    Protocol(ProtocolIoError),
    ChunkStart(ChunkStartV03Error),
    Data(DataV03Error),
    Receiver(ReceiverV03Error),
    UnexpectedMessageType(MessageType),
    InvalidCompletePayload(usize),
    CompleteWhileChunkActive { index: u64 },
    CompleteWhileChunkPendingPersistence { index: u64 },
    VerifiedChunkCountOverflow(usize),
    IncompleteVerifiedCoverage { expected: u64, actual: u64 },
}

impl fmt::Display for ReceiverV03TransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => write!(formatter, "WFP/0.3 transfer I/O error: {error}"),
            Self::ChunkStart(error) => write!(formatter, "WFP/0.3 CHUNK_START error: {error}"),
            Self::Data(error) => write!(formatter, "WFP/0.3 DATA error: {error}"),
            Self::Receiver(error) => write!(formatter, "WFP/0.3 transfer receiver error: {error}"),
            Self::UnexpectedMessageType(message_type) => write!(
                formatter,
                "unexpected WFP/0.3 transfer message type 0x{:02X}",
                *message_type as u8
            ),
            Self::InvalidCompletePayload(length) => write!(
                formatter,
                "WFP/0.3 COMPLETE payload must contain exactly 32 bytes, received {length}"
            ),
            Self::CompleteWhileChunkActive { index } => {
                write!(
                    formatter,
                    "WFP/0.3 COMPLETE arrived while chunk {index} is incomplete"
                )
            }
            Self::CompleteWhileChunkPendingPersistence { index } => write!(
                formatter,
                "WFP/0.3 COMPLETE arrived while chunk {index} is waiting for persistence"
            ),
            Self::VerifiedChunkCountOverflow(count) => write!(
                formatter,
                "verified WFP/0.3 chunk count does not fit in u64: {count}"
            ),
            Self::IncompleteVerifiedCoverage { expected, actual } => write!(
                formatter,
                "WFP/0.3 transfer completed with {actual} of {expected} chunks verified"
            ),
        }
    }
}

impl Error for ReceiverV03TransferError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::ChunkStart(error) => Some(error),
            Self::Data(error) => Some(error),
            Self::Receiver(error) => Some(error),
            Self::UnexpectedMessageType(_)
            | Self::InvalidCompletePayload(_)
            | Self::CompleteWhileChunkActive { .. }
            | Self::CompleteWhileChunkPendingPersistence { .. }
            | Self::VerifiedChunkCountOverflow(_)
            | Self::IncompleteVerifiedCoverage { .. } => None,
        }
    }
}

impl From<ProtocolIoError> for ReceiverV03TransferError {
    fn from(error: ProtocolIoError) -> Self {
        Self::Protocol(error)
    }
}

impl From<ChunkStartV03Error> for ReceiverV03TransferError {
    fn from(error: ChunkStartV03Error) -> Self {
        Self::ChunkStart(error)
    }
}

impl From<DataV03Error> for ReceiverV03TransferError {
    fn from(error: DataV03Error) -> Self {
        Self::Data(error)
    }
}

impl From<ReceiverV03Error> for ReceiverV03TransferError {
    fn from(error: ReceiverV03Error) -> Self {
        Self::Receiver(error)
    }
}

#[derive(Debug)]
pub enum ReceiverV03NegotiationError {
    Frame(FrameError),
    ChunkHashes(ChunkHashesError),
    Protocol(ProtocolIoError),
    InvalidAcceptPayload(usize),
    UnexpectedMessageType(MessageType),
}

impl fmt::Display for ReceiverV03NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame(error) => write!(formatter, "WFP/0.3 inventory frame error: {error}"),
            Self::ChunkHashes(error) => {
                write!(formatter, "WFP/0.3 chunk inventory encoding error: {error}")
            }
            Self::Protocol(error) => write!(formatter, "WFP/0.3 inventory I/O error: {error}"),
            Self::InvalidAcceptPayload(length) => {
                write!(
                    formatter,
                    "WFP/0.3 ACCEPT payload must be empty, received {length} bytes"
                )
            }
            Self::UnexpectedMessageType(message_type) => write!(
                formatter,
                "expected WFP/0.3 ACCEPT after chunk inventory, received message type 0x{:02X}",
                *message_type as u8
            ),
        }
    }
}

impl Error for ReceiverV03NegotiationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Frame(error) => Some(error),
            Self::ChunkHashes(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::InvalidAcceptPayload(_) | Self::UnexpectedMessageType(_) => None,
        }
    }
}

impl From<FrameError> for ReceiverV03NegotiationError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

impl From<ChunkHashesError> for ReceiverV03NegotiationError {
    fn from(error: ChunkHashesError) -> Self {
        Self::ChunkHashes(error)
    }
}

impl From<ProtocolIoError> for ReceiverV03NegotiationError {
    fn from(error: ProtocolIoError) -> Self {
        Self::Protocol(error)
    }
}

impl PreparedReceiverV03 {
    pub async fn negotiate_inventory<S>(
        self,
        stream: &mut S,
    ) -> Result<AcceptedReceiverV03, ReceiverV03NegotiationError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let resume = Frame::new_for_version(
            WFP_VERSION_V03,
            MessageType::Resume,
            encode_resume_v03(&self.resume),
        )?;
        write_frame(stream, &resume).await?;

        for batch in &self.chunk_hash_batches {
            let frame = Frame::new_for_version(
                WFP_VERSION_V03,
                MessageType::ChunkHashes,
                encode_chunk_hashes(batch)?,
            )?;
            write_frame(stream, &frame).await?;
        }

        let response = read_frame_for_version(stream, WFP_VERSION_V03).await?;
        if response.message_type != MessageType::Accept {
            return Err(ReceiverV03NegotiationError::UnexpectedMessageType(
                response.message_type,
            ));
        }
        if !response.payload.is_empty() {
            return Err(ReceiverV03NegotiationError::InvalidAcceptPayload(
                response.payload.len(),
            ));
        }

        Ok(AcceptedReceiverV03 {
            partial_path: self.partial_path,
            file: self.file,
            receiver: self.receiver,
        })
    }
}

impl AcceptedReceiverV03 {
    pub async fn receive_transfer<S>(
        mut self,
        stream: &mut S,
    ) -> Result<ReceivedCompleteV03, ReceiverV03TransferError>
    where
        S: AsyncRead + Unpin,
    {
        loop {
            let frame = read_frame_for_version(stream, WFP_VERSION_V03).await?;

            match frame.message_type {
                MessageType::ChunkStart => {
                    let chunk_start = decode_chunk_start_v03(&frame.payload)?;
                    self.receiver.begin_chunk(chunk_start)?;
                }
                MessageType::Data => {
                    let data = decode_data_v03(&frame.payload)?;
                    if self
                        .receiver
                        .write_data(&mut self.file, &data)
                        .await?
                        .is_some()
                    {
                        self.receiver
                            .persist_verified_chunk(&mut self.file, &self.partial_path)
                            .await?;
                    }
                }
                MessageType::Complete => {
                    if let Some(active) = &self.receiver.active {
                        return Err(ReceiverV03TransferError::CompleteWhileChunkActive {
                            index: active.range.index,
                        });
                    }
                    if let Some(pending) = self.receiver.pending_verified {
                        return Err(
                            ReceiverV03TransferError::CompleteWhileChunkPendingPersistence {
                                index: pending.index,
                            },
                        );
                    }

                    let hash: [u8; 32] = frame.payload.as_slice().try_into().map_err(|_| {
                        ReceiverV03TransferError::InvalidCompletePayload(frame.payload.len())
                    })?;
                    let expected = self.receiver.chunk_state.layout().chunk_count();
                    let actual = self.receiver.chunk_state.recorded_chunks().len();
                    let actual = u64::try_from(actual).map_err(|_| {
                        ReceiverV03TransferError::VerifiedChunkCountOverflow(actual)
                    })?;
                    if actual != expected {
                        return Err(ReceiverV03TransferError::IncompleteVerifiedCoverage {
                            expected,
                            actual,
                        });
                    }

                    return Ok(ReceivedCompleteV03 {
                        file: self.file,
                        partial_path: self.partial_path,
                        receiver: self.receiver,
                        sender_file_hash: ChunkHash::from_bytes(hash),
                    });
                }
                message_type => {
                    return Err(ReceiverV03TransferError::UnexpectedMessageType(
                        message_type,
                    ));
                }
            }
        }
    }
}

pub async fn prepare_receiver_v03(
    partial_path: &Path,
    offer: &FileOfferV03,
) -> Result<PreparedReceiverV03, ReceiverV03PreparationError> {
    let layout = ChunkLayout::new(offer.file_size, offer.chunk_size)?;
    let chunk_state = prepare_chunk_inventory(partial_path, layout).await?;
    let file = prepare_v03_partial_file(partial_path, offer.file_size).await?;
    let receiver = ChunkReceiverV03::new(chunk_state);
    let chunk_record_count = receiver.chunk_state.recorded_chunks().len();
    let chunk_record_count = u64::try_from(chunk_record_count)
        .map_err(|_| ReceiverV03PreparationError::ChunkRecordCountOverflow(chunk_record_count))?;
    let chunk_hash_batches = chunk_hash_batches(&receiver.chunk_state);

    Ok(PreparedReceiverV03 {
        partial_path: partial_path.to_path_buf(),
        file,
        receiver,
        resume: ResumeRequestV03 { chunk_record_count },
        chunk_hash_batches,
    })
}

fn chunk_hash_batches(chunk_state: &ChunkState) -> Vec<ChunkHashesBatch> {
    let maximum_records = maximum_chunk_hash_records_per_batch();

    chunk_state
        .recorded_chunks()
        .chunks(maximum_records)
        .map(|records| ChunkHashesBatch {
            records: records
                .iter()
                .map(|record| ChunkHashRecord {
                    chunk_index: record.index,
                    hash: record.hash,
                })
                .collect(),
        })
        .collect()
}

fn maximum_chunk_hash_records_per_batch() -> usize {
    (MAX_PAYLOAD_LENGTH
        .checked_sub(mem::size_of::<u32>())
        .expect("WFP payload limit must accommodate a CHUNK_HASHES record count"))
        / CHUNK_HASH_RECORD_LENGTH
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

    use std::io::Cursor;

    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, DuplexStream, duplex};

    use crate::chunk::ChunkLayout;
    use crate::chunk_state::{
        ChunkStateError, RecordedChunk, chunk_state_path, read_chunk_state, write_chunk_state,
    };
    use crate::protocol::frame::{ACTIVE_WFP_VERSION, WFP_VERSION_V02};
    use crate::protocol::{
        DecodeError, decode_chunk_hashes, decode_resume_v03, encode_chunk_start_v03,
        encode_data_v03, encode_frame,
    };

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

    fn transfer_frame(message_type: MessageType, payload: Vec<u8>) -> Frame {
        Frame::new_for_version(WFP_VERSION_V03, message_type, payload).unwrap()
    }

    fn chunk_start_frame(index: u64, bytes: &[u8]) -> Frame {
        transfer_frame(
            MessageType::ChunkStart,
            encode_chunk_start_v03(&chunk_start(index, bytes)),
        )
    }

    fn data_frame(offset: u64, bytes: &[u8]) -> Frame {
        transfer_frame(
            MessageType::Data,
            encode_data_v03(&data(offset, bytes)).unwrap(),
        )
    }

    fn complete_frame(hash: ChunkHash) -> Frame {
        transfer_frame(MessageType::Complete, hash.as_bytes().to_vec())
    }

    fn encoded_frames(frames: &[Frame]) -> Vec<u8> {
        frames
            .iter()
            .flat_map(|frame| encode_frame(frame).unwrap())
            .collect()
    }

    async fn accepted_from_state(partial: &Path, state: ChunkState) -> AcceptedReceiverV03 {
        AcceptedReceiverV03 {
            partial_path: partial.to_path_buf(),
            file: prepare_v03_partial_file(partial, state.layout().file_size())
                .await
                .unwrap(),
            receiver: ChunkReceiverV03::new(state),
        }
    }

    fn offer(file_size: u64, chunk_size: u64) -> FileOfferV03 {
        FileOfferV03 {
            transfer_id: crate::protocol::TransferId::from_bytes([0xA5; 16]),
            filename: "archive.bin".to_string(),
            file_size,
            chunk_size,
        }
    }

    fn records_for(data: &[u8], layout: ChunkLayout, indices: &[u64]) -> Vec<RecordedChunk> {
        indices
            .iter()
            .map(|&index| {
                let range = layout.range(index).unwrap();
                let start = usize::try_from(range.offset).unwrap();
                let end = usize::try_from(range.offset + range.length).unwrap();
                RecordedChunk {
                    index,
                    hash: hash(&data[start..end]),
                }
            })
            .collect()
    }

    fn advertised_records(prepared: &PreparedReceiverV03) -> Vec<ChunkHashRecord> {
        prepared
            .chunk_hash_batches
            .iter()
            .flat_map(|batch| batch.records.iter().copied())
            .collect()
    }

    fn assert_encoded_batches_fit(prepared: &PreparedReceiverV03) {
        let count = prepared
            .chunk_hash_batches
            .iter()
            .map(|batch| {
                let encoded = encode_chunk_hashes(batch).unwrap();
                assert!(encoded.len() <= MAX_PAYLOAD_LENGTH);
                batch.records.len()
            })
            .sum::<usize>();

        assert_eq!(
            prepared.resume.chunk_record_count,
            u64::try_from(count).unwrap()
        );
    }

    async fn read_inventory(stream: &mut DuplexStream, batch_count: usize) -> (Frame, Vec<Frame>) {
        let resume = read_frame_for_version(stream, WFP_VERSION_V03)
            .await
            .unwrap();
        let mut batches = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            batches.push(
                read_frame_for_version(stream, WFP_VERSION_V03)
                    .await
                    .unwrap(),
            );
        }
        (resume, batches)
    }

    fn accept_frame() -> Frame {
        Frame::new_for_version(WFP_VERSION_V03, MessageType::Accept, Vec::new()).unwrap()
    }

    async fn prepared_from_state(partial: &Path, state: ChunkState) -> PreparedReceiverV03 {
        let file = prepare_v03_partial_file(partial, state.layout().file_size())
            .await
            .unwrap();
        let chunk_record_count = u64::try_from(state.recorded_chunks().len()).unwrap();
        let chunk_hash_batches = chunk_hash_batches(&state);

        PreparedReceiverV03 {
            partial_path: partial.to_path_buf(),
            file,
            receiver: ChunkReceiverV03::new(state),
            resume: ResumeRequestV03 { chunk_record_count },
            chunk_hash_batches,
        }
    }

    #[tokio::test]
    async fn prepares_a_fresh_offer_with_an_empty_inventory() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.bin.part");

        let prepared = prepare_receiver_v03(&partial, &offer(12, 4)).await.unwrap();

        assert_eq!(prepared.file.metadata().await.unwrap().len(), 12);
        assert_eq!(prepared.resume.chunk_record_count, 0);
        assert!(prepared.chunk_hash_batches.is_empty());
        assert!(prepared.receiver.chunk_state.recorded_chunks().is_empty());
    }

    #[tokio::test]
    async fn advertises_the_revalidated_sparse_inventory_without_rewriting_it() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.bin.part");
        let bytes = b"abcdefghijklmnopqrstuvwxyz0123456789ABCD";
        let layout = ChunkLayout::new(40, 4).unwrap();
        let state = ChunkState::new(layout, records_for(bytes, layout, &[0, 3, 9])).unwrap();
        fs::write(&partial, bytes).await.unwrap();
        write_chunk_state(&partial, &state).await.unwrap();
        let snapshot_path = chunk_state_path(&partial);
        let snapshot_before = fs::read(&snapshot_path).await.unwrap();

        let prepared = prepare_receiver_v03(&partial, &offer(40, 4)).await.unwrap();

        let expected: Vec<ChunkHashRecord> = state
            .recorded_chunks()
            .iter()
            .map(|record| ChunkHashRecord {
                chunk_index: record.index,
                hash: record.hash,
            })
            .collect();
        assert_eq!(prepared.resume.chunk_record_count, 3);
        assert_eq!(advertised_records(&prepared), expected);
        assert_eq!(prepared.receiver.chunk_state, state);
        assert_encoded_batches_fit(&prepared);
        assert_eq!(fs::read(snapshot_path).await.unwrap(), snapshot_before);
    }

    #[tokio::test]
    async fn revalidates_before_extending_a_short_partial() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.bin.part");
        let layout = ChunkLayout::new(8, 4).unwrap();
        let state = ChunkState::new(
            layout,
            vec![RecordedChunk {
                index: 1,
                hash: hash(&[0; 4]),
            }],
        )
        .unwrap();
        fs::write(&partial, b"abcd").await.unwrap();
        write_chunk_state(&partial, &state).await.unwrap();

        let prepared = prepare_receiver_v03(&partial, &offer(8, 4)).await.unwrap();

        assert_eq!(prepared.file.metadata().await.unwrap().len(), 8);
        assert_eq!(prepared.resume.chunk_record_count, 0);
        assert!(prepared.chunk_hash_batches.is_empty());
        assert!(prepared.receiver.chunk_state.recorded_chunks().is_empty());
    }

    #[tokio::test]
    async fn retains_revalidated_chunks_before_truncating_a_long_partial() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.bin.part");
        let layout = ChunkLayout::new(8, 4).unwrap();
        let state = ChunkState::new(
            layout,
            vec![RecordedChunk {
                index: 0,
                hash: hash(b"abcd"),
            }],
        )
        .unwrap();
        fs::write(&partial, b"abcdefghsurplus").await.unwrap();
        write_chunk_state(&partial, &state).await.unwrap();

        let prepared = prepare_receiver_v03(&partial, &offer(8, 4)).await.unwrap();

        assert_eq!(prepared.file.metadata().await.unwrap().len(), 8);
        assert_eq!(prepared.resume.chunk_record_count, 1);
        assert_eq!(advertised_records(&prepared)[0].chunk_index, 0);
    }

    #[tokio::test]
    async fn omits_mismatched_or_corrupt_snapshots() {
        let temp = tempdir().unwrap();

        for (name, state) in [
            (
                "different-size",
                ChunkState::new(
                    ChunkLayout::new(16, 4).unwrap(),
                    vec![RecordedChunk {
                        index: 0,
                        hash: hash(b"abcd"),
                    }],
                )
                .unwrap(),
            ),
            (
                "different-chunks",
                ChunkState::new(
                    ChunkLayout::new(12, 3).unwrap(),
                    vec![RecordedChunk {
                        index: 0,
                        hash: hash(b"abc"),
                    }],
                )
                .unwrap(),
            ),
        ] {
            let partial = temp.path().join(format!("{name}.part"));
            fs::write(&partial, b"abcdefghijkl").await.unwrap();
            write_chunk_state(&partial, &state).await.unwrap();
            let prepared = prepare_receiver_v03(&partial, &offer(12, 4)).await.unwrap();
            assert_eq!(prepared.resume.chunk_record_count, 0);
            assert!(prepared.chunk_hash_batches.is_empty());
        }

        let partial = temp.path().join("corrupt.part");
        fs::write(&partial, b"abcdefghijkl").await.unwrap();
        fs::write(chunk_state_path(&partial), b"not JSON")
            .await
            .unwrap();
        let prepared = prepare_receiver_v03(&partial, &offer(12, 4)).await.unwrap();
        assert_eq!(prepared.resume.chunk_record_count, 0);
        assert!(prepared.chunk_hash_batches.is_empty());
    }

    #[tokio::test]
    async fn omits_a_stale_recorded_hash() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.bin.part");
        let layout = ChunkLayout::new(4, 4).unwrap();
        let state = ChunkState::new(
            layout,
            vec![RecordedChunk {
                index: 0,
                hash: hash(b"wxyz"),
            }],
        )
        .unwrap();
        fs::write(&partial, b"abcd").await.unwrap();
        write_chunk_state(&partial, &state).await.unwrap();

        let prepared = prepare_receiver_v03(&partial, &offer(4, 4)).await.unwrap();

        assert_eq!(prepared.resume.chunk_record_count, 0);
        assert!(prepared.chunk_hash_batches.is_empty());
    }

    #[tokio::test]
    async fn prepares_a_zero_length_offer_without_chunk_hash_batches() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("empty.part");

        let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();

        assert_eq!(prepared.file.metadata().await.unwrap().len(), 0);
        assert_eq!(prepared.resume.chunk_record_count, 0);
        assert!(prepared.chunk_hash_batches.is_empty());
    }

    #[test]
    fn batches_at_the_codec_payload_boundary() {
        let maximum = maximum_chunk_hash_records_per_batch();
        let layout = ChunkLayout::new(u64::try_from(maximum + 1).unwrap(), 1).unwrap();
        let records: Vec<RecordedChunk> = (0..=maximum)
            .map(|index| RecordedChunk {
                index: u64::try_from(index).unwrap(),
                hash: ChunkHash::from_bytes([0xA5; 32]),
            })
            .collect();
        let one_batch =
            chunk_hash_batches(&ChunkState::new(layout, records[..maximum].to_vec()).unwrap());
        let two_batches = chunk_hash_batches(&ChunkState::new(layout, records).unwrap());

        assert_eq!(one_batch.len(), 1);
        assert_eq!(one_batch[0].records.len(), maximum);
        assert_eq!(two_batches.len(), 2);
        assert_eq!(two_batches[0].records.len(), maximum);
        assert_eq!(two_batches[1].records.len(), 1);
        assert_eq!(
            two_batches
                .iter()
                .map(|batch| batch.records.len())
                .sum::<usize>(),
            maximum + 1
        );
        for batch in one_batch.iter().chain(&two_batches) {
            assert!(encode_chunk_hashes(batch).unwrap().len() <= MAX_PAYLOAD_LENGTH);
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

    #[tokio::test]
    async fn transfer_accepts_complete_only_for_zero_length_and_all_reuse() {
        let temp = tempdir().unwrap();
        let sender_hash = hash(b"");
        let partial = temp.path().join("empty.part");
        let accepted = accepted_from_state(&partial, state(0, 4, Vec::new())).await;
        let bytes = encoded_frames(&[
            complete_frame(sender_hash),
            transfer_frame(MessageType::Accept, Vec::new()),
        ]);
        let first_frame_length = encode_frame(&complete_frame(sender_hash)).unwrap().len();
        let mut stream = Cursor::new(bytes);

        let complete = accepted.receive_transfer(&mut stream).await.unwrap();

        assert_eq!(complete.sender_file_hash, sender_hash);
        assert_eq!(complete.partial_path, partial);
        assert_eq!(complete.file.metadata().await.unwrap().len(), 0);
        assert!(complete.receiver.chunk_state.recorded_chunks().is_empty());
        assert_eq!(
            stream.position(),
            u64::try_from(first_frame_length).unwrap()
        );
        assert_eq!(ACTIVE_WFP_VERSION, WFP_VERSION_V02);

        let data = b"abcdefghijkl";
        let layout = ChunkLayout::new(12, 4).unwrap();
        let partial = temp.path().join("all-reuse.part");
        fs::write(&partial, data).await.unwrap();
        let reused = ChunkState::new(layout, records_for(data, layout, &[0, 1, 2])).unwrap();
        let accepted = accepted_from_state(&partial, reused.clone()).await;
        let sender_hash = hash(data);
        let mut stream = Cursor::new(encoded_frames(&[complete_frame(sender_hash)]));

        let complete = accepted.receive_transfer(&mut stream).await.unwrap();

        assert_eq!(complete.sender_file_hash, sender_hash);
        assert_eq!(complete.receiver.chunk_state, reused);
        assert_eq!(fs::read(partial).await.unwrap(), data);
    }

    #[tokio::test]
    async fn transfer_receives_one_chunk_and_persists_it_before_complete() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("one.part");
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let frames = [
            chunk_start_frame(0, b"abcd"),
            data_frame(0, b"abcd"),
            complete_frame(hash(b"abcd")),
        ];
        let mut stream = Cursor::new(encoded_frames(&frames));

        let complete = accepted.receive_transfer(&mut stream).await.unwrap();

        assert_eq!(fs::read(&partial).await.unwrap(), b"abcd");
        assert_eq!(complete.receiver.chunk_state.hash(0), Some(hash(b"abcd")));
        assert_eq!(
            read_chunk_state(&partial).await.unwrap(),
            complete.receiver.chunk_state
        );
        assert!(complete.receiver.active.is_none());
        assert!(complete.receiver.pending_verified.is_none());
    }

    #[tokio::test]
    async fn transfer_combines_reused_split_transmitted_and_final_short_chunks() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("mixed.part");
        let expected = b"abcdefghijklmn";
        let layout = ChunkLayout::new(14, 4).unwrap();
        fs::write(&partial, b"abcd????ijkl??").await.unwrap();
        let initial = ChunkState::new(layout, records_for(expected, layout, &[0, 2])).unwrap();
        let accepted = accepted_from_state(&partial, initial).await;
        let frames = [
            chunk_start_frame(1, b"efgh"),
            data_frame(4, b"ef"),
            data_frame(6, b"gh"),
            chunk_start_frame(3, b"mn"),
            data_frame(12, b"mn"),
            complete_frame(hash(expected)),
        ];
        let mut stream = Cursor::new(encoded_frames(&frames));

        let complete = accepted.receive_transfer(&mut stream).await.unwrap();

        assert_eq!(fs::read(&partial).await.unwrap(), expected);
        assert_eq!(
            complete
                .receiver
                .chunk_state
                .recorded_chunks()
                .iter()
                .map(|chunk| chunk.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            read_chunk_state(&partial).await.unwrap(),
            complete.receiver.chunk_state
        );
    }

    #[tokio::test]
    async fn transfer_preserves_receiver_errors_for_invalid_chunk_and_data_sequences() {
        let temp = tempdir().unwrap();

        let cases = [
            (
                "data-before-start",
                state(4, 4, Vec::new()),
                vec![data_frame(0, b"a")],
                ReceiverV03Error::DataWithoutActiveChunk,
            ),
            (
                "start-while-active",
                state(8, 4, Vec::new()),
                vec![chunk_start_frame(0, b"abcd"), chunk_start_frame(1, b"efgh")],
                ReceiverV03Error::ChunkAlreadyActive,
            ),
            (
                "wrong-offset",
                state(4, 4, Vec::new()),
                vec![chunk_start_frame(0, b"abcd"), data_frame(1, b"a")],
                ReceiverV03Error::UnexpectedDataOffset {
                    expected: 0,
                    actual: 1,
                },
            ),
            (
                "cross-boundary",
                state(8, 4, Vec::new()),
                vec![chunk_start_frame(0, b"abcd"), data_frame(0, b"abcde")],
                ReceiverV03Error::DataCrossesChunkBoundary {
                    end: 5,
                    chunk_end: 4,
                },
            ),
        ];

        for (name, chunk_state, frames, expected) in cases {
            let partial = temp.path().join(format!("{name}.part"));
            let accepted = accepted_from_state(&partial, chunk_state).await;
            let mut stream = Cursor::new(encoded_frames(&frames));
            let error = match accepted.receive_transfer(&mut stream).await {
                Ok(_) => panic!("{name} unexpectedly succeeded"),
                Err(error) => error,
            };

            assert!(
                matches!(&error, ReceiverV03TransferError::Receiver(actual) if mem::discriminant(actual) == mem::discriminant(&expected)),
                "unexpected error for {name}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn transfer_stops_on_hash_mismatch_without_persisting_or_reading_ahead() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("mismatch.part");
        let accepted = accepted_from_state(&partial, state(8, 4, Vec::new())).await;
        let first = chunk_start_frame(0, b"wxyz");
        let second = data_frame(0, b"abcd");
        let frames = [first.clone(), second.clone(), chunk_start_frame(1, b"efgh")];
        let consumed = encode_frame(&first).unwrap().len() + encode_frame(&second).unwrap().len();
        let mut stream = Cursor::new(encoded_frames(&frames));

        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Receiver(
                ReceiverV03Error::ChunkHashMismatch { index: 0 }
            ))
        ));
        assert_eq!(stream.position(), u64::try_from(consumed).unwrap());
        assert!(!chunk_state_path(&partial).exists());
    }

    #[tokio::test]
    async fn transfer_preserves_begin_chunk_range_redundancy_and_order_errors() {
        let temp = tempdir().unwrap();

        let partial = temp.path().join("out-of-range.part");
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let mut stream = Cursor::new(encoded_frames(&[chunk_start_frame(1, b"efgh")]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Receiver(
                ReceiverV03Error::ChunkIndexOutOfRange(1)
            ))
        ));

        let partial = temp.path().join("redundant.part");
        fs::write(&partial, b"abcd").await.unwrap();
        let verified = vec![RecordedChunk {
            index: 0,
            hash: hash(b"abcd"),
        }];
        let accepted = accepted_from_state(&partial, state(4, 4, verified)).await;
        let mut stream = Cursor::new(encoded_frames(&[chunk_start_frame(0, b"abcd")]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Receiver(
                ReceiverV03Error::AlreadyVerifiedChunk(0)
            ))
        ));

        let partial = temp.path().join("non-increasing.part");
        let accepted = accepted_from_state(&partial, state(8, 4, Vec::new())).await;
        let frames = [
            chunk_start_frame(1, b"efgh"),
            data_frame(4, b"efgh"),
            chunk_start_frame(0, b"abcd"),
        ];
        let mut stream = Cursor::new(encoded_frames(&frames));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Receiver(
                ReceiverV03Error::NonIncreasingChunkIndex {
                    previous: 1,
                    current: 0
                }
            ))
        ));
        assert_eq!(
            read_chunk_state(&partial).await.unwrap().hash(1),
            Some(hash(b"efgh"))
        );
    }

    #[tokio::test]
    async fn transfer_rejects_complete_at_unclean_or_incomplete_boundaries() {
        let temp = tempdir().unwrap();

        let partial = temp.path().join("active.part");
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let frames = [
            chunk_start_frame(0, b"abcd"),
            data_frame(0, b"ab"),
            complete_frame(hash(b"abcd")),
        ];
        let mut stream = Cursor::new(encoded_frames(&frames));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::CompleteWhileChunkActive { index: 0 })
        ));

        let partial = temp.path().join("pending.part");
        let mut file = prepare_v03_partial_file(&partial, 4).await.unwrap();
        let mut receiver = ChunkReceiverV03::new(state(4, 4, Vec::new()));
        receiver.begin_chunk(chunk_start(0, b"abcd")).unwrap();
        receiver
            .write_data(&mut file, &data(0, b"abcd"))
            .await
            .unwrap();
        let accepted = AcceptedReceiverV03 {
            partial_path: partial,
            file,
            receiver,
        };
        let mut stream = Cursor::new(encoded_frames(&[complete_frame(hash(b"abcd"))]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::CompleteWhileChunkPendingPersistence { index: 0 })
        ));

        let partial = temp.path().join("missing.part");
        fs::write(&partial, b"abcd????????").await.unwrap();
        let layout = ChunkLayout::new(12, 4).unwrap();
        let initial = ChunkState::new(layout, records_for(b"abcdefghijkl", layout, &[0])).unwrap();
        let accepted = accepted_from_state(&partial, initial).await;
        let frames = [
            chunk_start_frame(1, b"efgh"),
            data_frame(4, b"efgh"),
            complete_frame(hash(b"abcdefghijkl")),
        ];
        let mut stream = Cursor::new(encoded_frames(&frames));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::IncompleteVerifiedCoverage {
                expected: 3,
                actual: 2
            })
        ));
        assert_eq!(
            read_chunk_state(&partial)
                .await
                .unwrap()
                .recorded_chunks()
                .iter()
                .map(|chunk| chunk.index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[tokio::test]
    async fn transfer_stops_before_complete_when_chunk_persistence_fails() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("persistence-failure.part");
        fs::create_dir(chunk_state_path(&partial)).await.unwrap();
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let first = chunk_start_frame(0, b"abcd");
        let second = data_frame(0, b"abcd");
        let frames = [first.clone(), second.clone(), complete_frame(hash(b"abcd"))];
        let consumed = encode_frame(&first).unwrap().len() + encode_frame(&second).unwrap().len();
        let mut stream = Cursor::new(encoded_frames(&frames));

        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Receiver(
                ReceiverV03Error::ChunkState(ChunkStateError::Io(_))
            ))
        ));
        assert_eq!(stream.position(), u64::try_from(consumed).unwrap());
        assert!(chunk_state_path(&partial).is_dir());
    }

    #[tokio::test]
    async fn transfer_rejects_malformed_complete_and_transfer_payloads() {
        let temp = tempdir().unwrap();

        for length in [31, 33] {
            let partial = temp.path().join(format!("complete-{length}.part"));
            let accepted = accepted_from_state(&partial, state(0, 4, Vec::new())).await;
            let frame = transfer_frame(MessageType::Complete, vec![0; length]);
            let mut stream = Cursor::new(encoded_frames(&[frame]));
            assert!(matches!(
                accepted.receive_transfer(&mut stream).await,
                Err(ReceiverV03TransferError::InvalidCompletePayload(actual)) if actual == length
            ));
        }

        let partial = temp.path().join("chunk-start-codec.part");
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let frame = transfer_frame(MessageType::ChunkStart, vec![0; 39]);
        let mut stream = Cursor::new(encoded_frames(&[frame]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::ChunkStart(
                ChunkStartV03Error::InvalidPayloadLength(39)
            ))
        ));

        let partial = temp.path().join("data-codec.part");
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let frame = transfer_frame(MessageType::Data, vec![0; 7]);
        let mut stream = Cursor::new(encoded_frames(&[frame]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Data(
                DataV03Error::InvalidPayloadLength
            ))
        ));
    }

    #[tokio::test]
    async fn transfer_preserves_wrong_version_unexpected_message_and_eof_errors() {
        let temp = tempdir().unwrap();

        let partial = temp.path().join("wrong-version.part");
        let accepted = accepted_from_state(&partial, state(0, 4, Vec::new())).await;
        let frame =
            Frame::new_for_version(WFP_VERSION_V02, MessageType::Complete, vec![0; 32]).unwrap();
        let mut stream = Cursor::new(encoded_frames(&[frame]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Protocol(ProtocolIoError::Decode(
                DecodeError::UnsupportedVersion(WFP_VERSION_V02)
            )))
        ));

        let partial = temp.path().join("unexpected.part");
        let accepted = accepted_from_state(&partial, state(0, 4, Vec::new())).await;
        let frame = transfer_frame(MessageType::Accept, Vec::new());
        let mut stream = Cursor::new(encoded_frames(&[frame]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::UnexpectedMessageType(
                MessageType::Accept
            ))
        ));

        let partial = temp.path().join("empty-eof.part");
        let accepted = accepted_from_state(&partial, state(0, 4, Vec::new())).await;
        let mut stream = Cursor::new(Vec::new());
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Protocol(ProtocolIoError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
        ));

        let partial = temp.path().join("active-eof.part");
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let mut stream = Cursor::new(encoded_frames(&[
            chunk_start_frame(0, b"abcd"),
            data_frame(0, b"ab"),
        ]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Protocol(ProtocolIoError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
        ));

        let partial = temp.path().join("persisted-eof.part");
        let accepted = accepted_from_state(&partial, state(4, 4, Vec::new())).await;
        let mut stream = Cursor::new(encoded_frames(&[
            chunk_start_frame(0, b"abcd"),
            data_frame(0, b"abcd"),
        ]));
        assert!(matches!(
            accepted.receive_transfer(&mut stream).await,
            Err(ReceiverV03TransferError::Protocol(ProtocolIoError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
        ));
        assert_eq!(
            read_chunk_state(&partial).await.unwrap().hash(0),
            Some(hash(b"abcd"))
        );
    }

    #[tokio::test]
    async fn negotiates_an_empty_inventory_with_explicit_wfp_v03_frames() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("empty.part");
        let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();
        let (mut receiver_stream, mut peer_stream) = duplex(1024);

        let peer = async {
            let (resume, batches) = read_inventory(&mut peer_stream, 0).await;
            write_frame(&mut peer_stream, &accept_frame())
                .await
                .unwrap();
            (resume, batches)
        };
        let (accepted, (resume, batches)) =
            tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        let accepted = accepted.unwrap();

        assert_eq!(ACTIVE_WFP_VERSION, WFP_VERSION_V02);
        assert_eq!(resume.version, WFP_VERSION_V03);
        assert_eq!(resume.message_type, MessageType::Resume);
        assert_eq!(
            decode_resume_v03(&resume.payload)
                .unwrap()
                .chunk_record_count,
            0
        );
        assert!(batches.is_empty());
        assert_eq!(accepted.file.metadata().await.unwrap().len(), 0);
        assert!(accepted.receiver.chunk_state.recorded_chunks().is_empty());
        assert!(accepted.receiver.active.is_none());
        assert!(accepted.receiver.pending_verified.is_none());
    }

    #[tokio::test]
    async fn negotiates_a_sparse_prepared_inventory_without_changing_local_state() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("archive.part");
        let bytes = b"abcdefghijklmnopqrstuvwxyz0123456789ABCD";
        let layout = ChunkLayout::new(40, 4).unwrap();
        let state = ChunkState::new(layout, records_for(bytes, layout, &[0, 3, 9])).unwrap();
        fs::write(&partial, bytes).await.unwrap();
        write_chunk_state(&partial, &state).await.unwrap();
        let state_path = chunk_state_path(&partial);
        let partial_before = fs::read(&partial).await.unwrap();
        let snapshot_before = fs::read(&state_path).await.unwrap();
        let prepared = prepare_receiver_v03(&partial, &offer(40, 4)).await.unwrap();
        let expected_resume = prepared.resume;
        let expected_batches = prepared.chunk_hash_batches.clone();
        let expected_state = prepared.receiver.chunk_state.clone();
        let (mut receiver_stream, mut peer_stream) = duplex(4096);

        let peer = async {
            let (resume, batches) = read_inventory(&mut peer_stream, expected_batches.len()).await;
            write_frame(&mut peer_stream, &accept_frame())
                .await
                .unwrap();
            (resume, batches)
        };
        let (accepted, (resume, batches)) =
            tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        let accepted = accepted.unwrap();

        assert_eq!(resume.version, WFP_VERSION_V03);
        assert_eq!(resume.message_type, MessageType::Resume);
        assert_eq!(decode_resume_v03(&resume.payload).unwrap(), expected_resume);
        assert!(batches.iter().all(|frame| frame.version == WFP_VERSION_V03));
        assert!(
            batches
                .iter()
                .all(|frame| frame.message_type == MessageType::ChunkHashes)
        );
        assert_eq!(
            batches
                .iter()
                .map(|frame| decode_chunk_hashes(&frame.payload).unwrap())
                .collect::<Vec<_>>(),
            expected_batches
        );
        assert_eq!(accepted.receiver.chunk_state, expected_state);
        assert_eq!(fs::read(&partial).await.unwrap(), partial_before);
        assert_eq!(fs::read(&state_path).await.unwrap(), snapshot_before);
    }

    #[tokio::test]
    async fn negotiates_multiple_prepared_batches_in_order_before_accept() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("many.part");
        let maximum = maximum_chunk_hash_records_per_batch();
        let records = (0..=maximum)
            .map(|index| RecordedChunk {
                index: u64::try_from(index).unwrap(),
                hash: ChunkHash::from_bytes([u8::try_from(index % 256).unwrap(); 32]),
            })
            .collect();
        let state = ChunkState::new(
            ChunkLayout::new(u64::try_from(maximum + 1).unwrap(), 1).unwrap(),
            records,
        )
        .unwrap();
        let prepared = prepared_from_state(&partial, state).await;
        let expected_resume = prepared.resume;
        let expected_batches = prepared.chunk_hash_batches.clone();
        assert_eq!(expected_batches.len(), 2);
        let (mut receiver_stream, mut peer_stream) = duplex(2 * (MAX_PAYLOAD_LENGTH + 12));

        let peer = async {
            let (resume, batches) = read_inventory(&mut peer_stream, expected_batches.len()).await;
            write_frame(&mut peer_stream, &accept_frame())
                .await
                .unwrap();
            (resume, batches)
        };
        let (accepted, (resume, batches)) =
            tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        let accepted = accepted.unwrap();

        assert_eq!(decode_resume_v03(&resume.payload).unwrap(), expected_resume);
        let received_batches: Vec<_> = batches
            .iter()
            .map(|frame| decode_chunk_hashes(&frame.payload).unwrap())
            .collect();
        assert_eq!(received_batches, expected_batches);
        assert_eq!(
            received_batches
                .iter()
                .map(|batch| batch.records.len())
                .sum::<usize>(),
            usize::try_from(expected_resume.chunk_record_count).unwrap()
        );
        assert_eq!(
            accepted.file.metadata().await.unwrap().len(),
            expected_resume.chunk_record_count
        );
    }

    #[tokio::test]
    async fn rejects_nonempty_accept_and_unexpected_response_types() {
        let temp = tempdir().unwrap();

        let partial = temp.path().join("nonempty-accept.part");
        let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();
        let (mut receiver_stream, mut peer_stream) = duplex(1024);
        let peer = async {
            let _ = read_inventory(&mut peer_stream, 0).await;
            let response =
                Frame::new_for_version(WFP_VERSION_V03, MessageType::Accept, vec![0xA5]).unwrap();
            write_frame(&mut peer_stream, &response).await.unwrap();
        };
        let (result, ()) = tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        assert!(matches!(
            result,
            Err(ReceiverV03NegotiationError::InvalidAcceptPayload(1))
        ));

        for message_type in [
            MessageType::ChunkStart,
            MessageType::Data,
            MessageType::Complete,
        ] {
            let partial = temp.path().join(format!("{message_type:?}.part"));
            let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();
            let (mut receiver_stream, mut peer_stream) = duplex(1024);
            let peer = async {
                let _ = read_inventory(&mut peer_stream, 0).await;
                let response =
                    Frame::new_for_version(WFP_VERSION_V03, message_type, Vec::new()).unwrap();
                write_frame(&mut peer_stream, &response).await.unwrap();
            };
            let (result, ()) =
                tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
            assert!(matches!(
                result,
                Err(ReceiverV03NegotiationError::UnexpectedMessageType(actual)) if actual == message_type
            ));
        }
    }

    #[tokio::test]
    async fn preserves_version_decode_errors_and_eof_before_accept() {
        let temp = tempdir().unwrap();

        let partial = temp.path().join("wrong-version.part");
        let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();
        let (mut receiver_stream, mut peer_stream) = duplex(1024);
        let peer = async {
            let _ = read_inventory(&mut peer_stream, 0).await;
            let response =
                Frame::new_for_version(WFP_VERSION_V02, MessageType::Accept, Vec::new()).unwrap();
            write_frame(&mut peer_stream, &response).await.unwrap();
        };
        let (result, ()) = tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        assert!(matches!(
            result,
            Err(ReceiverV03NegotiationError::Protocol(
                ProtocolIoError::Decode(DecodeError::UnsupportedVersion(WFP_VERSION_V02))
            ))
        ));

        let partial = temp.path().join("malformed.part");
        let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();
        let (mut receiver_stream, mut peer_stream) = duplex(1024);
        let peer = async {
            let _ = read_inventory(&mut peer_stream, 0).await;
            peer_stream
                .write_all(&[b'W', b'F', b'P', 0, WFP_VERSION_V03, 0x7E, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        };
        let (result, ()) = tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        assert!(matches!(
            result,
            Err(ReceiverV03NegotiationError::Protocol(
                ProtocolIoError::Decode(DecodeError::UnknownMessageType(0x7E))
            ))
        ));

        let partial = temp.path().join("eof.part");
        let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();
        let (mut receiver_stream, mut peer_stream) = duplex(1024);
        let peer = async {
            let _ = read_inventory(&mut peer_stream, 0).await;
            drop(peer_stream);
        };
        let (result, ()) = tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        assert!(matches!(
            result,
            Err(ReceiverV03NegotiationError::Protocol(ProtocolIoError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
        ));
    }

    #[tokio::test]
    async fn leaves_the_frame_after_accept_for_the_next_phase() {
        let temp = tempdir().unwrap();
        let partial = temp.path().join("next-frame.part");
        let prepared = prepare_receiver_v03(&partial, &offer(0, 4)).await.unwrap();
        let (mut receiver_stream, mut peer_stream) = duplex(1024);
        let next =
            Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkStart, Vec::new()).unwrap();

        let peer = async {
            let _ = read_inventory(&mut peer_stream, 0).await;
            write_frame(&mut peer_stream, &accept_frame())
                .await
                .unwrap();
            write_frame(&mut peer_stream, &next).await.unwrap();
        };
        let (accepted, ()) = tokio::join!(prepared.negotiate_inventory(&mut receiver_stream), peer);
        let accepted = accepted.unwrap();
        assert!(accepted.receiver.active.is_none());
        assert_eq!(
            read_frame_for_version(&mut receiver_stream, WFP_VERSION_V03)
                .await
                .unwrap(),
            next
        );
    }
}
