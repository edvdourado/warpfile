use std::error::Error;
use std::fmt;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use crate::chunk::ChunkLayout;
use crate::chunk_manifest::ChunkHash;
use crate::protocol::frame::{FrameError, WFP_VERSION_V03};
use crate::protocol::{
    ChunkHashRecord, ChunkHashesError, ChunkStartV03, DataV03, DataV03Error, Frame, MessageType,
    ProtocolIoError, ResumeV03Error, V03_MAX_DATA_BYTES, decode_chunk_hashes, decode_resume_v03,
    encode_chunk_start_v03, encode_data_v03, read_frame_for_version, write_frame,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverInventoryV03 {
    layout: ChunkLayout,
    records: Vec<ChunkHashRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceChunkDispositionV03 {
    Reuse,
    Transmit,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SourceChunkV03 {
    index: u64,
    offset: u64,
    hash: ChunkHash,
    disposition: SourceChunkDispositionV03,
    data: Vec<u8>,
}

impl SourceChunkV03 {
    pub const fn index(&self) -> u64 {
        self.index
    }

    pub const fn offset(&self) -> u64 {
        self.offset
    }

    pub const fn hash(&self) -> ChunkHash {
        self.hash
    }

    pub const fn disposition(&self) -> SourceChunkDispositionV03 {
        self.disposition
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceScanSummaryV03 {
    file_hash: ChunkHash,
}

impl SourceScanSummaryV03 {
    pub const fn file_hash(&self) -> ChunkHash {
        self.file_hash
    }
}

#[derive(Debug)]
pub enum SourceScanV03Error {
    Io(std::io::Error),
    ChunkLengthTooLarge { length: u64 },
    ChunkAllocationFailed { length: u64 },
    SourceTooShort { expected: u64, actual: u64 },
    SourceTooLong { expected: u64 },
    IncompleteScan,
    UnreconciledInventory { consumed: usize, total: usize },
    InvalidChunkGeometry { index: u64 },
    ChunkIndexOverflow,
}

impl fmt::Display for SourceScanV03Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "WFP/0.3 source I/O error: {error}"),
            Self::ChunkLengthTooLarge { length } => {
                write!(
                    formatter,
                    "source chunk length cannot fit in memory: {length}"
                )
            }
            Self::ChunkAllocationFailed { length } => {
                write!(
                    formatter,
                    "could not allocate source chunk buffer of {length} bytes"
                )
            }
            Self::SourceTooShort { expected, actual } => write!(
                formatter,
                "source is shorter than its offered layout: expected {expected} bytes, read {actual}"
            ),
            Self::SourceTooLong { expected } => write!(
                formatter,
                "source is longer than its offered layout of {expected} bytes"
            ),
            Self::IncompleteScan => formatter.write_str("source scan is not complete"),
            Self::UnreconciledInventory { consumed, total } => write!(
                formatter,
                "source scan completed with {consumed} of {total} receiver inventory records reconciled"
            ),
            Self::InvalidChunkGeometry { index } => {
                write!(formatter, "missing source chunk geometry for index {index}")
            }
            Self::ChunkIndexOverflow => formatter.write_str("source chunk index overflow"),
        }
    }
}

impl Error for SourceScanV03Error {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::ChunkLengthTooLarge { .. }
            | Self::ChunkAllocationFailed { .. }
            | Self::SourceTooShort { .. }
            | Self::SourceTooLong { .. }
            | Self::IncompleteScan
            | Self::UnreconciledInventory { .. }
            | Self::InvalidChunkGeometry { .. }
            | Self::ChunkIndexOverflow => None,
        }
    }
}

pub struct SourceScannerV03<R> {
    source: R,
    layout: ChunkLayout,
    inventory: ReceiverInventoryV03,
    inventory_cursor: usize,
    next_chunk_index: u64,
    full_file_hasher: blake3::Hasher,
    source_length_confirmed: bool,
}

impl<R> SourceScannerV03<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(source: R, inventory: ReceiverInventoryV03) -> Self {
        Self {
            source,
            layout: inventory.layout,
            inventory,
            inventory_cursor: 0,
            next_chunk_index: 0,
            full_file_hasher: blake3::Hasher::new(),
            source_length_confirmed: false,
        }
    }

    pub async fn next_chunk(&mut self) -> Result<Option<SourceChunkV03>, SourceScanV03Error> {
        if self.source_length_confirmed {
            return Ok(None);
        }

        if self.next_chunk_index == self.layout.chunk_count() {
            self.confirm_source_end().await?;
            self.source_length_confirmed = true;
            return Ok(None);
        }

        let range = self.layout.range(self.next_chunk_index).ok_or(
            SourceScanV03Error::InvalidChunkGeometry {
                index: self.next_chunk_index,
            },
        )?;
        let length =
            usize::try_from(range.length).map_err(|_| SourceScanV03Error::ChunkLengthTooLarge {
                length: range.length,
            })?;
        let mut data = Vec::new();
        data.try_reserve_exact(length)
            .map_err(|_| SourceScanV03Error::ChunkAllocationFailed {
                length: range.length,
            })?;
        data.resize(length, 0);

        let mut bytes_read = 0usize;
        while bytes_read < data.len() {
            let read = self
                .source
                .read(&mut data[bytes_read..])
                .await
                .map_err(SourceScanV03Error::Io)?;
            if read == 0 {
                let actual = range
                    .offset
                    .checked_add(u64::try_from(bytes_read).expect("usize fits in u64"))
                    .ok_or(SourceScanV03Error::ChunkIndexOverflow)?;
                return Err(SourceScanV03Error::SourceTooShort {
                    expected: self.layout.file_size(),
                    actual,
                });
            }
            bytes_read += read;
        }

        self.full_file_hasher.update(&data);
        let hash = ChunkHash::from_bytes(*blake3::hash(&data).as_bytes());
        let disposition = match self.inventory.records.get(self.inventory_cursor) {
            Some(record) if record.chunk_index == range.index => {
                self.inventory_cursor += 1;
                if record.hash == hash {
                    SourceChunkDispositionV03::Reuse
                } else {
                    SourceChunkDispositionV03::Transmit
                }
            }
            Some(record) => {
                debug_assert!(record.chunk_index > range.index);
                SourceChunkDispositionV03::Transmit
            }
            None => SourceChunkDispositionV03::Transmit,
        };
        self.next_chunk_index = self
            .next_chunk_index
            .checked_add(1)
            .ok_or(SourceScanV03Error::ChunkIndexOverflow)?;

        Ok(Some(SourceChunkV03 {
            index: range.index,
            offset: range.offset,
            hash,
            disposition,
            data,
        }))
    }

    pub fn finish(&self) -> Result<SourceScanSummaryV03, SourceScanV03Error> {
        if !self.source_length_confirmed {
            return Err(SourceScanV03Error::IncompleteScan);
        }
        if self.inventory_cursor != self.inventory.records.len() {
            return Err(SourceScanV03Error::UnreconciledInventory {
                consumed: self.inventory_cursor,
                total: self.inventory.records.len(),
            });
        }

        Ok(SourceScanSummaryV03 {
            file_hash: ChunkHash::from_bytes(*self.full_file_hasher.finalize().as_bytes()),
        })
    }

    async fn confirm_source_end(&mut self) -> Result<(), SourceScanV03Error> {
        let mut byte = [0u8; 1];
        if self
            .source
            .read(&mut byte)
            .await
            .map_err(SourceScanV03Error::Io)?
            != 0
        {
            return Err(SourceScanV03Error::SourceTooLong {
                expected: self.layout.file_size(),
            });
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum SenderV03NegotiationError {
    Frame(FrameError),
    Protocol(ProtocolIoError),
    Resume(ResumeV03Error),
    ChunkHashes(ChunkHashesError),
    UnexpectedMessageType(MessageType),
    RecordCountExceedsLayout { declared: u64, chunk_count: u64 },
    EmptyChunkHashes,
    RecordCountOverflow,
    RecordCountOverrun { declared: u64, attempted: u64 },
    ChunkIndexOutOfRange(u64),
    NonIncreasingChunkIndex { previous: u64, current: u64 },
}

impl fmt::Display for SenderV03NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame(error) => write!(formatter, "WFP/0.3 inventory frame error: {error}"),
            Self::Protocol(error) => write!(formatter, "WFP/0.3 inventory I/O error: {error}"),
            Self::Resume(error) => write!(formatter, "WFP/0.3 RESUME error: {error}"),
            Self::ChunkHashes(error) => write!(formatter, "WFP/0.3 CHUNK_HASHES error: {error}"),
            Self::UnexpectedMessageType(message_type) => write!(
                formatter,
                "expected WFP/0.3 inventory message, received message type 0x{:02X}",
                *message_type as u8
            ),
            Self::RecordCountExceedsLayout {
                declared,
                chunk_count,
            } => write!(
                formatter,
                "receiver declared {declared} chunk records for a layout with {chunk_count} chunks"
            ),
            Self::EmptyChunkHashes => {
                formatter.write_str("empty CHUNK_HASHES batch while records remain")
            }
            Self::RecordCountOverflow => formatter.write_str("chunk record count overflow"),
            Self::RecordCountOverrun {
                declared,
                attempted,
            } => write!(
                formatter,
                "receiver advertised {attempted} chunk records but declared {declared}"
            ),
            Self::ChunkIndexOutOfRange(index) => {
                write!(formatter, "chunk index is out of range: {index}")
            }
            Self::NonIncreasingChunkIndex { previous, current } => {
                write!(
                    formatter,
                    "chunk index {current} does not follow {previous}"
                )
            }
        }
    }
}

impl Error for SenderV03NegotiationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Frame(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Resume(error) => Some(error),
            Self::ChunkHashes(error) => Some(error),
            Self::UnexpectedMessageType(_)
            | Self::RecordCountExceedsLayout { .. }
            | Self::EmptyChunkHashes
            | Self::RecordCountOverflow
            | Self::RecordCountOverrun { .. }
            | Self::ChunkIndexOutOfRange(_)
            | Self::NonIncreasingChunkIndex { .. } => None,
        }
    }
}

impl From<FrameError> for SenderV03NegotiationError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

impl From<ProtocolIoError> for SenderV03NegotiationError {
    fn from(error: ProtocolIoError) -> Self {
        Self::Protocol(error)
    }
}

impl From<ResumeV03Error> for SenderV03NegotiationError {
    fn from(error: ResumeV03Error) -> Self {
        Self::Resume(error)
    }
}

impl From<ChunkHashesError> for SenderV03NegotiationError {
    fn from(error: ChunkHashesError) -> Self {
        Self::ChunkHashes(error)
    }
}

pub async fn receive_inventory_and_accept<S>(
    stream: &mut S,
    layout: ChunkLayout,
) -> Result<ReceiverInventoryV03, SenderV03NegotiationError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let resume_frame = read_frame_for_version(stream, WFP_VERSION_V03).await?;
    if resume_frame.message_type != MessageType::Resume {
        return Err(SenderV03NegotiationError::UnexpectedMessageType(
            resume_frame.message_type,
        ));
    }
    let resume = decode_resume_v03(&resume_frame.payload)?;
    let chunk_count = layout.chunk_count();
    if resume.chunk_record_count > chunk_count {
        return Err(SenderV03NegotiationError::RecordCountExceedsLayout {
            declared: resume.chunk_record_count,
            chunk_count,
        });
    }

    let mut records = Vec::new();
    let mut received = 0u64;
    let mut previous_index = None;

    while received < resume.chunk_record_count {
        let frame = read_frame_for_version(stream, WFP_VERSION_V03).await?;
        if frame.message_type != MessageType::ChunkHashes {
            return Err(SenderV03NegotiationError::UnexpectedMessageType(
                frame.message_type,
            ));
        }

        let batch = decode_chunk_hashes(&frame.payload)?;
        if batch.records.is_empty() {
            return Err(SenderV03NegotiationError::EmptyChunkHashes);
        }
        let batch_count = u64::try_from(batch.records.len())
            .map_err(|_| SenderV03NegotiationError::RecordCountOverflow)?;
        let total = received
            .checked_add(batch_count)
            .ok_or(SenderV03NegotiationError::RecordCountOverflow)?;
        if total > resume.chunk_record_count {
            return Err(SenderV03NegotiationError::RecordCountOverrun {
                declared: resume.chunk_record_count,
                attempted: total,
            });
        }

        for record in batch.records {
            if layout.range(record.chunk_index).is_none() {
                return Err(SenderV03NegotiationError::ChunkIndexOutOfRange(
                    record.chunk_index,
                ));
            }
            if let Some(previous) = previous_index
                && record.chunk_index <= previous
            {
                return Err(SenderV03NegotiationError::NonIncreasingChunkIndex {
                    previous,
                    current: record.chunk_index,
                });
            }
            previous_index = Some(record.chunk_index);
            records.push(record);
        }
        received = total;
    }

    let accept = Frame::new_for_version(WFP_VERSION_V03, MessageType::Accept, Vec::new())?;
    write_frame(stream, &accept).await?;

    Ok(ReceiverInventoryV03 { layout, records })
}

#[derive(Debug)]
pub enum SenderV03TransferError {
    SourceScan(SourceScanV03Error),
    Frame(FrameError),
    Protocol(ProtocolIoError),
    Data(DataV03Error),
    DataLengthTooLarge(usize),
    DataOffsetOverflow,
}

impl fmt::Display for SenderV03TransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceScan(error) => write!(formatter, "WFP/0.3 source scan error: {error}"),
            Self::Frame(error) => write!(formatter, "WFP/0.3 transfer frame error: {error}"),
            Self::Protocol(error) => write!(formatter, "WFP/0.3 transfer I/O error: {error}"),
            Self::Data(error) => write!(formatter, "WFP/0.3 DATA encoding error: {error}"),
            Self::DataLengthTooLarge(length) => {
                write!(
                    formatter,
                    "WFP/0.3 DATA length does not fit in u64: {length}"
                )
            }
            Self::DataOffsetOverflow => formatter.write_str("WFP/0.3 DATA offset overflow"),
        }
    }
}

impl Error for SenderV03TransferError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceScan(error) => Some(error),
            Self::Frame(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Data(error) => Some(error),
            Self::DataLengthTooLarge(_) | Self::DataOffsetOverflow => None,
        }
    }
}

impl From<SourceScanV03Error> for SenderV03TransferError {
    fn from(error: SourceScanV03Error) -> Self {
        Self::SourceScan(error)
    }
}

impl From<FrameError> for SenderV03TransferError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

impl From<ProtocolIoError> for SenderV03TransferError {
    fn from(error: ProtocolIoError) -> Self {
        Self::Protocol(error)
    }
}

impl From<DataV03Error> for SenderV03TransferError {
    fn from(error: DataV03Error) -> Self {
        Self::Data(error)
    }
}

pub async fn send_source_chunks_v03<S, R>(
    stream: &mut S,
    scanner: &mut SourceScannerV03<R>,
) -> Result<SourceScanSummaryV03, SenderV03TransferError>
where
    S: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    while let Some(chunk) = scanner.next_chunk().await? {
        if chunk.disposition() == SourceChunkDispositionV03::Transmit {
            send_transmit_chunk_v03(stream, &chunk).await?;
        }
    }

    Ok(scanner.finish()?)
}

async fn send_transmit_chunk_v03<S>(
    stream: &mut S,
    chunk: &SourceChunkV03,
) -> Result<(), SenderV03TransferError>
where
    S: AsyncWrite + Unpin,
{
    let chunk_start = Frame::new_for_version(
        WFP_VERSION_V03,
        MessageType::ChunkStart,
        encode_chunk_start_v03(&ChunkStartV03 {
            chunk_index: chunk.index(),
            expected_hash: chunk.hash(),
        }),
    )?;
    write_frame(stream, &chunk_start).await?;

    let mut absolute_offset = chunk.offset();
    for fragment in chunk.data().chunks(V03_MAX_DATA_BYTES) {
        let data_length = u64::try_from(fragment.len())
            .map_err(|_| SenderV03TransferError::DataLengthTooLarge(fragment.len()))?;
        let payload = encode_data_v03(&DataV03 {
            absolute_offset,
            data: fragment.to_vec(),
        })?;
        let frame = Frame::new_for_version(WFP_VERSION_V03, MessageType::Data, payload)?;
        write_frame(stream, &frame).await?;
        absolute_offset = absolute_offset
            .checked_add(data_length)
            .ok_or(SenderV03TransferError::DataOffsetOverflow)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{self, Cursor};

    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

    use crate::chunk_manifest::{ChunkHash, ChunkManifestBuilder};
    use crate::protocol::frame::{ACTIVE_WFP_VERSION, WFP_VERSION_V02};
    use crate::protocol::{
        ChunkHashesBatch, DecodeError, ResumeRequestV03, V03_MAX_DATA_BYTES,
        decode_chunk_start_v03, decode_data_v03, encode_chunk_hashes, encode_resume_v03,
    };

    fn layout() -> ChunkLayout {
        ChunkLayout::new(40, 4).unwrap()
    }

    fn record(index: u64, byte: u8) -> ChunkHashRecord {
        ChunkHashRecord {
            chunk_index: index,
            hash: ChunkHash::from_bytes([byte; 32]),
        }
    }

    fn chunk_hash(data: &[u8]) -> ChunkHash {
        ChunkHash::from_bytes(*blake3::hash(data).as_bytes())
    }

    fn inventory(layout: ChunkLayout, records: Vec<ChunkHashRecord>) -> ReceiverInventoryV03 {
        ReceiverInventoryV03 { layout, records }
    }

    fn manifest_records(data: &[u8], layout: ChunkLayout, indices: &[u64]) -> Vec<ChunkHashRecord> {
        let mut builder = ChunkManifestBuilder::new(layout);
        builder.update(data).unwrap();
        let manifest = builder.finish().unwrap();

        indices
            .iter()
            .map(|&index| ChunkHashRecord {
                chunk_index: index,
                hash: manifest.hash(index).unwrap(),
            })
            .collect()
    }

    async fn scan_all(
        source: Vec<u8>,
        inventory: ReceiverInventoryV03,
    ) -> Result<(Vec<SourceChunkV03>, SourceScanSummaryV03), SourceScanV03Error> {
        let mut scanner = SourceScannerV03::new(Cursor::new(source), inventory);
        let mut chunks = Vec::new();
        while let Some(chunk) = scanner.next_chunk().await? {
            chunks.push(chunk);
        }
        assert_eq!(scanner.inventory_cursor, scanner.inventory.records.len());
        Ok((chunks, scanner.finish()?))
    }

    fn resume(count: u64) -> Frame {
        Frame::new_for_version(
            WFP_VERSION_V03,
            MessageType::Resume,
            encode_resume_v03(&ResumeRequestV03 {
                chunk_record_count: count,
            }),
        )
        .unwrap()
    }

    fn hashes(records: Vec<ChunkHashRecord>) -> Frame {
        Frame::new_for_version(
            WFP_VERSION_V03,
            MessageType::ChunkHashes,
            encode_chunk_hashes(&ChunkHashesBatch { records }).unwrap(),
        )
        .unwrap()
    }

    async fn send_frames(stream: &mut DuplexStream, frames: &[Frame]) {
        for frame in frames {
            write_frame(stream, frame).await.unwrap();
        }
    }

    async fn read_accept(stream: &mut DuplexStream) {
        let accept = read_frame_for_version(stream, WFP_VERSION_V03)
            .await
            .unwrap();
        assert_eq!(accept.version, WFP_VERSION_V03);
        assert_eq!(accept.message_type, MessageType::Accept);
        assert!(accept.payload.is_empty());
    }

    async fn assert_no_accept(sender: &mut DuplexStream, peer: &mut DuplexStream) {
        sender.shutdown().await.unwrap();
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
    }

    async fn read_frames(stream: &mut DuplexStream, count: usize) -> Vec<Frame> {
        let mut frames = Vec::with_capacity(count);
        for _ in 0..count {
            frames.push(
                read_frame_for_version(stream, WFP_VERSION_V03)
                    .await
                    .unwrap(),
            );
        }
        frames
    }

    #[tokio::test]
    async fn accepts_zero_record_inventory_without_reading_a_chunk_hashes_frame() {
        let (mut sender, mut peer) = duplex(1024);
        let next =
            Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkStart, Vec::new()).unwrap();
        send_frames(&mut peer, &[resume(0), next.clone()]).await;

        let inventory = receive_inventory_and_accept(&mut sender, layout())
            .await
            .unwrap();

        assert_eq!(inventory.layout, layout());
        assert!(inventory.records.is_empty());
        assert_eq!(
            read_frame_for_version(&mut sender, WFP_VERSION_V03)
                .await
                .unwrap(),
            next
        );
        read_accept(&mut peer).await;
    }

    #[tokio::test]
    async fn accepts_sparse_inventory_in_multiple_batches_without_read_ahead() {
        let (mut sender, mut peer) = duplex(2048);
        let records = vec![record(0, 0xA0), record(3, 0xA3), record(9, 0xA9)];
        let next =
            Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkStart, Vec::new()).unwrap();
        send_frames(
            &mut peer,
            &[
                resume(3),
                hashes(records[..2].to_vec()),
                hashes(records[2..].to_vec()),
                next.clone(),
            ],
        )
        .await;

        let inventory = receive_inventory_and_accept(&mut sender, layout())
            .await
            .unwrap();

        assert_eq!(inventory.layout, layout());
        assert_eq!(inventory.records, records);
        assert_eq!(
            read_frame_for_version(&mut sender, WFP_VERSION_V03)
                .await
                .unwrap(),
            next
        );
        read_accept(&mut peer).await;
    }

    #[tokio::test]
    async fn rejects_declared_count_above_layout_before_reading_inventory() {
        let (mut sender, mut peer) = duplex(1024);
        let next = hashes(vec![record(0, 0xA0)]);
        send_frames(&mut peer, &[resume(11), next.clone()]).await;

        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::RecordCountExceedsLayout {
                declared: 11,
                chunk_count: 10
            })
        ));
        assert_eq!(
            read_frame_for_version(&mut sender, WFP_VERSION_V03)
                .await
                .unwrap(),
            next
        );
        assert_no_accept(&mut sender, &mut peer).await;
    }

    #[tokio::test]
    async fn rejects_wrong_first_message_and_empty_batch_without_accepting() {
        let (mut sender, mut peer) = duplex(1024);
        send_frames(
            &mut peer,
            &[Frame::new_for_version(WFP_VERSION_V03, MessageType::Accept, Vec::new()).unwrap()],
        )
        .await;
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::UnexpectedMessageType(
                MessageType::Accept
            ))
        ));
        assert_no_accept(&mut sender, &mut peer).await;

        let (mut sender, mut peer) = duplex(1024);
        send_frames(&mut peer, &[resume(1), hashes(Vec::new())]).await;
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::EmptyChunkHashes)
        ));
        assert_no_accept(&mut sender, &mut peer).await;
    }

    #[tokio::test]
    async fn rejects_count_overrun_without_accepting() {
        let (mut sender, mut peer) = duplex(1024);
        send_frames(
            &mut peer,
            &[
                resume(2),
                hashes(vec![record(0, 0xA0), record(1, 0xA1), record(2, 0xA2)]),
            ],
        )
        .await;

        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::RecordCountOverrun {
                declared: 2,
                attempted: 3
            })
        ));
        assert_no_accept(&mut sender, &mut peer).await;
    }

    #[tokio::test]
    async fn rejects_duplicate_and_backwards_indices_without_accepting() {
        for frames in [
            vec![resume(2), hashes(vec![record(1, 0xA1), record(1, 0xB1)])],
            vec![resume(2), hashes(vec![record(3, 0xA3), record(1, 0xA1)])],
            vec![
                resume(2),
                hashes(vec![record(1, 0xA1)]),
                hashes(vec![record(1, 0xB1)]),
            ],
            vec![
                resume(2),
                hashes(vec![record(3, 0xA3)]),
                hashes(vec![record(2, 0xA2)]),
            ],
        ] {
            let (mut sender, mut peer) = duplex(1024);
            send_frames(&mut peer, &frames).await;
            assert!(matches!(
                receive_inventory_and_accept(&mut sender, layout()).await,
                Err(SenderV03NegotiationError::NonIncreasingChunkIndex { .. })
            ));
            assert_no_accept(&mut sender, &mut peer).await;
        }
    }

    #[tokio::test]
    async fn rejects_out_of_range_index_and_accepts_the_final_sparse_index() {
        let (mut sender, mut peer) = duplex(1024);
        send_frames(&mut peer, &[resume(1), hashes(vec![record(10, 0xAA)])]).await;
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::ChunkIndexOutOfRange(10))
        ));
        assert_no_accept(&mut sender, &mut peer).await;

        let (mut sender, mut peer) = duplex(1024);
        let final_record = record(9, 0xA9);
        send_frames(&mut peer, &[resume(1), hashes(vec![final_record])]).await;
        let inventory = receive_inventory_and_accept(&mut sender, layout())
            .await
            .unwrap();
        assert_eq!(inventory.records, vec![final_record]);
        read_accept(&mut peer).await;
    }

    #[tokio::test]
    async fn preserves_wrong_version_and_malformed_codec_errors() {
        let (mut sender, mut peer) = duplex(1024);
        let v02_resume =
            Frame::new_for_version(WFP_VERSION_V02, MessageType::Resume, Vec::new()).unwrap();
        send_frames(&mut peer, &[v02_resume]).await;
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::Protocol(
                ProtocolIoError::Decode(DecodeError::UnsupportedVersion(WFP_VERSION_V02))
            ))
        ));
        assert_no_accept(&mut sender, &mut peer).await;

        let (mut sender, mut peer) = duplex(1024);
        let malformed_resume =
            Frame::new_for_version(WFP_VERSION_V03, MessageType::Resume, vec![0; 7]).unwrap();
        send_frames(&mut peer, &[malformed_resume]).await;
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::Resume(_))
        ));
        assert_no_accept(&mut sender, &mut peer).await;

        let (mut sender, mut peer) = duplex(1024);
        let malformed_hashes =
            Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkHashes, vec![0, 0, 0, 1])
                .unwrap();
        send_frames(&mut peer, &[resume(1), malformed_hashes]).await;
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::ChunkHashes(_))
        ));
        assert_no_accept(&mut sender, &mut peer).await;
    }

    #[tokio::test]
    async fn preserves_eof_before_resume_and_before_the_declared_inventory_is_complete() {
        let (mut sender, peer) = duplex(1024);
        drop(peer);
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::Protocol(ProtocolIoError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
        ));

        let (mut sender, mut peer) = duplex(1024);
        send_frames(&mut peer, &[resume(2), hashes(vec![record(0, 0xA0)])]).await;
        drop(peer);
        assert!(matches!(
            receive_inventory_and_accept(&mut sender, layout()).await,
            Err(SenderV03NegotiationError::Protocol(ProtocolIoError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
        ));
    }

    #[tokio::test]
    async fn scanner_transmits_every_chunk_for_an_empty_inventory() {
        let data = b"abcdefghij".to_vec();
        let layout = ChunkLayout::new(10, 4).unwrap();
        let (chunks, _) = scan_all(data, inventory(layout, Vec::new())).await.unwrap();

        assert_eq!(chunks.len(), 3);
        assert!(
            chunks
                .iter()
                .all(|chunk| { chunk.disposition() == SourceChunkDispositionV03::Transmit })
        );
    }

    #[tokio::test]
    async fn scanner_derives_geometry_from_its_inventory() {
        let layout = ChunkLayout::new(6, 4).unwrap();
        let inventory = inventory(layout, Vec::new());
        assert_eq!(inventory.layout, layout);

        let mut scanner = SourceScannerV03::new(Cursor::new(b"abcdef".to_vec()), inventory);
        assert_eq!(scanner.layout, layout);
        let first = scanner.next_chunk().await.unwrap().unwrap();
        let second = scanner.next_chunk().await.unwrap().unwrap();

        assert_eq!(first.index(), 0);
        assert_eq!(first.offset(), 0);
        assert_eq!(first.data(), b"abcd");
        assert_eq!(second.index(), 1);
        assert_eq!(second.offset(), 4);
        assert_eq!(second.data(), b"ef");
        assert!(scanner.next_chunk().await.unwrap().is_none());
        assert_eq!(scanner.inventory_cursor, scanner.inventory.records.len());
    }

    #[tokio::test]
    async fn scanner_reuses_a_fully_matching_inventory_and_matches_the_manifest() {
        let data = b"abcdefghij".to_vec();
        let layout = ChunkLayout::new(10, 4).unwrap();
        let records = manifest_records(&data, layout, &[0, 1, 2]);
        let (chunks, _) = scan_all(data.clone(), inventory(layout, records))
            .await
            .unwrap();

        let mut builder = ChunkManifestBuilder::new(layout);
        builder.update(&data).unwrap();
        let manifest = builder.finish().unwrap();
        assert!(
            chunks
                .iter()
                .all(|chunk| { chunk.disposition() == SourceChunkDispositionV03::Reuse })
        );
        for chunk in chunks {
            assert_eq!(chunk.hash(), manifest.hash(chunk.index()).unwrap());
        }
    }

    #[tokio::test]
    async fn scanner_uses_a_linear_cursor_for_sparse_matching_records() {
        let data = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_vec();
        let layout = ChunkLayout::new(40, 4).unwrap();
        let records = manifest_records(&data, layout, &[0, 3, 9]);
        let mut scanner = SourceScannerV03::new(Cursor::new(data), inventory(layout, records));
        let mut dispositions = Vec::new();
        while let Some(chunk) = scanner.next_chunk().await.unwrap() {
            dispositions.push(chunk.disposition());
        }

        assert_eq!(
            dispositions,
            vec![
                SourceChunkDispositionV03::Reuse,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Reuse,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Reuse,
            ]
        );
        assert_eq!(scanner.inventory_cursor, scanner.inventory.records.len());
        assert_eq!(
            scanner.finish().unwrap().file_hash(),
            chunk_hash(b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN")
        );
    }

    #[tokio::test]
    async fn scanner_transmits_divergent_hashes_and_preserves_mixed_decisions() {
        let data = b"abcdefghijkl".to_vec();
        let layout = ChunkLayout::new(12, 4).unwrap();
        let mut records = manifest_records(&data, layout, &[0, 1]);
        records[1].hash = chunk_hash(b"wrong");
        let (chunks, _) = scan_all(data, inventory(layout, records)).await.unwrap();

        assert_eq!(
            chunks
                .iter()
                .map(SourceChunkV03::disposition)
                .collect::<Vec<_>>(),
            vec![
                SourceChunkDispositionV03::Reuse,
                SourceChunkDispositionV03::Transmit,
                SourceChunkDispositionV03::Transmit,
            ]
        );
    }

    #[tokio::test]
    async fn scanner_preserves_final_short_chunk_metadata_and_bytes() {
        let data = b"abcdef".to_vec();
        let layout = ChunkLayout::new(6, 4).unwrap();
        let records = manifest_records(&data, layout, &[1]);
        let (chunks, _) = scan_all(data, inventory(layout, records)).await.unwrap();

        assert_eq!(chunks[1].index(), 1);
        assert_eq!(chunks[1].offset(), 4);
        assert_eq!(chunks[1].data(), b"ef");
        assert_eq!(chunks[1].hash(), chunk_hash(b"ef"));
        assert_eq!(chunks[1].disposition(), SourceChunkDispositionV03::Reuse);
    }

    #[tokio::test]
    async fn scanner_handles_an_empty_source_and_hashes_each_byte_once() {
        let layout = ChunkLayout::new(0, 4).unwrap();
        let (chunks, summary) = scan_all(Vec::new(), inventory(layout, Vec::new()))
            .await
            .unwrap();

        assert!(chunks.is_empty());
        assert_eq!(summary.file_hash(), chunk_hash(b""));
    }

    #[tokio::test]
    async fn scanner_full_file_hash_matches_the_exact_source() {
        let data = b"abcdefghij".to_vec();
        let (_, summary) = scan_all(
            data.clone(),
            inventory(ChunkLayout::new(10, 4).unwrap(), Vec::new()),
        )
        .await
        .unwrap();

        assert_eq!(summary.file_hash(), chunk_hash(&data));
    }

    #[tokio::test]
    async fn scanner_rejects_a_short_source_without_a_final_digest() {
        let mut scanner = SourceScannerV03::new(
            Cursor::new(b"abcdef".to_vec()),
            inventory(ChunkLayout::new(8, 4).unwrap(), Vec::new()),
        );
        assert!(scanner.next_chunk().await.unwrap().is_some());
        assert!(matches!(
            scanner.next_chunk().await,
            Err(SourceScanV03Error::SourceTooShort {
                expected: 8,
                actual: 6
            })
        ));
        assert!(matches!(
            scanner.finish(),
            Err(SourceScanV03Error::IncompleteScan)
        ));
    }

    #[tokio::test]
    async fn scanner_rejects_a_long_source_without_a_final_digest() {
        let mut scanner = SourceScannerV03::new(
            Cursor::new(b"abcdef".to_vec()),
            inventory(ChunkLayout::new(4, 4).unwrap(), Vec::new()),
        );
        assert!(scanner.next_chunk().await.unwrap().is_some());
        assert!(matches!(
            scanner.next_chunk().await,
            Err(SourceScanV03Error::SourceTooLong { expected: 4 })
        ));
        assert!(matches!(
            scanner.finish(),
            Err(SourceScanV03Error::IncompleteScan)
        ));
    }

    #[tokio::test]
    async fn scanner_rejects_early_finish() {
        let mut scanner = SourceScannerV03::new(
            Cursor::new(b"abcdefgh".to_vec()),
            inventory(ChunkLayout::new(8, 4).unwrap(), Vec::new()),
        );
        assert!(matches!(
            scanner.finish(),
            Err(SourceScanV03Error::IncompleteScan)
        ));
        assert!(scanner.next_chunk().await.unwrap().is_some());
        assert!(matches!(
            scanner.finish(),
            Err(SourceScanV03Error::IncompleteScan)
        ));
    }

    #[tokio::test]
    async fn scanner_rejects_a_summary_with_unreconciled_inventory() {
        let layout = ChunkLayout::new(4, 4).unwrap();
        let mut scanner = SourceScannerV03::new(
            Cursor::new(b"abcd".to_vec()),
            inventory(layout, vec![record(1, 0xA1)]),
        );

        assert!(scanner.next_chunk().await.unwrap().is_some());
        assert!(scanner.next_chunk().await.unwrap().is_none());
        assert!(matches!(
            scanner.finish(),
            Err(SourceScanV03Error::UnreconciledInventory {
                consumed: 0,
                total: 1
            })
        ));
    }

    #[tokio::test]
    async fn sends_no_frames_for_all_reuse_and_returns_the_full_file_hash() {
        let data = b"abcdefghijkl".to_vec();
        let layout = ChunkLayout::new(12, 4).unwrap();
        let records = manifest_records(&data, layout, &[0, 1, 2]);
        let mut scanner =
            SourceScannerV03::new(Cursor::new(data.clone()), inventory(layout, records));
        let (mut sender, mut peer) = duplex(1024);

        let summary = send_source_chunks_v03(&mut sender, &mut scanner)
            .await
            .unwrap();
        sender.shutdown().await.unwrap();

        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(summary.file_hash(), chunk_hash(&data));
    }

    #[tokio::test]
    async fn sends_only_transmit_chunks_with_local_hashes_in_order() {
        let data = b"abcdefghijklmnop".to_vec();
        let layout = ChunkLayout::new(16, 4).unwrap();
        let mut records = manifest_records(&data, layout, &[0, 1, 2]);
        records[1].hash = chunk_hash(b"wrong");
        let mut scanner =
            SourceScannerV03::new(Cursor::new(data.clone()), inventory(layout, records));
        let (mut sender, mut peer) = duplex(4096);

        let summary = send_source_chunks_v03(&mut sender, &mut scanner)
            .await
            .unwrap();
        sender.shutdown().await.unwrap();
        let frames = read_frames(&mut peer, 4).await;
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining).await.unwrap();

        assert!(remaining.is_empty());
        assert!(frames.iter().all(|frame| frame.version == WFP_VERSION_V03));
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.message_type)
                .collect::<Vec<_>>(),
            vec![
                MessageType::ChunkStart,
                MessageType::Data,
                MessageType::ChunkStart,
                MessageType::Data,
            ]
        );
        assert_eq!(
            decode_chunk_start_v03(&frames[0].payload)
                .unwrap()
                .chunk_index,
            1
        );
        assert_eq!(
            decode_chunk_start_v03(&frames[0].payload)
                .unwrap()
                .expected_hash,
            chunk_hash(b"efgh")
        );
        assert_eq!(decode_data_v03(&frames[1].payload).unwrap().data, b"efgh");
        assert_eq!(
            decode_chunk_start_v03(&frames[2].payload)
                .unwrap()
                .chunk_index,
            3
        );
        assert_eq!(
            decode_chunk_start_v03(&frames[2].payload)
                .unwrap()
                .expected_hash,
            chunk_hash(b"mnop")
        );
        assert_eq!(decode_data_v03(&frames[3].payload).unwrap().data, b"mnop");
        assert_eq!(summary.file_hash(), chunk_hash(&data));
    }

    #[tokio::test]
    async fn sends_a_final_short_chunk_at_its_absolute_offset() {
        let data = b"abcdef".to_vec();
        let layout = ChunkLayout::new(6, 4).unwrap();
        let records = manifest_records(&data, layout, &[0]);
        let mut scanner =
            SourceScannerV03::new(Cursor::new(data.clone()), inventory(layout, records));
        let (mut sender, mut peer) = duplex(1024);

        let summary = send_source_chunks_v03(&mut sender, &mut scanner)
            .await
            .unwrap();
        let frames = read_frames(&mut peer, 2).await;

        assert_eq!(
            decode_chunk_start_v03(&frames[0].payload)
                .unwrap()
                .chunk_index,
            1
        );
        assert_eq!(
            decode_data_v03(&frames[1].payload).unwrap(),
            DataV03 {
                absolute_offset: 4,
                data: b"ef".to_vec(),
            }
        );
        assert_eq!(summary.file_hash(), chunk_hash(&data));
    }

    #[tokio::test]
    async fn fragments_data_at_the_existing_wire_limit() {
        for (length, expected_data_frames) in [
            (V03_MAX_DATA_BYTES - 1, 1usize),
            (V03_MAX_DATA_BYTES, 1),
            (V03_MAX_DATA_BYTES + 1, 2),
        ] {
            let data = vec![0xA5; length];
            let layout = ChunkLayout::new(
                u64::try_from(length).unwrap(),
                u64::try_from(length).unwrap(),
            )
            .unwrap();
            let mut scanner =
                SourceScannerV03::new(Cursor::new(data.clone()), inventory(layout, Vec::new()));
            let (mut sender, mut peer) = duplex(256 * 1024);

            let summary = send_source_chunks_v03(&mut sender, &mut scanner)
                .await
                .unwrap();
            let frames = read_frames(&mut peer, expected_data_frames + 1).await;

            assert_eq!(frames[0].message_type, MessageType::ChunkStart);
            let data_frames = frames[1..]
                .iter()
                .map(|frame| decode_data_v03(&frame.payload).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(data_frames.len(), expected_data_frames);
            assert!(data_frames.iter().all(|frame| !frame.data.is_empty()));
            assert!(
                data_frames
                    .iter()
                    .all(|frame| frame.data.len() <= V03_MAX_DATA_BYTES)
            );
            assert_eq!(data_frames[0].absolute_offset, 0);
            for pair in data_frames.windows(2) {
                assert_eq!(
                    pair[1].absolute_offset,
                    pair[0]
                        .absolute_offset
                        .checked_add(u64::try_from(pair[0].data.len()).unwrap())
                        .unwrap()
                );
            }
            assert_eq!(
                data_frames
                    .iter()
                    .flat_map(|frame| frame.data.iter().copied())
                    .collect::<Vec<_>>(),
                data
            );
            assert_eq!(summary.file_hash(), chunk_hash(&data));
        }
    }

    #[tokio::test]
    async fn finishes_one_transmitted_chunk_before_starting_the_next() {
        let chunk_length = V03_MAX_DATA_BYTES + 1;
        let data = vec![0x5A; chunk_length * 2];
        let layout = ChunkLayout::new(
            u64::try_from(data.len()).unwrap(),
            u64::try_from(chunk_length).unwrap(),
        )
        .unwrap();
        let mut scanner = SourceScannerV03::new(Cursor::new(data), inventory(layout, Vec::new()));
        let (mut sender, mut peer) = duplex(512 * 1024);

        send_source_chunks_v03(&mut sender, &mut scanner)
            .await
            .unwrap();
        let frames = read_frames(&mut peer, 6).await;

        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.message_type)
                .collect::<Vec<_>>(),
            vec![
                MessageType::ChunkStart,
                MessageType::Data,
                MessageType::Data,
                MessageType::ChunkStart,
                MessageType::Data,
                MessageType::Data,
            ]
        );
        assert_eq!(
            decode_chunk_start_v03(&frames[0].payload)
                .unwrap()
                .chunk_index,
            0
        );
        assert_eq!(
            decode_chunk_start_v03(&frames[3].payload)
                .unwrap()
                .chunk_index,
            1
        );
    }

    #[tokio::test]
    async fn confirms_an_empty_source_without_emitting_a_transfer_frame() {
        let mut scanner = SourceScannerV03::new(
            Cursor::new(Vec::new()),
            inventory(ChunkLayout::new(0, 4).unwrap(), Vec::new()),
        );
        let (mut sender, mut peer) = duplex(1024);

        let summary = send_source_chunks_v03(&mut sender, &mut scanner)
            .await
            .unwrap();
        sender.shutdown().await.unwrap();

        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(summary.file_hash(), chunk_hash(b""));
    }

    #[tokio::test]
    async fn propagates_source_length_errors_without_a_complete_frame() {
        for (source, layout, is_short_source) in [
            (b"abcd".to_vec(), ChunkLayout::new(8, 4).unwrap(), true),
            (b"abcdef".to_vec(), ChunkLayout::new(4, 4).unwrap(), false),
        ] {
            let mut scanner =
                SourceScannerV03::new(Cursor::new(source), inventory(layout, Vec::new()));
            let (mut sender, mut peer) = duplex(1024);

            let result = send_source_chunks_v03(&mut sender, &mut scanner).await;
            sender.shutdown().await.unwrap();
            let frames = read_frames(&mut peer, 2).await;
            let mut remaining = Vec::new();
            peer.read_to_end(&mut remaining).await.unwrap();

            if is_short_source {
                assert!(matches!(
                    result,
                    Err(SenderV03TransferError::SourceScan(
                        SourceScanV03Error::SourceTooShort {
                            expected: 8,
                            actual: 4,
                        }
                    ))
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(SenderV03TransferError::SourceScan(
                        SourceScanV03Error::SourceTooLong { expected: 4 }
                    ))
                ));
            }
            assert!(remaining.is_empty());
            assert!(
                frames
                    .iter()
                    .all(|frame| frame.message_type != MessageType::Complete)
            );
        }
    }

    #[tokio::test]
    async fn stops_on_a_network_write_error() {
        let mut scanner = SourceScannerV03::new(
            Cursor::new(b"abcd".to_vec()),
            inventory(ChunkLayout::new(4, 4).unwrap(), Vec::new()),
        );
        let (mut sender, peer) = duplex(1024);
        drop(peer);

        assert!(matches!(
            send_source_chunks_v03(&mut sender, &mut scanner).await,
            Err(SenderV03TransferError::Protocol(ProtocolIoError::Io(_)))
        ));
    }

    #[test]
    fn active_protocol_version_remains_wfp_v02() {
        assert_eq!(ACTIVE_WFP_VERSION, WFP_VERSION_V02);
    }
}
