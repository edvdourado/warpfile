use std::error::Error;
use std::fmt;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::chunk::ChunkLayout;
use crate::protocol::frame::{FrameError, WFP_VERSION_V03};
use crate::protocol::{
    ChunkHashRecord, ChunkHashesError, Frame, MessageType, ProtocolIoError, ResumeV03Error,
    decode_chunk_hashes, decode_resume_v03, read_frame_for_version, write_frame,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverInventoryV03 {
    records: Vec<ChunkHashRecord>,
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

    Ok(ReceiverInventoryV03 { records })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io;

    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

    use crate::chunk_manifest::ChunkHash;
    use crate::protocol::frame::{ACTIVE_WFP_VERSION, WFP_VERSION_V02};
    use crate::protocol::{
        ChunkHashesBatch, DecodeError, ResumeRequestV03, encode_chunk_hashes, encode_resume_v03,
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

    #[tokio::test]
    async fn accepts_zero_record_inventory_without_reading_a_chunk_hashes_frame() {
        let (mut sender, mut peer) = duplex(1024);
        let next =
            Frame::new_for_version(WFP_VERSION_V03, MessageType::ChunkStart, Vec::new()).unwrap();
        send_frames(&mut peer, &[resume(0), next.clone()]).await;

        let inventory = receive_inventory_and_accept(&mut sender, layout())
            .await
            .unwrap();

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

    #[test]
    fn active_protocol_version_remains_wfp_v02() {
        assert_eq!(ACTIVE_WFP_VERSION, WFP_VERSION_V02);
    }
}
