// Run manually: cargo test --test performance_baseline -- --ignored --nocapture
mod common;

use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use common::proxy_v03_session;
use tempfile::tempdir;
use tokio::net::TcpListener;
use warpfile::chunk::ChunkLayout;
use warpfile::chunk_manifest::ChunkHash;
use warpfile::chunk_state::{ChunkState, RecordedChunk, encode_chunk_state};
use warpfile::protocol::frame::WFP_VERSION_V03;
use warpfile::protocol::{
    MessageType, TransferId, decode_chunk_start_v03, decode_data_v03, encode_frame,
};
use warpfile::receiver_v03::run_receiver_v03;
use warpfile::sender_v03::{V03_DEFAULT_CHUNK_SIZE, send_session_v03};

const FILE_SIZE_BYTES: usize = 32 * 1024 * 1024;
const MIB: f64 = 1024.0 * 1024.0;

#[tokio::test]
#[ignore = "manual local performance baseline"]
async fn fresh_transfer() {
    let temp = tempdir().unwrap();
    let source_path = temp.path().join("payload.bin");
    let dest_dir = temp.path().join("receiver");
    let contents: Vec<u8> = (0..FILE_SIZE_BYTES)
        .map(|index| (index % 251) as u8)
        .collect();
    std::fs::write(&source_path, &contents).unwrap();

    let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let receiver_address = receiver_listener.local_addr().unwrap().to_string();
    drop(receiver_listener);
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap().to_string();

    let receiver_dest = dest_dir.clone();
    let receiver_bind = receiver_address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_bind, &receiver_dest).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let proxy_receiver_address = receiver_address;
    let proxy_task =
        tokio::spawn(
            async move { proxy_v03_session(&proxy, &proxy_receiver_address, false).await },
        );
    let started = Instant::now();
    send_session_v03(
        &source_path,
        &proxy_address,
        TransferId::generate().unwrap(),
        NonZeroU64::new(V03_DEFAULT_CHUNK_SIZE).unwrap(),
    )
    .await
    .unwrap();
    let elapsed = started.elapsed();
    let frames = proxy_task.await.unwrap();
    receiver.abort();
    let _ = receiver.await;

    let useful_data_bytes: u64 = frames
        .iter()
        .filter(|frame| frame.message_type == warpfile::protocol::MessageType::Data)
        .map(|frame| decode_data_v03(&frame.payload).unwrap().data.len() as u64)
        .sum();
    let protocol_bytes_sender_to_receiver: u64 = frames
        .iter()
        .map(|frame| {
            let encoded_length = encode_frame(frame).unwrap().len();
            encoded_length as u64
        })
        .sum();
    let file_size_bytes = FILE_SIZE_BYTES as u64;
    let reused_bytes = file_size_bytes
        .checked_sub(useful_data_bytes)
        .expect("useful data bytes cannot exceed file size");
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let effective_throughput_mib_s = file_size_bytes as f64 / MIB / elapsed.as_secs_f64();

    let final_path = dest_dir.join("payload.bin");
    assert!(final_path.is_file(), "final destination file must exist");
    assert_eq!(std::fs::read(&final_path).unwrap(), contents);
    assert_eq!(useful_data_bytes, file_size_bytes);
    assert_eq!(reused_bytes, 0);
    assert!(!dest_dir.join("payload.bin.part").exists());
    assert!(!dest_dir.join("payload.bin.part.warpchunks").exists());

    println!(
        "{}",
        serde_json::json!({
            "wfp_version": format!("0.{}", WFP_VERSION_V03),
            "scenario": "fresh_transfer",
            "file_size_bytes": file_size_bytes,
            "chunk_size_bytes": V03_DEFAULT_CHUNK_SIZE,
            "reused_bytes": reused_bytes,
            "useful_data_bytes": useful_data_bytes,
            "protocol_bytes_sender_to_receiver": protocol_bytes_sender_to_receiver,
            "elapsed_ms": elapsed_ms,
            "effective_throughput_mib_s": effective_throughput_mib_s,
        })
    );
}

#[tokio::test]
#[ignore = "manual local performance baseline"]
async fn resume_50_percent() {
    let temp = tempdir().unwrap();
    let source_path = temp.path().join("payload.bin");
    let dest_dir = temp.path().join("receiver");
    std::fs::create_dir_all(&dest_dir).unwrap();
    let contents: Vec<u8> = (0..FILE_SIZE_BYTES)
        .map(|index| (index % 251) as u8)
        .collect();
    std::fs::write(&source_path, &contents).unwrap();

    let file_size_bytes = FILE_SIZE_BYTES as u64;
    let layout = ChunkLayout::new(file_size_bytes, V03_DEFAULT_CHUNK_SIZE).unwrap();
    assert_eq!(layout.chunk_count(), 32);
    let partial_path = dest_dir.join("payload.bin.part");
    let mut partial = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial_path)
        .unwrap();
    partial.set_len(file_size_bytes).unwrap();
    let mut records = Vec::new();
    for index in 0..16 {
        let range = layout.range(index).unwrap();
        let start = range.offset as usize;
        let end = (range.offset + range.length) as usize;
        partial.seek(SeekFrom::Start(range.offset)).unwrap();
        partial.write_all(&contents[start..end]).unwrap();
        records.push(RecordedChunk {
            index,
            hash: ChunkHash::from_bytes(*blake3::hash(&contents[start..end]).as_bytes()),
        });
    }
    drop(partial);
    let state = ChunkState::new(layout, records).unwrap();
    assert_eq!(
        state
            .recorded_chunks()
            .iter()
            .map(|chunk| chunk.index)
            .collect::<Vec<_>>(),
        (0..16).collect::<Vec<_>>()
    );
    std::fs::write(
        dest_dir.join("payload.bin.part.warpchunks"),
        encode_chunk_state(&state).unwrap(),
    )
    .unwrap();

    let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let receiver_address = receiver_listener.local_addr().unwrap().to_string();
    drop(receiver_listener);
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap().to_string();
    let receiver_dest = dest_dir.clone();
    let receiver_bind = receiver_address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_bind, &receiver_dest).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let proxy_task =
        tokio::spawn(async move { proxy_v03_session(&proxy, &receiver_address, false).await });

    let started = Instant::now();
    send_session_v03(
        &source_path,
        &proxy_address,
        TransferId::generate().unwrap(),
        NonZeroU64::new(V03_DEFAULT_CHUNK_SIZE).unwrap(),
    )
    .await
    .unwrap();
    let elapsed = started.elapsed();
    let frames = proxy_task.await.unwrap();
    receiver.abort();
    let _ = receiver.await;

    let chunk_starts: Vec<u64> = frames
        .iter()
        .filter(|frame| frame.message_type == MessageType::ChunkStart)
        .map(|frame| decode_chunk_start_v03(&frame.payload).unwrap().chunk_index)
        .collect();
    assert_eq!(chunk_starts, (16..32).collect::<Vec<_>>());
    let useful_data_bytes: u64 = frames
        .iter()
        .filter(|frame| frame.message_type == MessageType::Data)
        .map(|frame| decode_data_v03(&frame.payload).unwrap().data.len() as u64)
        .sum();
    let protocol_bytes_sender_to_receiver: u64 = frames
        .iter()
        .map(|frame| encode_frame(frame).unwrap().len() as u64)
        .sum();
    let reused_bytes = file_size_bytes
        .checked_sub(useful_data_bytes)
        .expect("useful data bytes cannot exceed file size");
    assert_eq!(useful_data_bytes, 16 * 1024 * 1024);
    assert_eq!(reused_bytes, 16 * 1024 * 1024);
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let effective_throughput_mib_s = file_size_bytes as f64 / MIB / elapsed.as_secs_f64();

    let final_path = dest_dir.join("payload.bin");
    assert!(final_path.is_file(), "final destination file must exist");
    assert_eq!(std::fs::read(&final_path).unwrap(), contents);
    assert!(
        !partial_path.exists(),
        ".part must be removed after finalize"
    );
    assert!(!dest_dir.join("payload.bin.part.warpchunks").exists());

    println!(
        "{}",
        serde_json::json!({
            "wfp_version": format!("0.{}", WFP_VERSION_V03),
            "scenario": "resume_50_percent",
            "file_size_bytes": file_size_bytes,
            "chunk_size_bytes": V03_DEFAULT_CHUNK_SIZE,
            "reused_bytes": reused_bytes,
            "useful_data_bytes": useful_data_bytes,
            "protocol_bytes_sender_to_receiver": protocol_bytes_sender_to_receiver,
            "elapsed_ms": elapsed_ms,
            "effective_throughput_mib_s": effective_throughput_mib_s,
        })
    );
}
