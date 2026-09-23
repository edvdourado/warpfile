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
const RUN_COUNT: usize = 5;
const MIB: f64 = 1024.0 * 1024.0;

struct RunResult {
    useful_data_bytes: u64,
    reused_bytes: u64,
    protocol_bytes_sender_to_receiver: u64,
    elapsed_ms: f64,
    effective_throughput_mib_s: f64,
}

fn print_run(scenario: &str, run: usize, result: &RunResult) {
    println!(
        "{}",
        serde_json::json!({
            "wfp_version": format!("0.{}", WFP_VERSION_V03),
            "scenario": scenario,
            "file_size_bytes": FILE_SIZE_BYTES as u64,
            "chunk_size_bytes": V03_DEFAULT_CHUNK_SIZE,
            "reused_bytes": result.reused_bytes,
            "useful_data_bytes": result.useful_data_bytes,
            "protocol_bytes_sender_to_receiver": result.protocol_bytes_sender_to_receiver,
            "elapsed_ms": result.elapsed_ms,
            "effective_throughput_mib_s": result.effective_throughput_mib_s,
            "run": run,
        })
    );
}

fn print_summary(scenario: &str, results: &[RunResult]) {
    let mut elapsed: Vec<f64> = results.iter().map(|result| result.elapsed_ms).collect();
    elapsed.sort_by(f64::total_cmp);
    let median_elapsed_ms = elapsed[RUN_COUNT / 2];
    println!(
        "{}",
        serde_json::json!({
            "record_type": "summary",
            "scenario": scenario,
            "run_count": RUN_COUNT,
            "file_size_bytes": FILE_SIZE_BYTES as u64,
            "chunk_size_bytes": V03_DEFAULT_CHUNK_SIZE,
            "median_elapsed_ms": median_elapsed_ms,
            "median_effective_throughput_mib_s": FILE_SIZE_BYTES as f64 / MIB / (median_elapsed_ms / 1000.0),
        })
    );
}

async fn run_fresh_once() -> RunResult {
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

    let useful_data_bytes: u64 = frames
        .iter()
        .filter(|frame| frame.message_type == MessageType::Data)
        .map(|frame| decode_data_v03(&frame.payload).unwrap().data.len() as u64)
        .sum();
    let protocol_bytes_sender_to_receiver: u64 = frames
        .iter()
        .map(|frame| encode_frame(frame).unwrap().len() as u64)
        .sum();
    let file_size_bytes = FILE_SIZE_BYTES as u64;
    let reused_bytes = file_size_bytes.checked_sub(useful_data_bytes).unwrap();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let effective_throughput_mib_s = file_size_bytes as f64 / MIB / elapsed.as_secs_f64();

    let final_path = dest_dir.join("payload.bin");
    assert!(final_path.is_file(), "final destination file must exist");
    assert_eq!(std::fs::read(&final_path).unwrap(), contents);
    assert_eq!(useful_data_bytes, file_size_bytes);
    assert_eq!(reused_bytes, 0);
    assert!(!dest_dir.join("payload.bin.part").exists());
    assert!(!dest_dir.join("payload.bin.part.warpchunks").exists());
    RunResult {
        useful_data_bytes,
        reused_bytes,
        protocol_bytes_sender_to_receiver,
        elapsed_ms,
        effective_throughput_mib_s,
    }
}

async fn run_resume_once(reused_chunk_count: u64) -> RunResult {
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
    assert!(reused_chunk_count <= layout.chunk_count());
    let partial_path = dest_dir.join("payload.bin.part");
    let mut partial = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial_path)
        .unwrap();
    partial.set_len(file_size_bytes).unwrap();
    let mut records = Vec::new();
    for index in 0..reused_chunk_count {
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
        (0..reused_chunk_count).collect::<Vec<_>>()
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
    assert_eq!(
        chunk_starts,
        (reused_chunk_count..layout.chunk_count()).collect::<Vec<_>>()
    );
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
    let expected_reused_bytes = reused_chunk_count * V03_DEFAULT_CHUNK_SIZE;
    let expected_useful_data_bytes = file_size_bytes
        .checked_sub(expected_reused_bytes)
        .expect("expected reused bytes cannot exceed file size");
    assert_eq!(useful_data_bytes, expected_useful_data_bytes);
    assert_eq!(reused_bytes, expected_reused_bytes);
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
    RunResult {
        useful_data_bytes,
        reused_bytes,
        protocol_bytes_sender_to_receiver,
        elapsed_ms,
        effective_throughput_mib_s,
    }
}

#[tokio::test]
#[ignore = "manual local performance baseline"]
async fn performance_baseline() {
    let mut fresh_results = Vec::with_capacity(RUN_COUNT);
    let mut resume_25_results = Vec::with_capacity(RUN_COUNT);
    let mut resume_50_results = Vec::with_capacity(RUN_COUNT);
    let mut resume_75_results = Vec::with_capacity(RUN_COUNT);
    let mut resume_87_5_results = Vec::with_capacity(RUN_COUNT);
    for run in 1..=RUN_COUNT {
        let fresh = run_fresh_once().await;
        print_run("fresh_transfer", run, &fresh);
        fresh_results.push(fresh);

        let resume_25 = run_resume_once(8).await;
        print_run("resume_25_percent", run, &resume_25);
        resume_25_results.push(resume_25);

        let resume_50 = run_resume_once(16).await;
        print_run("resume_50_percent", run, &resume_50);
        resume_50_results.push(resume_50);

        let resume_75 = run_resume_once(24).await;
        print_run("resume_75_percent", run, &resume_75);
        resume_75_results.push(resume_75);

        let resume_87_5 = run_resume_once(28).await;
        print_run("resume_87_5_percent", run, &resume_87_5);
        resume_87_5_results.push(resume_87_5);
    }
    print_summary("fresh_transfer", &fresh_results);
    print_summary("resume_25_percent", &resume_25_results);
    print_summary("resume_50_percent", &resume_50_results);
    print_summary("resume_75_percent", &resume_75_results);
    print_summary("resume_87_5_percent", &resume_87_5_results);
}
