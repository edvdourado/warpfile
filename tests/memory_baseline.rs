// Run manually: cargo test --test memory_baseline -- --ignored --nocapture
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use proc_memstat::try_snapshot;
use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use warpfile::protocol::TransferId;
use warpfile::protocol::frame::WFP_VERSION_V03;
use warpfile::protocol::{
    MessageType, ProtocolIoError, decode_data_v03, read_frame_for_version, write_frame,
};
use warpfile::receiver_v03::run_receiver_v03;
use warpfile::sender_v03::{V03_DEFAULT_CHUNK_SIZE, send_session_v03};

const FILE_SIZE_BYTES: usize = 32 * 1024 * 1024;
const RUN_COUNT: usize = 5;
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Default)]
struct RunResult {
    rss_before: u64,
    rss_peak_observed: u64,
    rss_peak_observed_delta: u64,
    rss_after: u64,
}

fn current_rss_bytes() -> u64 {
    try_snapshot()
        .expect("process RSS snapshot must be available")
        .rss
}

async fn proxy_v03_memory_session(listener: &TcpListener, receiver_address: &str) -> u64 {
    let (sender, _) = listener.accept().await.unwrap();
    let receiver = TcpStream::connect(receiver_address).await.unwrap();
    let (mut sender_read, mut sender_write) = sender.into_split();
    let (mut receiver_read, mut receiver_write) = receiver.into_split();
    let sender_to_receiver = tokio::spawn(async move {
        let mut useful_data_bytes = 0u64;
        loop {
            match read_frame_for_version(&mut sender_read, WFP_VERSION_V03).await {
                Ok(frame) => {
                    if frame.message_type == MessageType::Data {
                        useful_data_bytes +=
                            decode_data_v03(&frame.payload).unwrap().data.len() as u64;
                    }
                    write_frame(&mut receiver_write, &frame).await.unwrap();
                }
                Err(ProtocolIoError::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("sender-to-receiver proxy failed: {error}"),
            }
        }
        useful_data_bytes
    });

    loop {
        let frame = read_frame_for_version(&mut receiver_read, WFP_VERSION_V03)
            .await
            .unwrap();
        let verified = frame.message_type == MessageType::Verified;
        write_frame(&mut sender_write, &frame).await.unwrap();
        if verified {
            break;
        }
    }
    drop(sender_write);
    tokio::time::timeout(Duration::from_secs(2), sender_to_receiver)
        .await
        .unwrap()
        .unwrap()
}

async fn run_fresh_once(run: usize) -> RunResult {
    let temp = tempdir().unwrap();
    let source_path = temp.path().join("payload.bin");
    let dest_dir = temp.path().join("receiver");
    let contents: Vec<u8> = (0..FILE_SIZE_BYTES)
        .map(|index| (index % 251) as u8)
        .collect();
    std::fs::write(&source_path, &contents).unwrap();
    drop(contents);

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
        tokio::spawn(async move { proxy_v03_memory_session(&proxy, &receiver_address).await });

    let rss_before = current_rss_bytes();
    let observed_peak = Arc::new(AtomicU64::new(rss_before));
    let sampler_peak = Arc::clone(&observed_peak);
    let (stop_sampler, stopped) = oneshot::channel();
    let (sampler_ready, ready) = oneshot::channel();
    let sampler = tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + RSS_SAMPLE_INTERVAL,
            RSS_SAMPLE_INTERVAL,
        );
        let _ = sampler_ready.send(());
        tokio::pin!(stopped);
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                _ = interval.tick() => {
                    sampler_peak.fetch_max(current_rss_bytes(), Ordering::Relaxed);
                }
            }
        }
    });
    ready.await.unwrap();
    send_session_v03(
        &source_path,
        &proxy_address,
        TransferId::generate().unwrap(),
        NonZeroU64::new(V03_DEFAULT_CHUNK_SIZE).unwrap(),
    )
    .await
    .unwrap();
    let _ = stop_sampler.send(());
    sampler.await.unwrap();
    let rss_after = current_rss_bytes();

    let useful_data_bytes = proxy_task.await.unwrap();
    receiver.abort();
    let _ = receiver.await;
    let final_path = dest_dir.join("payload.bin");
    assert!(final_path.is_file(), "final destination file must exist");
    assert_eq!(
        std::fs::read(&final_path).unwrap(),
        std::fs::read(&source_path).unwrap()
    );
    assert_eq!(useful_data_bytes, FILE_SIZE_BYTES as u64);
    assert!(!dest_dir.join("payload.bin.part").exists());
    assert!(!dest_dir.join("payload.bin.part.warpchunks").exists());

    let rss_peak_observed = observed_peak.load(Ordering::Relaxed);
    // RSS growth below the initial reading is reported as zero, never as a negative value.
    let rss_peak_observed_delta = rss_peak_observed.saturating_sub(rss_before);
    let result = RunResult {
        rss_before,
        rss_peak_observed,
        rss_peak_observed_delta,
        rss_after,
    };
    println!(
        "{}",
        serde_json::json!({
            "record_type": "memory_run",
            "scenario": "fresh_transfer",
            "run": run,
            "wfp_version": format!("0.{}", WFP_VERSION_V03),
            "file_size_bytes": FILE_SIZE_BYTES as u64,
            "chunk_size_bytes": V03_DEFAULT_CHUNK_SIZE,
            "benchmark_process_rss_before_bytes": result.rss_before,
            "benchmark_process_rss_peak_observed_bytes": result.rss_peak_observed,
            "benchmark_process_rss_peak_observed_delta_bytes": result.rss_peak_observed_delta,
            "benchmark_process_rss_after_bytes": result.rss_after,
            "rss_sample_interval_ms": RSS_SAMPLE_INTERVAL.as_millis(),
        })
    );
    result
}

fn median(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

#[tokio::test]
#[ignore = "manual local memory baseline"]
async fn memory_baseline() {
    let started = Instant::now();
    let mut results = Vec::with_capacity(RUN_COUNT);
    for run in 1..=RUN_COUNT {
        results.push(run_fresh_once(run).await);
    }
    let mut before: Vec<_> = results.iter().map(|r| r.rss_before).collect();
    let mut peak: Vec<_> = results.iter().map(|r| r.rss_peak_observed).collect();
    let mut delta: Vec<_> = results.iter().map(|r| r.rss_peak_observed_delta).collect();
    let mut after: Vec<_> = results.iter().map(|r| r.rss_after).collect();
    println!(
        "{}",
        serde_json::json!({
            "record_type": "memory_summary",
            "scenario": "fresh_transfer",
            "run_count": RUN_COUNT,
            "median_benchmark_process_rss_before_bytes": median(&mut before),
            "median_benchmark_process_rss_peak_observed_bytes": median(&mut peak),
            "median_benchmark_process_rss_peak_observed_delta_bytes": median(&mut delta),
            "median_benchmark_process_rss_after_bytes": median(&mut after),
        })
    );
    eprintln!(
        "memory baseline elapsed: {:.1}s",
        started.elapsed().as_secs_f64()
    );
}
