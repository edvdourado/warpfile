use std::time::Duration;

use tempfile::tempdir;
use tokio::net::TcpListener;

use warpfile::completion_receipt::decode_completion_receipt;
use warpfile::receiver_v03::run_receiver_v03;
use warpfile::sender_v03::run_sender_v03;

#[tokio::test]
async fn wfp_v03_transfers_a_small_file_end_to_end() {
    let temp = tempdir().unwrap();

    let source_dir = temp.path().join("source");
    let dest_dir = temp.path().join("dest");

    std::fs::create_dir_all(&source_dir).unwrap();

    let contents: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
    let source_path = source_dir.join("payload.bin");
    std::fs::write(&source_path, &contents).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);

    let dest_clone = dest_dir.clone();
    let receiver_address = address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_address, &dest_clone).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    run_sender_v03(&source_path, &address).await.unwrap();

    let final_path = dest_dir.join("payload.bin");
    let wait = async {
        for _ in 0..50 {
            if final_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap();

    receiver.abort();
    let _ = receiver.await;

    assert!(final_path.exists(), "destination file must exist");

    let received = std::fs::read(&final_path).unwrap();
    assert_eq!(received, contents, "payload bytes must match");

    let receipts_dir = dest_dir.join(".warpfile").join("receipts");
    assert!(receipts_dir.is_dir(), "receipts directory must exist");

    let mut matched_receipt = false;
    for entry in std::fs::read_dir(&receipts_dir).unwrap() {
        let path = entry.unwrap().path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let receipt = decode_completion_receipt(&std::fs::read(&path).unwrap()).unwrap();

        if receipt.filename == "payload.bin" && receipt.file_size == contents.len() as u64 {
            matched_receipt = true;
        }
    }

    assert!(
        matched_receipt,
        "a completion receipt for payload.bin must be persisted"
    );
}

#[tokio::test]
async fn wfp_v03_resumes_from_preseeded_partial() {
    use std::io::ErrorKind;
    use std::num::NonZeroU64;

    use warpfile::chunk::ChunkLayout;
    use warpfile::chunk_manifest::ChunkHash;
    use warpfile::chunk_state::{ChunkState, RecordedChunk, encode_chunk_state};
    use warpfile::protocol::TransferId;
    use warpfile::sender_v03::{SenderV03SessionError, send_session_v03};

    let temp = tempdir().unwrap();

    let dest_dir = temp.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();

    let contents: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
    let chunk_size: u64 = 1024;
    let layout = ChunkLayout::new(4096, chunk_size).unwrap();

    // Preseed the first two chunks as verified: [0..1024] and [1024..2048].
    let mut records = Vec::new();
    for index in 0..2u64 {
        let range = layout.range(index).unwrap();
        let start = range.offset as usize;
        let end = (range.offset + range.length) as usize;
        records.push(RecordedChunk {
            index,
            hash: ChunkHash::from_bytes(*blake3::hash(&contents[start..end]).as_bytes()),
        });
    }
    let state = ChunkState::new(layout, records).unwrap();

    let partial = dest_dir.join("payload.bin.part");
    std::fs::write(&partial, &contents[..2048]).unwrap();
    std::fs::write(
        dest_dir.join("payload.bin.part.warpchunks"),
        encode_chunk_state(&state).unwrap(),
    )
    .unwrap();

    // The sender derives the destination filename from the source path,
    // so the source must share the preseeded partial's basename.
    let source_path = temp.path().join("payload.bin");
    std::fs::write(&source_path, &contents).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);

    let dest_clone = dest_dir.clone();
    let receiver_address = address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_address, &dest_clone).await;
    });

    let transfer_id = TransferId::generate().unwrap();

    // send_session_v03 is single-shot, so retry while the receiver is
    // still binding; only ConnectionRefused is expected during startup.
    let mut connected = false;
    for _ in 0..50 {
        match send_session_v03(
            &source_path,
            &address,
            transfer_id,
            NonZeroU64::new(chunk_size).unwrap(),
        )
        .await
        {
            Ok(()) => {
                connected = true;
                break;
            }
            Err(SenderV03SessionError::Connect(error))
                if error.kind() == ErrorKind::ConnectionRefused =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("WFP/0.3 session failed: {error}"),
        }
    }
    assert!(connected, "receiver never started listening");

    receiver.abort();
    let _ = receiver.await;

    let final_path = dest_dir.join("payload.bin");
    assert_eq!(std::fs::read(&final_path).unwrap(), contents);
    assert!(!partial.exists(), ".part must be renamed after finalize");
    assert!(
        !dest_dir.join("payload.bin.part.warpchunks").exists(),
        ".warpchunks must be removed after finalize"
    );

    let receipts_dir = dest_dir.join(".warpfile").join("receipts");
    assert!(receipts_dir.is_dir(), "receipts directory must exist");

    let mut matched_receipt = false;
    for entry in std::fs::read_dir(&receipts_dir).unwrap() {
        let path = entry.unwrap().path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let receipt = decode_completion_receipt(&std::fs::read(&path).unwrap()).unwrap();

        if receipt.filename == "payload.bin" && receipt.file_size == 4096 {
            matched_receipt = true;
        }
    }

    assert!(
        matched_receipt,
        "a completion receipt for payload.bin must be persisted"
    );
}
