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
