use std::fs;

use tempfile::tempdir;
use tokio::net::TcpListener;

use warpfile::receiver::receive_once;
use warpfile::sender::run_sender;

#[tokio::test]
async fn transfers_file_end_to_end() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("payload.bin");

    let original_data: Vec<u8> = (0..200_000).map(|index| (index % 251) as u8).collect();

    fs::write(&source_path, &original_data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let receiver = receive_once(listener, &destination_directory);

    let sender = run_sender(&source_path_string, &address);

    let (receiver_result, sender_result) = tokio::join!(receiver, sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed: {:?}",
        receiver_result.err()
    );

    assert!(
        sender_result.is_ok(),
        "sender failed: {:?}",
        sender_result.err()
    );

    let received_path = destination_directory.join("payload.bin");

    let received_data = fs::read(received_path).unwrap();

    assert_eq!(received_data, original_data);
}
