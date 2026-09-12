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

#[tokio::test]
async fn rejects_file_when_destination_already_exists() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&source_directory).unwrap();

    fs::create_dir_all(&destination_directory).unwrap();

    let source_path = source_directory.join("payload.bin");

    let destination_path = destination_directory.join("payload.bin");

    let source_data = b"this is the new file";

    let existing_data = b"this file already existed";

    fs::write(&source_path, source_data).unwrap();

    fs::write(&destination_path, existing_data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let receiver = receive_once(listener, &destination_directory);

    let sender = run_sender(&source_path_string, &address);

    let (receiver_result, sender_result) = tokio::join!(receiver, sender);

    assert!(
        receiver_result.is_err(),
        "receiver should reject an existing destination"
    );

    assert!(sender_result.is_err(), "sender should receive a REJECT");

    let sender_error = sender_result.unwrap_err().to_string();

    assert!(
        sender_error.contains("FILE_EXISTS"),
        "sender did not receive FILE_EXISTS: {sender_error}"
    );

    assert!(
        sender_error.contains("destination file already exists"),
        "sender did not receive the reject message: {sender_error}"
    );

    let final_destination_data = fs::read(&destination_path).unwrap();

    assert_eq!(
        final_destination_data, existing_data,
        "existing destination file was modified"
    );

    let partial_path = destination_directory.join("payload.bin.part");

    assert!(
        !partial_path.exists(),
        "receiver created a partial file even though the transfer was rejected"
    );
}
