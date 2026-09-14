use std::fs;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};

use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{FileOffer, Frame, MessageType, encode_offer, read_frame, write_frame};

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
async fn transfers_empty_file_end_to_end() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("empty.bin");

    fs::write(&source_path, []).unwrap();

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

    let received_path = destination_directory.join("empty.bin");

    assert!(
        received_path.exists(),
        "empty destination file was not created"
    );

    let metadata = fs::metadata(&received_path).unwrap();

    assert_eq!(metadata.len(), 0, "received empty file is not empty");

    let partial_path = destination_directory.join("empty.bin.part");

    assert!(
        !partial_path.exists(),
        "partial file remained after successful empty transfer"
    );
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

#[tokio::test]
async fn preserves_partial_file_when_sender_disconnects() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer_and_wait_for_accept(&mut stream, "interrupted.bin", 100_000).await;

        let partial_data = vec![0xAB; 4096];

        let data = Frame::new(MessageType::Data, partial_data);

        write_frame(&mut stream, &data).await.unwrap();

        drop(stream);
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_err(),
        "receiver should report an interrupted transfer"
    );

    let final_path = destination_directory.join("interrupted.bin");

    assert!(
        !final_path.exists(),
        "receiver created a final file after an interrupted transfer"
    );

    let partial_path = destination_directory.join("interrupted.bin.part");

    assert!(
        partial_path.exists(),
        "partial file was removed after a recoverable connection loss"
    );

    let partial_data = fs::read(&partial_path).unwrap();

    assert_eq!(
        partial_data,
        vec![0xAB; 4096],
        "preserved partial file does not contain the bytes received before disconnection"
    );
}

#[tokio::test]
async fn rejects_file_with_invalid_hash() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        let data = vec![0xCD; 4096];

        send_offer_and_wait_for_accept(&mut stream, "corrupted.bin", data.len() as u64).await;

        let data_frame = Frame::new(MessageType::Data, data.clone());

        write_frame(&mut stream, &data_frame).await.unwrap();

        let real_hash = blake3::hash(&data);

        let mut wrong_hash = real_hash.as_bytes().to_vec();

        wrong_hash[0] ^= 0xFF;

        let complete = Frame::new(MessageType::Complete, wrong_hash);

        write_frame(&mut stream, &complete).await.unwrap();
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_err(),
        "receiver should reject a file with an invalid hash"
    );

    let receiver_error = receiver_result.unwrap_err().to_string();

    assert!(
        receiver_error.contains("file integrity verification failed"),
        "receiver reported the wrong error: {receiver_error}"
    );

    let final_path = destination_directory.join("corrupted.bin");

    assert!(
        !final_path.exists(),
        "receiver created a final file even though integrity verification failed"
    );

    let partial_path = destination_directory.join("corrupted.bin.part");

    assert!(
        !partial_path.exists(),
        "partial file remained after integrity verification failed"
    );
}

#[tokio::test]
async fn removes_partial_file_when_sender_cancels() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer_and_wait_for_accept(&mut stream, "cancelled.bin", 100_000).await;

        let data = Frame::new(MessageType::Data, vec![0xEF; 4096]);

        write_frame(&mut stream, &data).await.unwrap();

        let cancel = Frame::new(MessageType::Cancel, Vec::new());

        write_frame(&mut stream, &cancel).await.unwrap();
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_err(),
        "receiver should report a cancelled transfer"
    );

    let receiver_error = receiver_result.unwrap_err().to_string();

    assert!(
        receiver_error.contains("transfer cancelled by sender"),
        "receiver reported the wrong cancellation error: {receiver_error}"
    );

    let final_path = destination_directory.join("cancelled.bin");

    assert!(
        !final_path.exists(),
        "receiver created a final file after cancellation"
    );

    let partial_path = destination_directory.join("cancelled.bin.part");

    assert!(
        !partial_path.exists(),
        "partial file remained after cancellation"
    );
}

async fn perform_handshake(stream: &mut TcpStream) {
    let hello = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

    write_frame(stream, &hello).await.unwrap();

    let hello_ack = read_frame(stream).await.unwrap();

    assert_eq!(hello_ack.message_type, MessageType::HelloAck);

    assert_eq!(hello_ack.payload, vec![WFP_VERSION]);
}

async fn send_offer_and_wait_for_accept(stream: &mut TcpStream, filename: &str, file_size: u64) {
    let offer = FileOffer {
        filename: filename.to_string(),
        file_size,
    };

    let offer_payload = encode_offer(&offer).unwrap();

    let offer_frame = Frame::new(MessageType::Offer, offer_payload);

    write_frame(stream, &offer_frame).await.unwrap();

    let accept = read_frame(stream).await.unwrap();

    assert_eq!(accept.message_type, MessageType::Accept);

    assert!(accept.payload.is_empty());
}
