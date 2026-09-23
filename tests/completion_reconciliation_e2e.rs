use std::fs;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};

use warpfile::completion_receipt::{
    CompletionReceipt, completion_receipt_path, read_completion_receipt, write_completion_receipt,
};
use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{
    FileOffer, Frame, MessageType, RejectCode, TransferId, decode_reject, encode_offer, read_frame,
    write_frame,
};
use warpfile::receiver::receive_once;
use warpfile::transfer_metadata::{
    TransferMetadata, TransferState, transfer_metadata_path, write_transfer_metadata,
};

#[tokio::test]
async fn successful_transfer_persists_completion_receipt() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    let transfer_id = transfer_id_a();

    let original_data: Vec<u8> = (0..12_000).map(|index| (index % 251) as u8).collect();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(
            &mut stream,
            transfer_id,
            "receipt.bin",
            original_data.len() as u64,
        )
        .await;

        expect_accept(&mut stream).await;

        let data = Frame::new(MessageType::Data, original_data.clone());

        write_frame(&mut stream, &data).await.unwrap();

        let digest = blake3::hash(&original_data);

        let complete = Frame::new(MessageType::Complete, digest.as_bytes().to_vec());

        write_frame(&mut stream, &complete).await.unwrap();

        expect_verified(&mut stream).await;
    };

    let (receiver_result, _) = tokio::join!(receiver, sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed successful transfer: {:?}",
        receiver_result.err()
    );

    let receipt = read_completion_receipt(&destination_directory, transfer_id)
        .await
        .unwrap();

    assert_eq!(receipt.transfer_id, transfer_id);

    assert_eq!(receipt.filename, "receipt.bin");

    assert_eq!(receipt.file_size, original_data.len() as u64);

    assert_eq!(receipt.blake3, *blake3::hash(&original_data).as_bytes());

    let final_path = destination_directory.join("receipt.bin");

    assert_eq!(fs::read(final_path).unwrap(), original_data);
}

#[tokio::test]
async fn reconciles_existing_final_file_from_completion_receipt() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&destination_directory).unwrap();

    let transfer_id = transfer_id_a();

    let data: Vec<u8> = (0..8000).map(|index| (index % 239) as u8).collect();

    let final_path = destination_directory.join("already-complete.bin");

    fs::write(&final_path, &data).unwrap();

    let receipt = CompletionReceipt {
        transfer_id,
        filename: "already-complete.bin".to_string(),
        file_size: data.len() as u64,
        blake3: *blake3::hash(&data).as_bytes(),
    };

    write_completion_receipt(&destination_directory, &receipt)
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(
            &mut stream,
            transfer_id,
            "already-complete.bin",
            data.len() as u64,
        )
        .await;

        /*
         * There must be no ACCEPT and no DATA phase.
         *
         * The valid receipt + valid final file are enough
         * to reconcile this logical transfer.
         */
        expect_verified(&mut stream).await;
    };

    let (receiver_result, _) = tokio::join!(receiver, sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed completion reconciliation: {:?}",
        receiver_result.err()
    );

    assert_eq!(fs::read(final_path).unwrap(), data);
}

#[tokio::test]
async fn commits_completed_partial_from_completion_receipt() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&destination_directory).unwrap();

    let transfer_id = transfer_id_a();

    let data: Vec<u8> = (0..9000).map(|index| (index % 223) as u8).collect();

    let partial_path = destination_directory.join(".warpfile/partials/crash-window.bin.part");

    std::fs::create_dir_all(partial_path.parent().unwrap()).unwrap();

    let final_path = destination_directory.join("crash-window.bin");

    fs::write(&partial_path, &data).unwrap();

    let metadata = TransferMetadata {
        transfer_id,
        filename: "crash-window.bin".to_string(),
        file_size: data.len() as u64,
        state: TransferState::Partial,
    };

    write_transfer_metadata(&partial_path, &metadata)
        .await
        .unwrap();

    let receipt = CompletionReceipt {
        transfer_id,
        filename: "crash-window.bin".to_string(),
        file_size: data.len() as u64,
        blake3: *blake3::hash(&data).as_bytes(),
    };

    write_completion_receipt(&destination_directory, &receipt)
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(
            &mut stream,
            transfer_id,
            "crash-window.bin",
            data.len() as u64,
        )
        .await;

        expect_verified(&mut stream).await;
    };

    let (receiver_result, _) = tokio::join!(receiver, sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed completed-partial reconciliation: {:?}",
        receiver_result.err()
    );

    assert!(final_path.exists(), "completed partial was not committed");

    assert!(
        !partial_path.exists(),
        "completed partial remained after reconciliation"
    );

    let metadata_path = transfer_metadata_path(&partial_path);

    assert!(
        !metadata_path.exists(),
        "partial transfer metadata remained after reconciliation"
    );

    assert_eq!(fs::read(final_path).unwrap(), data);
}

#[tokio::test]
async fn refuses_verified_when_final_file_does_not_match_receipt() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&destination_directory).unwrap();

    let transfer_id = transfer_id_a();

    let expected_data = vec![0x44; 4096];

    let tampered_data = vec![0x55; 4096];

    let final_path = destination_directory.join("tampered.bin");

    fs::write(&final_path, &tampered_data).unwrap();

    let receipt = CompletionReceipt {
        transfer_id,
        filename: "tampered.bin".to_string(),
        file_size: expected_data.len() as u64,
        blake3: *blake3::hash(&expected_data).as_bytes(),
    };

    write_completion_receipt(&destination_directory, &receipt)
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(
            &mut stream,
            transfer_id,
            "tampered.bin",
            expected_data.len() as u64,
        )
        .await;

        let response = read_frame(&mut stream).await.unwrap();

        assert_eq!(
            response.message_type,
            MessageType::Reject,
            "receiver trusted a completion receipt without verifying the final file"
        );

        let reject = decode_reject(&response.payload).unwrap();

        assert_eq!(reject.code, RejectCode::CannotPrepareDestination);
    };

    let (receiver_result, _) = tokio::join!(receiver, sender);

    assert!(
        receiver_result.is_err(),
        "receiver should reject a final file that does not match its completion receipt"
    );

    /*
     * Reconciliation must never mutate the file merely because
     * a receipt exists.
     */
    assert_eq!(fs::read(final_path).unwrap(), tampered_data);

    let receipt_path = completion_receipt_path(&destination_directory, transfer_id);

    assert!(
        receipt_path.exists(),
        "failed reconciliation unexpectedly removed the completion receipt"
    );
}

async fn perform_handshake(stream: &mut TcpStream) {
    let hello = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

    write_frame(stream, &hello).await.unwrap();

    let hello_ack = read_frame(stream).await.unwrap();

    assert_eq!(hello_ack.message_type, MessageType::HelloAck);

    assert_eq!(hello_ack.payload, vec![WFP_VERSION]);
}

async fn send_offer(
    stream: &mut TcpStream,
    transfer_id: TransferId,
    filename: &str,
    file_size: u64,
) {
    let offer = FileOffer {
        transfer_id,
        filename: filename.to_string(),
        file_size,
    };

    let payload = encode_offer(&offer).unwrap();

    let frame = Frame::new(MessageType::Offer, payload);

    write_frame(stream, &frame).await.unwrap();
}

async fn expect_accept(stream: &mut TcpStream) {
    let response = read_frame(stream).await.unwrap();

    assert_eq!(response.message_type, MessageType::Accept);

    assert!(response.payload.is_empty());
}

async fn expect_verified(stream: &mut TcpStream) {
    let response = read_frame(stream).await.unwrap();

    assert_eq!(response.message_type, MessageType::Verified);

    assert!(response.payload.is_empty());
}

fn transfer_id_a() -> TransferId {
    TransferId::from_bytes([
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E,
        0x3F,
    ])
}
