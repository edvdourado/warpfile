use std::fs;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};

use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{
    FileOffer, Frame, MessageType, TransferId, decode_resume, encode_offer, read_frame, write_frame,
};
use warpfile::receiver::receive_once;
use warpfile::transfer_metadata::{
    TransferMetadata, TransferState, read_transfer_metadata, transfer_metadata_path,
    write_transfer_metadata,
};

#[tokio::test]
async fn preserves_transfer_metadata_after_connection_loss() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    let partial_path = destination_directory.join("interrupted.bin.part");

    let transfer_id = transfer_id_a();

    let file_size = 10_000u64;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(&mut stream, transfer_id, "interrupted.bin", file_size).await;

        expect_accept(&mut stream).await;

        let data = Frame::new(MessageType::Data, vec![0xAB; 4096]);

        write_frame(&mut stream, &data).await.unwrap();

        drop(stream);
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_err(),
        "receiver should report the interrupted transfer"
    );

    assert!(partial_path.exists(), "partial file was not preserved");

    let metadata_path = transfer_metadata_path(&partial_path);

    assert!(
        metadata_path.exists(),
        "transfer metadata was not preserved"
    );

    let metadata = read_transfer_metadata(&partial_path).await.unwrap();

    assert_eq!(metadata.transfer_id, transfer_id);

    assert_eq!(metadata.filename, "interrupted.bin");

    assert_eq!(metadata.file_size, file_size);

    assert_eq!(metadata.state, TransferState::Partial);
}

#[tokio::test]
async fn adopts_verified_partial_for_new_transfer_identity() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&destination_directory).unwrap();

    let partial_path = destination_directory.join("adopt.bin.part");

    let prefix = vec![0x11; 4096];

    fs::write(&partial_path, &prefix).unwrap();

    let old_transfer_id = transfer_id_a();

    let new_transfer_id = transfer_id_b();

    let offered_size = 12_000u64;

    let old_metadata = TransferMetadata {
        transfer_id: old_transfer_id,
        filename: "adopt.bin".to_string(),
        file_size: offered_size,
        state: TransferState::Partial,
    };

    write_transfer_metadata(&partial_path, &old_metadata)
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(&mut stream, new_transfer_id, "adopt.bin", offered_size).await;

        let resume_frame = read_frame(&mut stream).await.unwrap();

        assert_eq!(resume_frame.message_type, MessageType::Resume);

        let resume = decode_resume(&resume_frame.payload).unwrap();

        assert_eq!(resume.offset, prefix.len() as u64);

        assert_eq!(resume.prefix_hash, *blake3::hash(&prefix).as_bytes());

        let accept = Frame::new(MessageType::Accept, Vec::new());

        write_frame(&mut stream, &accept).await.unwrap();

        let additional_data = Frame::new(MessageType::Data, vec![0x22; 1024]);

        write_frame(&mut stream, &additional_data).await.unwrap();

        drop(stream);
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_err(),
        "receiver should report the intentional connection loss"
    );

    let metadata = read_transfer_metadata(&partial_path).await.unwrap();

    assert_eq!(
        metadata.transfer_id, new_transfer_id,
        "receiver did not adopt the verified partial for the new transfer"
    );

    assert_eq!(metadata.filename, "adopt.bin");

    assert_eq!(metadata.file_size, offered_size);

    let partial_data = fs::read(&partial_path).unwrap();

    let mut expected = prefix;

    expected.extend_from_slice(&vec![0x22; 1024]);

    assert_eq!(
        partial_data, expected,
        "verified prefix was not preserved during adoption"
    );
}

#[tokio::test]
async fn restart_replaces_partial_and_transfer_metadata() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&destination_directory).unwrap();

    let partial_path = destination_directory.join("restart.bin.part");

    let stale_prefix = vec![0xAA; 4096];

    fs::write(&partial_path, &stale_prefix).unwrap();

    let old_transfer_id = transfer_id_a();

    let new_transfer_id = transfer_id_b();

    let offered_size = 10_000u64;

    let old_metadata = TransferMetadata {
        transfer_id: old_transfer_id,
        filename: "restart.bin".to_string(),
        file_size: offered_size,
        state: TransferState::Partial,
    };

    write_transfer_metadata(&partial_path, &old_metadata)
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(&mut stream, new_transfer_id, "restart.bin", offered_size).await;

        let resume_frame = read_frame(&mut stream).await.unwrap();

        assert_eq!(resume_frame.message_type, MessageType::Resume);

        let resume = decode_resume(&resume_frame.payload).unwrap();

        assert_eq!(resume.offset, stale_prefix.len() as u64);

        let restart = Frame::new(MessageType::Restart, Vec::new());

        write_frame(&mut stream, &restart).await.unwrap();

        expect_accept(&mut stream).await;

        let new_data = Frame::new(MessageType::Data, vec![0x33; 1024]);

        write_frame(&mut stream, &new_data).await.unwrap();

        drop(stream);
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_err(),
        "receiver should report the intentional connection loss"
    );

    let partial_data = fs::read(&partial_path).unwrap();

    assert_eq!(
        partial_data,
        vec![0x33; 1024],
        "stale partial bytes survived RESTART"
    );

    let metadata = read_transfer_metadata(&partial_path).await.unwrap();

    assert_eq!(
        metadata.transfer_id, new_transfer_id,
        "metadata still belongs to the rejected transfer"
    );

    assert_eq!(metadata.filename, "restart.bin");

    assert_eq!(metadata.file_size, offered_size);
}

#[tokio::test]
async fn removes_partial_metadata_after_successful_transfer() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    let partial_path = destination_directory.join("complete.bin.part");

    let final_path = destination_directory.join("complete.bin");

    let transfer_id = transfer_id_a();

    let original_data: Vec<u8> = (0..5000).map(|index| (index % 251) as u8).collect();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(
            &mut stream,
            transfer_id,
            "complete.bin",
            original_data.len() as u64,
        )
        .await;

        expect_accept(&mut stream).await;

        let data = Frame::new(MessageType::Data, original_data.clone());

        write_frame(&mut stream, &data).await.unwrap();

        let digest = blake3::hash(&original_data);

        let complete = Frame::new(MessageType::Complete, digest.as_bytes().to_vec());

        write_frame(&mut stream, &complete).await.unwrap();

        let verified = read_frame(&mut stream).await.unwrap();

        assert_eq!(verified.message_type, MessageType::Verified);

        assert!(verified.payload.is_empty());
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed successful transfer: {:?}",
        receiver_result.err()
    );

    assert!(final_path.exists(), "final file was not created");

    assert!(
        !partial_path.exists(),
        "partial file remained after completion"
    );

    let metadata_path = transfer_metadata_path(&partial_path);

    assert!(
        !metadata_path.exists(),
        "partial transfer metadata remained after completion"
    );

    let received = fs::read(final_path).unwrap();

    assert_eq!(received, original_data);
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
    let accept = read_frame(stream).await.unwrap();

    assert_eq!(accept.message_type, MessageType::Accept);

    assert!(accept.payload.is_empty());
}

fn transfer_id_a() -> TransferId {
    TransferId::from_bytes([
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
        0x1F,
    ])
}

fn transfer_id_b() -> TransferId {
    TransferId::from_bytes([
        0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E,
        0x2F,
    ])
}
