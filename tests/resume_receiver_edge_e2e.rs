use std::fs;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};

use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{
    FileOffer, Frame, MessageType, TransferId, encode_offer, read_frame, write_frame,
};

use warpfile::receiver::receive_once;

#[tokio::test]
async fn ignores_empty_partial_and_starts_from_zero() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&destination_directory).unwrap();

    let partial_path = destination_directory.join(".warpfile/partials/empty-partial.bin.part");

    std::fs::create_dir_all(partial_path.parent().unwrap()).unwrap();

    /*
     * An empty .part exists.
     *
     * It contains zero useful bytes,
     * so it must not trigger RESUME.
     */
    fs::write(&partial_path, []).unwrap();

    let original_data: Vec<u8> = (0..20_000).map(|index| (index % 251) as u8).collect();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(&mut stream, "empty-partial.bin", original_data.len() as u64).await;

        /*
         * The important assertion:
         *
         * an empty .part must NOT cause
         * a RESUME frame.
         */
        let response = read_frame(&mut stream).await.unwrap();

        assert_eq!(
            response.message_type,
            MessageType::Accept,
            "receiver tried to resume an empty partial file instead of starting fresh"
        );

        assert!(response.payload.is_empty());

        send_complete_file(&mut stream, &original_data).await;

        expect_verified(&mut stream).await;
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed fresh transfer after empty partial: {:?}",
        receiver_result.err()
    );

    let final_path = destination_directory.join("empty-partial.bin");

    let received = fs::read(&final_path).unwrap();

    assert_eq!(received, original_data);

    assert!(
        !partial_path.exists(),
        "empty partial remained after successful transfer"
    );
}

#[tokio::test]
async fn discards_partial_larger_than_announced_file() {
    let temp = tempdir().unwrap();

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&destination_directory).unwrap();

    let partial_path = destination_directory.join(".warpfile/partials/oversized-partial.bin.part");

    std::fs::create_dir_all(partial_path.parent().unwrap()).unwrap();

    /*
     * The announced file will contain
     * only 10,000 bytes.
     *
     * This fake partial contains 25,000.
     *
     * It cannot possibly be a valid prefix
     * of the incoming file.
     */
    let impossible_partial = vec![0xAA; 25_000];

    fs::write(&partial_path, &impossible_partial).unwrap();

    let original_data: Vec<u8> = (0..10_000)
        .map(|index| ((index * 11) % 251) as u8)
        .collect();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let receiver = receive_once(listener, &destination_directory);

    let fake_sender = async {
        let mut stream = TcpStream::connect(address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer(
            &mut stream,
            "oversized-partial.bin",
            original_data.len() as u64,
        )
        .await;

        /*
         * Again, the receiver must NOT
         * propose RESUME.
         *
         * The oversized state is impossible,
         * so it should have been discarded.
         */
        let response = read_frame(&mut stream).await.unwrap();

        assert_eq!(
            response.message_type,
            MessageType::Accept,
            "receiver tried to resume from a partial larger than the announced file"
        );

        assert!(response.payload.is_empty());

        send_complete_file(&mut stream, &original_data).await;

        expect_verified(&mut stream).await;
    };

    let (receiver_result, _) = tokio::join!(receiver, fake_sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed after discarding oversized partial: {:?}",
        receiver_result.err()
    );

    let final_path = destination_directory.join("oversized-partial.bin");

    let received = fs::read(&final_path).unwrap();

    assert_eq!(
        received, original_data,
        "oversized stale partial contaminated the final file"
    );

    assert!(
        !partial_path.exists(),
        "oversized partial remained after successful fresh transfer"
    );
}

async fn perform_handshake(stream: &mut TcpStream) {
    let hello = Frame::new(MessageType::Hello, vec![WFP_VERSION]);

    write_frame(stream, &hello).await.unwrap();

    let hello_ack = read_frame(stream).await.unwrap();

    assert_eq!(hello_ack.message_type, MessageType::HelloAck);

    assert_eq!(hello_ack.payload, vec![WFP_VERSION]);
}

async fn send_offer(stream: &mut TcpStream, filename: &str, file_size: u64) {
    let offer = FileOffer {
        transfer_id: test_transfer_id(),
        filename: filename.to_string(),
        file_size,
    };

    let payload = encode_offer(&offer).unwrap();

    let frame = Frame::new(MessageType::Offer, payload);

    write_frame(stream, &frame).await.unwrap();
}

async fn send_complete_file(stream: &mut TcpStream, data: &[u8]) {
    let frame = Frame::new(MessageType::Data, data.to_vec());

    write_frame(stream, &frame).await.unwrap();

    let digest = blake3::hash(data);

    let complete = Frame::new(MessageType::Complete, digest.as_bytes().to_vec());

    write_frame(stream, &complete).await.unwrap();
}

async fn expect_verified(stream: &mut TcpStream) {
    let verified = read_frame(stream).await.unwrap();

    assert_eq!(verified.message_type, MessageType::Verified);

    assert!(verified.payload.is_empty());
}

fn test_transfer_id() -> TransferId {
    TransferId::from_bytes([
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
        0x1F,
    ])
}
