use std::fs;
use std::time::Duration;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{
    FileReject, Frame, MessageType, RejectCode, decode_offer, encode_reject, read_frame,
    write_frame,
};
use warpfile::sender::run_sender;

const EXPECTED_MAX_ATTEMPTS: usize = 3;
const NO_EXTRA_CONNECTION_WINDOW: Duration = Duration::from_millis(1_500);

#[tokio::test]
async fn sender_stops_after_maximum_retry_attempts() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("retry-limit.bin");

    let original_data: Vec<u8> = (0..100_000)
        .map(|index| ((index * 11) % 251) as u8)
        .collect();

    fs::write(&source_path, &original_data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let fake_receiver = async {
        for _ in 0..EXPECTED_MAX_ATTEMPTS {
            let (mut stream, _peer) = listener.accept().await.unwrap();

            let hello = read_frame(&mut stream).await.unwrap();

            assert_eq!(hello.message_type, MessageType::Hello);
            assert_eq!(hello.payload, vec![WFP_VERSION]);

            drop(stream);
        }

        let fourth_connection = timeout(NO_EXTRA_CONNECTION_WINDOW, listener.accept()).await;

        assert!(
            fourth_connection.is_err(),
            "sender attempted more than {EXPECTED_MAX_ATTEMPTS} transfer sessions"
        );
    };

    let sender = run_sender(&source_path_string, &address);

    let (_, sender_result) = timeout(Duration::from_secs(8), async {
        tokio::join!(fake_receiver, sender)
    })
    .await
    .expect("retry limit test timed out");

    assert!(
        sender_result.is_err(),
        "sender unexpectedly succeeded after every transfer attempt was interrupted"
    );
}

#[tokio::test]
async fn sender_does_not_retry_permanent_receiver_reject() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("permanent-reject.bin");

    let original_data: Vec<u8> = (0..100_000)
        .map(|index| ((index * 23) % 251) as u8)
        .collect();

    fs::write(&source_path, &original_data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let fake_receiver = async {
        let (mut stream, _peer) = listener.accept().await.unwrap();

        receive_sender_handshake(&mut stream).await;

        let offer = receive_offer(&mut stream).await;

        assert_eq!(offer.filename, "permanent-reject.bin");
        assert_eq!(offer.file_size, original_data.len() as u64);

        let rejection = FileReject {
            code: RejectCode::FileExists,
            message: "destination already exists".to_string(),
        };

        let reject_frame = Frame::new(MessageType::Reject, encode_reject(&rejection).unwrap());

        write_frame(&mut stream, &reject_frame).await.unwrap();

        drop(stream);

        let second_connection = timeout(NO_EXTRA_CONNECTION_WINDOW, listener.accept()).await;

        assert!(
            second_connection.is_err(),
            "sender retried after a permanent receiver REJECT"
        );
    };

    let sender = run_sender(&source_path_string, &address);

    let (_, sender_result) = timeout(Duration::from_secs(4), async {
        tokio::join!(fake_receiver, sender)
    })
    .await
    .expect("permanent rejection test timed out");

    assert!(
        sender_result.is_err(),
        "sender unexpectedly succeeded after receiver REJECT"
    );
}

async fn receive_sender_handshake(stream: &mut TcpStream) {
    let hello = read_frame(stream).await.unwrap();

    assert_eq!(hello.message_type, MessageType::Hello);
    assert_eq!(hello.payload, vec![WFP_VERSION]);

    let hello_ack = Frame::new(MessageType::HelloAck, vec![WFP_VERSION]);

    write_frame(stream, &hello_ack).await.unwrap();
}

async fn receive_offer(stream: &mut TcpStream) -> warpfile::protocol::FileOffer {
    let offer_frame = read_frame(stream).await.unwrap();

    assert_eq!(offer_frame.message_type, MessageType::Offer);

    decode_offer(&offer_frame.payload).unwrap()
}
