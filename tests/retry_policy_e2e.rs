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

#[tokio::test]
async fn sender_retries_and_reconciles_when_verified_confirmation_is_lost() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("lost-verified.bin");

    let original_data: Vec<u8> = (0..200_000)
        .map(|index| ((index * 29) % 251) as u8)
        .collect();

    fs::write(&source_path, &original_data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let fake_receiver = async {
        let (mut first_stream, _peer) = listener.accept().await.unwrap();

        receive_sender_handshake(&mut first_stream).await;

        let first_offer = receive_offer(&mut first_stream).await;

        assert_eq!(first_offer.filename, "lost-verified.bin");
        assert_eq!(first_offer.file_size, original_data.len() as u64);

        let transfer_id = first_offer.transfer_id;

        let accept = Frame::new(MessageType::Accept, Vec::new());

        write_frame(&mut first_stream, &accept).await.unwrap();

        let (received, complete_hash) = receive_until_complete(&mut first_stream).await;

        assert_eq!(received, original_data);

        let expected_hash = blake3::hash(&original_data);

        assert_eq!(complete_hash, expected_hash.as_bytes().to_vec());

        // COMPLETE reached the receiver, but VERIFIED is lost with the connection.
        drop(first_stream);

        let (mut second_stream, _peer) = listener.accept().await.unwrap();

        receive_sender_handshake(&mut second_stream).await;

        let second_offer = receive_offer(&mut second_stream).await;

        assert_eq!(second_offer.filename, "lost-verified.bin");
        assert_eq!(second_offer.file_size, original_data.len() as u64);
        assert_eq!(
            second_offer.transfer_id, transfer_id,
            "sender changed transfer identity between retry sessions"
        );

        // A reconciled VERIFIED response must not require reopening or reading the source file.
        fs::remove_file(&source_path).unwrap();

        let verified = Frame::new(MessageType::Verified, Vec::new());

        write_frame(&mut second_stream, &verified).await.unwrap();

        let next_frame = timeout(NO_EXTRA_CONNECTION_WINDOW, read_frame(&mut second_stream)).await;

        match next_frame {
            Ok(Ok(frame)) => {
                panic!(
                    "sender transmitted {:?} after direct VERIFIED response to OFFER",
                    frame.message_type
                );
            }
            Ok(Err(_)) => {}
            Err(_) => {
                panic!("sender kept the reconciled transfer session open after VERIFIED");
            }
        }

        let third_connection = timeout(NO_EXTRA_CONNECTION_WINDOW, listener.accept()).await;

        assert!(
            third_connection.is_err(),
            "sender attempted a third transfer session after reconciliation"
        );
    };

    let sender = run_sender(&source_path_string, &address);

    let (_, sender_result) = timeout(Duration::from_secs(6), async {
        tokio::join!(fake_receiver, sender)
    })
    .await
    .expect("lost VERIFIED reconciliation test timed out");

    assert!(
        sender_result.is_ok(),
        "sender failed to reconcile after lost VERIFIED: {sender_result:?}"
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

async fn receive_until_complete(stream: &mut TcpStream) -> (Vec<u8>, Vec<u8>) {
    let mut received = Vec::new();

    loop {
        let frame = read_frame(stream).await.unwrap();

        match frame.message_type {
            MessageType::Data => {
                received.extend_from_slice(&frame.payload);
            }

            MessageType::Complete => {
                assert_eq!(frame.payload.len(), 32);

                return (received, frame.payload);
            }

            other => {
                panic!("unexpected message while waiting for COMPLETE: {other:?}");
            }
        }
    }
}
