use std::fs;
use std::time::Duration;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{
    Frame, MessageType, ResumeRequest, decode_offer, encode_resume, read_frame, write_frame,
};
use warpfile::sender::run_sender;

#[tokio::test]
async fn sender_reconnects_and_resumes_after_connection_loss() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("automatic-reconnect.bin");

    let original_data: Vec<u8> = (0..600_000)
        .map(|index| ((index * 17) % 251) as u8)
        .collect();

    fs::write(&source_path, &original_data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let fake_receiver = async {
        let (mut first_stream, _peer) = listener.accept().await.unwrap();

        receive_sender_handshake(&mut first_stream).await;

        let first_offer = receive_offer(&mut first_stream).await;

        assert_eq!(first_offer.filename, "automatic-reconnect.bin");
        assert_eq!(first_offer.file_size, original_data.len() as u64);

        let first_transfer_id = first_offer.transfer_id;

        let accept = Frame::new(MessageType::Accept, Vec::new());

        write_frame(&mut first_stream, &accept).await.unwrap();

        let retained = receive_partial_prefix(&mut first_stream, 100_000).await;

        assert!(
            retained.len() >= 100_000,
            "receiver did not retain enough data before simulated connection loss"
        );

        assert_eq!(retained, original_data[..retained.len()]);

        let retained_length = retained.len();

        drop(first_stream);

        let (mut second_stream, _peer) = listener.accept().await.unwrap();

        receive_sender_handshake(&mut second_stream).await;

        let second_offer = receive_offer(&mut second_stream).await;

        assert_eq!(second_offer.filename, "automatic-reconnect.bin");
        assert_eq!(second_offer.file_size, original_data.len() as u64);

        assert_eq!(
            second_offer.transfer_id, first_transfer_id,
            "sender generated a different transfer identity after reconnect"
        );

        let prefix_hash = blake3::hash(&retained);

        let resume_request = ResumeRequest {
            offset: retained_length as u64,
            prefix_hash: *prefix_hash.as_bytes(),
        };

        let resume = Frame::new(MessageType::Resume, encode_resume(&resume_request));

        write_frame(&mut second_stream, &resume).await.unwrap();

        let resume_accept = read_frame(&mut second_stream).await.unwrap();

        assert_eq!(resume_accept.message_type, MessageType::Accept);
        assert!(resume_accept.payload.is_empty());

        let (suffix, complete_hash) = receive_transfer_frames(&mut second_stream).await;

        assert_eq!(suffix, original_data[retained_length..]);

        let mut reconstructed = retained;

        reconstructed.extend_from_slice(&suffix);

        assert_eq!(reconstructed, original_data);

        let expected_hash = blake3::hash(&original_data);

        assert_eq!(complete_hash, expected_hash.as_bytes().to_vec());

        send_verified(&mut second_stream).await;
    };

    let sender = run_sender(&source_path_string, &address);

    let (_, sender_result) = timeout(Duration::from_secs(10), async {
        tokio::join!(fake_receiver, sender)
    })
    .await
    .expect("automatic reconnect test timed out");

    assert!(
        sender_result.is_ok(),
        "sender failed automatic reconnect flow: {:?}",
        sender_result.err()
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

async fn receive_partial_prefix(stream: &mut TcpStream, minimum_length: usize) -> Vec<u8> {
    let mut retained = Vec::new();

    while retained.len() < minimum_length {
        let frame = read_frame(stream).await.unwrap();

        match frame.message_type {
            MessageType::Data => {
                retained.extend_from_slice(&frame.payload);
            }

            MessageType::Complete => {
                panic!("sender completed before simulated connection loss");
            }

            other => {
                panic!("unexpected message before simulated connection loss: {other:?}");
            }
        }
    }

    retained
}

async fn receive_transfer_frames(stream: &mut TcpStream) -> (Vec<u8>, Vec<u8>) {
    let mut transferred = Vec::new();

    loop {
        let frame = read_frame(stream).await.unwrap();

        match frame.message_type {
            MessageType::Data => {
                transferred.extend_from_slice(&frame.payload);
            }

            MessageType::Complete => {
                assert_eq!(frame.payload.len(), 32);

                return (transferred, frame.payload);
            }

            other => {
                panic!("unexpected transfer message: {other:?}");
            }
        }
    }
}

async fn send_verified(stream: &mut TcpStream) {
    let verified = Frame::new(MessageType::Verified, Vec::new());

    write_frame(stream, &verified).await.unwrap();
}
