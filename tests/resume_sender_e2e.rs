use std::fs;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};

use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{
    Frame, MessageType, ResumeRequest, decode_offer, encode_resume, read_frame, write_frame,
};

use warpfile::receiver::receive_once;
use warpfile::sender::run_sender;

#[tokio::test]
async fn sender_accepts_matching_resume_and_sends_only_suffix() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("matching.bin");

    let original_data: Vec<u8> = (0..200_000).map(|index| (index % 251) as u8).collect();

    fs::write(&source_path, &original_data).unwrap();

    let prefix_length = 70_123usize;

    let prefix_hash = blake3::hash(&original_data[..prefix_length]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let address_string = address.to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let fake_receiver = async {
        let (mut stream, _peer) = listener.accept().await.unwrap();

        receive_sender_handshake(&mut stream).await;

        let offer = receive_offer(&mut stream).await;

        assert_eq!(offer.filename, "matching.bin");

        assert_eq!(offer.file_size, original_data.len() as u64);

        let request = ResumeRequest {
            offset: prefix_length as u64,

            prefix_hash: *prefix_hash.as_bytes(),
        };

        let resume = Frame::new(MessageType::Resume, encode_resume(&request));

        write_frame(&mut stream, &resume).await.unwrap();

        let response = read_frame(&mut stream).await.unwrap();

        assert_eq!(response.message_type, MessageType::Accept);

        assert!(response.payload.is_empty());

        let (transferred, complete_hash) = receive_transfer_frames(&mut stream).await;

        assert_eq!(transferred, original_data[prefix_length..]);

        assert_eq!(
            complete_hash,
            blake3::hash(&original_data,).as_bytes().to_vec()
        );

        send_verified(&mut stream).await;
    };

    let sender = run_sender(&source_path_string, &address_string);

    let (_, sender_result) = tokio::join!(fake_receiver, sender);

    assert!(
        sender_result.is_ok(),
        "sender failed matching resume: {:?}",
        sender_result.err()
    );
}

#[tokio::test]
async fn sender_restarts_when_resume_prefix_does_not_match() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("restart.bin");

    let original_data: Vec<u8> = (0..180_000)
        .map(|index| ((index * 7) % 251) as u8)
        .collect();

    fs::write(&source_path, &original_data).unwrap();

    let prefix_length = 50_321usize;

    let stale_prefix = vec![0xAA; prefix_length];

    let stale_hash = blake3::hash(&stale_prefix);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let address_string = address.to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let fake_receiver = async {
        let (mut stream, _peer) = listener.accept().await.unwrap();

        receive_sender_handshake(&mut stream).await;

        let offer = receive_offer(&mut stream).await;

        assert_eq!(offer.filename, "restart.bin");

        assert_eq!(offer.file_size, original_data.len() as u64);

        let request = ResumeRequest {
            offset: prefix_length as u64,

            prefix_hash: *stale_hash.as_bytes(),
        };

        let resume = Frame::new(MessageType::Resume, encode_resume(&request));

        write_frame(&mut stream, &resume).await.unwrap();

        let restart = read_frame(&mut stream).await.unwrap();

        assert_eq!(restart.message_type, MessageType::Restart);

        assert!(restart.payload.is_empty());

        let accept = Frame::new(MessageType::Accept, Vec::new());

        write_frame(&mut stream, &accept).await.unwrap();

        let (transferred, complete_hash) = receive_transfer_frames(&mut stream).await;

        assert_eq!(transferred, original_data);

        assert_eq!(
            complete_hash,
            blake3::hash(&original_data,).as_bytes().to_vec()
        );

        send_verified(&mut stream).await;
    };

    let sender = run_sender(&source_path_string, &address_string);

    let (_, sender_result) = tokio::join!(fake_receiver, sender);

    assert!(
        sender_result.is_ok(),
        "sender failed restart flow: {:?}",
        sender_result.err()
    );
}

#[tokio::test]
async fn real_sender_and_receiver_resume_end_to_end() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&source_directory).unwrap();

    fs::create_dir_all(&destination_directory).unwrap();

    let source_path = source_directory.join("real-resume.bin");

    let original_data: Vec<u8> = (0..250_000)
        .map(|index| ((index * 13) % 251) as u8)
        .collect();

    fs::write(&source_path, &original_data).unwrap();

    let prefix_length = 83_777usize;

    let partial_path = destination_directory.join("real-resume.bin.part");

    fs::write(&partial_path, &original_data[..prefix_length]).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let receiver = receive_once(listener, &destination_directory);

    let sender = run_sender(&source_path_string, &address);

    let (receiver_result, sender_result) = tokio::join!(receiver, sender);

    assert!(
        receiver_result.is_ok(),
        "receiver failed real resume: {:?}",
        receiver_result.err()
    );

    assert!(
        sender_result.is_ok(),
        "sender failed real resume: {:?}",
        sender_result.err()
    );

    let final_path = destination_directory.join("real-resume.bin");

    let received = fs::read(final_path).unwrap();

    assert_eq!(received, original_data);

    assert!(
        !partial_path.exists(),
        "partial file remained after real resumed transfer"
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
