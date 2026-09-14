use std::fs;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};

use warpfile::protocol::frame::{MAX_DATA_PAYLOAD_LENGTH, WFP_VERSION};
use warpfile::protocol::{
    FileOffer, Frame, MessageType, TransferId, encode_offer, read_frame, write_frame,
};

use warpfile::receiver::receive_once;
use warpfile::sender::run_sender;

#[tokio::test]
async fn resumes_after_real_connection_loss() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&source_directory).unwrap();

    let source_path = source_directory.join("recovery.bin");

    let original_data: Vec<u8> = (0..400_000)
        .map(|index| ((index * 17) % 251) as u8)
        .collect();

    fs::write(&source_path, &original_data).unwrap();

    let interrupted_at = 123_457usize;

    /*
     * FIRST CONNECTION
     *
     * A controlled sender sends a real
     * prefix over TCP to the real receiver
     * and then disappears unexpectedly.
     */

    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let first_address = first_listener.local_addr().unwrap();

    let first_receiver = receive_once(first_listener, &destination_directory);

    let interrupted_sender = async {
        let mut stream = TcpStream::connect(first_address).await.unwrap();

        perform_handshake(&mut stream).await;

        send_offer_and_wait_for_accept(&mut stream, "recovery.bin", original_data.len() as u64)
            .await;

        for chunk in original_data[..interrupted_at].chunks(MAX_DATA_PAYLOAD_LENGTH) {
            let data = Frame::new(MessageType::Data, chunk.to_vec());

            write_frame(&mut stream, &data).await.unwrap();
        }

        /*
         * No CANCEL and no COMPLETE.
         *
         * Dropping the socket simulates
         * an unexpected connection loss.
         */
        drop(stream);
    };

    let (first_receiver_result, _) = tokio::join!(first_receiver, interrupted_sender);

    assert!(
        first_receiver_result.is_err(),
        "receiver should report the interrupted first transfer"
    );

    let partial_path = destination_directory.join("recovery.bin.part");

    assert!(
        partial_path.exists(),
        "receiver did not preserve the partial file after connection loss"
    );

    let preserved = fs::read(&partial_path).unwrap();

    assert_eq!(
        preserved,
        original_data[..interrupted_at],
        "preserved partial does not match the bytes sent before disconnection"
    );

    /*
     * SECOND CONNECTION
     *
     * Now both sides are the actual WarpFile
     * implementation.
     *
     * The receiver must discover the .part,
     * send RESUME, and run_sender must verify
     * it and continue from the correct offset.
     */

    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let second_address = second_listener.local_addr().unwrap().to_string();

    let source_path_string = source_path.to_string_lossy().into_owned();

    let second_receiver = receive_once(second_listener, &destination_directory);

    let real_sender = run_sender(&source_path_string, &second_address);

    let (second_receiver_result, sender_result) = tokio::join!(second_receiver, real_sender);

    assert!(
        second_receiver_result.is_ok(),
        "receiver failed resumed transfer: {:?}",
        second_receiver_result.err()
    );

    assert!(
        sender_result.is_ok(),
        "sender failed resumed transfer: {:?}",
        sender_result.err()
    );

    /*
     * The .part must now have become the
     * final file.
     */

    let final_path = destination_directory.join("recovery.bin");

    assert!(
        final_path.exists(),
        "final file was not created after resumed transfer"
    );

    let received = fs::read(&final_path).unwrap();

    assert_eq!(
        received, original_data,
        "resumed file differs from the original source"
    );

    assert!(
        !partial_path.exists(),
        "partial file remained after successful recovery"
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
        transfer_id: test_transfer_id(),
        filename: filename.to_string(),
        file_size,
    };

    let payload = encode_offer(&offer).unwrap();

    let frame = Frame::new(MessageType::Offer, payload);

    write_frame(stream, &frame).await.unwrap();

    let response = read_frame(stream).await.unwrap();

    assert_eq!(response.message_type, MessageType::Accept);

    assert!(response.payload.is_empty());
}

fn test_transfer_id() -> TransferId {
    TransferId::from_bytes([
        0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E,
        0x2F,
    ])
}
