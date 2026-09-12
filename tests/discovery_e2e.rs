use tokio::net::UdpSocket;

use warpfile::discovery::respond_to_discovery_once;
use warpfile::protocol::{Frame, MessageType, decode_announcement, decode_frame, encode_frame};

#[tokio::test]
async fn discovers_device_over_real_udp() {
    let responder_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let responder_address = responder_socket.local_addr().unwrap();

    let discovery_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let responder = respond_to_discovery_once(&responder_socket, "edbook", 42069);

    let discoverer = async {
        let discover_frame = Frame::new(MessageType::Discover, Vec::new());

        let encoded_discover = encode_frame(&discover_frame).unwrap();

        discovery_socket
            .send_to(&encoded_discover, responder_address)
            .await
            .unwrap();

        let mut buffer = vec![0u8; 65_507];

        let (bytes_received, source_address) =
            discovery_socket.recv_from(&mut buffer).await.unwrap();

        assert_eq!(source_address, responder_address);

        let announce_frame = decode_frame(&buffer[..bytes_received]).unwrap();

        assert_eq!(announce_frame.message_type, MessageType::Announce);

        let announcement = decode_announcement(&announce_frame.payload).unwrap();

        assert_eq!(announcement.device_name, "edbook");

        assert_eq!(announcement.tcp_port, 42069);
    };

    let (responder_result, _) = tokio::join!(responder, discoverer);

    assert!(
        responder_result.is_ok(),
        "discovery responder failed: {:?}",
        responder_result.err()
    );
}
