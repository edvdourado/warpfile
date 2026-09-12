use std::error::Error;
use std::io;
use std::net::SocketAddr;

use tokio::net::UdpSocket;

use crate::protocol::{
    DeviceAnnouncement, Frame, MessageType, decode_frame, encode_announcement, encode_frame,
};

pub const DISCOVERY_PORT: u16 = 42070;

pub async fn respond_to_discovery_once(
    socket: &UdpSocket,
    device_name: &str,
    tcp_port: u16,
) -> Result<SocketAddr, Box<dyn Error>> {
    let mut buffer = vec![0u8; 65_507];

    let (bytes_received, peer_address) = socket.recv_from(&mut buffer).await?;

    let frame = decode_frame(&buffer[..bytes_received])?;

    if frame.message_type != MessageType::Discover {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected DISCOVER").into());
    }

    if !frame.payload.is_empty() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "DISCOVER payload must be empty").into(),
        );
    }

    let announcement = DeviceAnnouncement {
        device_name: device_name.to_string(),

        tcp_port,
    };

    let announcement_payload = encode_announcement(&announcement)?;

    let announce_frame = Frame::new(MessageType::Announce, announcement_payload);

    let encoded = encode_frame(&announce_frame)?;

    socket.send_to(&encoded, peer_address).await?;

    Ok(peer_address)
}
