use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use if_addrs::{IfAddr, get_if_addrs};
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout};

use crate::protocol::{
    DeviceAnnouncement, Frame, MessageType, decode_announcement, decode_frame, encode_announcement,
    encode_frame,
};
use crate::tailscale::online_peer_ipv4_addresses;

pub const DISCOVERY_PORT: u16 = 42070;

const MAX_UDP_DATAGRAM_SIZE: usize = 65_507;

const DISCOVERY_RESPONSE_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    pub device_name: String,
    pub address: SocketAddr,
}

pub fn local_device_name() -> String {
    env::var("COMPUTERNAME")
        .or_else(|_| env::var("HOSTNAME"))
        .ok()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "warpfile-device".to_string())
}

pub async fn respond_to_discovery_once(
    socket: &UdpSocket,
    device_name: &str,
    tcp_port: u16,
) -> Result<SocketAddr, Box<dyn Error>> {
    let mut buffer = vec![0u8; MAX_UDP_DATAGRAM_SIZE];

    let (bytes_received, peer_address) = socket.recv_from(&mut buffer).await?;

    let frame = decode_frame(&buffer[..bytes_received])?;

    validate_discover_frame(&frame)?;

    send_announcement(socket, peer_address, device_name, tcp_port).await?;

    Ok(peer_address)
}

pub async fn run_discovery_responder(
    socket: &UdpSocket,
    device_name: &str,
    tcp_port: u16,
) -> Result<(), Box<dyn Error>> {
    let encoded_announcement = build_announcement_frame(device_name, tcp_port)?;

    let mut buffer = vec![0u8; MAX_UDP_DATAGRAM_SIZE];

    loop {
        let (bytes_received, peer_address) = socket.recv_from(&mut buffer).await?;

        let Ok(frame) = decode_frame(&buffer[..bytes_received]) else {
            continue;
        };

        if frame.message_type != MessageType::Discover {
            continue;
        }

        if !frame.payload.is_empty() {
            continue;
        }

        socket.send_to(&encoded_announcement, peer_address).await?;
    }
}

pub async fn discover_devices() -> Result<Vec<DiscoveredDevice>, Box<dyn Error>> {
    let (local_addresses, broadcast_addresses) = discover_local_networks()?;

    let tailscale_addresses = online_peer_ipv4_addresses();

    let discovery_targets = build_discovery_targets(broadcast_addresses, tailscale_addresses);

    if discovery_targets.is_empty() {
        return Ok(Vec::new());
    }

    let socket = UdpSocket::bind("0.0.0.0:0").await?;

    socket.set_broadcast(true)?;

    let discover_frame = Frame::new(MessageType::Discover, Vec::new());

    let encoded_discover = encode_frame(&discover_frame)?;

    let mut sent_any = false;

    for target in discovery_targets {
        if socket.send_to(&encoded_discover, target).await.is_ok() {
            sent_any = true;
        }
    }

    if !sent_any {
        return Ok(Vec::new());
    }

    let deadline = Instant::now() + DISCOVERY_RESPONSE_WINDOW;

    let mut devices = Vec::new();

    let mut buffer = vec![0u8; MAX_UDP_DATAGRAM_SIZE];

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            break;
        }

        let receive_result = timeout(remaining, socket.recv_from(&mut buffer)).await;

        let (bytes_received, source_address) = match receive_result {
            Ok(Ok(result)) => result,

            Ok(Err(error)) if is_ignorable_udp_error(&error) => {
                continue;
            }

            Ok(Err(error)) => {
                return Err(error.into());
            }

            Err(_) => {
                break;
            }
        };

        if is_local_source(source_address.ip(), &local_addresses) {
            continue;
        }

        let Ok(frame) = decode_frame(&buffer[..bytes_received]) else {
            continue;
        };

        if frame.message_type != MessageType::Announce {
            continue;
        }

        let Ok(announcement) = decode_announcement(&frame.payload) else {
            continue;
        };

        let tcp_address = SocketAddr::new(source_address.ip(), announcement.tcp_port);

        let device = DiscoveredDevice {
            device_name: announcement.device_name,

            address: tcp_address,
        };

        let already_known = devices.iter().any(|known: &DiscoveredDevice| {
            known.address == device.address && known.device_name == device.device_name
        });

        if !already_known {
            devices.push(device);
        }
    }

    devices.sort_by(|left, right| {
        left.device_name
            .cmp(&right.device_name)
            .then_with(|| left.address.to_string().cmp(&right.address.to_string()))
    });

    Ok(devices)
}

fn build_discovery_targets(
    broadcast_addresses: Vec<Ipv4Addr>,
    tailscale_addresses: Vec<Ipv4Addr>,
) -> Vec<SocketAddr> {
    let mut targets = HashSet::new();

    for address in broadcast_addresses {
        targets.insert(SocketAddr::from((address, DISCOVERY_PORT)));
    }

    for address in tailscale_addresses {
        targets.insert(SocketAddr::from((address, DISCOVERY_PORT)));
    }

    let mut targets: Vec<SocketAddr> = targets.into_iter().collect();

    targets.sort_by_key(|left| left.to_string());

    targets
}

fn discover_local_networks() -> Result<(HashSet<Ipv4Addr>, Vec<Ipv4Addr>), Box<dyn Error>> {
    let interfaces = get_if_addrs()?;

    let mut local_addresses = HashSet::new();

    let mut broadcast_addresses = HashSet::new();

    for interface in interfaces {
        let IfAddr::V4(ipv4) = interface.addr else {
            continue;
        };

        let ip = ipv4.ip;

        local_addresses.insert(ip);

        if ip.is_loopback() || ip.is_unspecified() || ip.is_link_local() {
            continue;
        }

        let Some(broadcast) = calculate_broadcast(ip, ipv4.netmask) else {
            continue;
        };

        broadcast_addresses.insert(broadcast);
    }

    let mut broadcast_addresses: Vec<Ipv4Addr> = broadcast_addresses.into_iter().collect();

    broadcast_addresses.sort_by_key(|address| u32::from(*address));

    Ok((local_addresses, broadcast_addresses))
}

fn calculate_broadcast(ip: Ipv4Addr, netmask: Ipv4Addr) -> Option<Ipv4Addr> {
    let ip_bits = u32::from(ip);

    let mask_bits = u32::from(netmask);

    let broadcast_bits = ip_bits | !mask_bits;

    let broadcast = Ipv4Addr::from(broadcast_bits);

    if broadcast == ip {
        return None;
    }

    Some(broadcast)
}

fn is_local_source(source_ip: IpAddr, local_addresses: &HashSet<Ipv4Addr>) -> bool {
    match source_ip {
        IpAddr::V4(ip) => local_addresses.contains(&ip),

        IpAddr::V6(_) => false,
    }
}

fn is_ignorable_udp_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused
    )
}

fn validate_discover_frame(frame: &Frame) -> Result<(), Box<dyn Error>> {
    if frame.message_type != MessageType::Discover {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected DISCOVER").into());
    }

    if !frame.payload.is_empty() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "DISCOVER payload must be empty").into(),
        );
    }

    Ok(())
}

async fn send_announcement(
    socket: &UdpSocket,
    peer_address: SocketAddr,
    device_name: &str,
    tcp_port: u16,
) -> Result<(), Box<dyn Error>> {
    let encoded = build_announcement_frame(device_name, tcp_port)?;

    socket.send_to(&encoded, peer_address).await?;

    Ok(())
}

fn build_announcement_frame(device_name: &str, tcp_port: u16) -> Result<Vec<u8>, Box<dyn Error>> {
    let announcement = DeviceAnnouncement {
        device_name: device_name.to_string(),

        tcp_port,
    };

    let announcement_payload = encode_announcement(&announcement)?;

    let announce_frame = Frame::new(MessageType::Announce, announcement_payload);

    Ok(encode_frame(&announce_frame)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_slash_24_broadcast() {
        let broadcast = calculate_broadcast(
            Ipv4Addr::new(192, 168, 1, 10),
            Ipv4Addr::new(255, 255, 255, 0),
        );

        assert_eq!(broadcast, Some(Ipv4Addr::new(192, 168, 1, 255,)));
    }

    #[test]
    fn calculates_slash_20_broadcast() {
        let broadcast = calculate_broadcast(
            Ipv4Addr::new(172, 20, 144, 1),
            Ipv4Addr::new(255, 255, 240, 0),
        );

        assert_eq!(broadcast, Some(Ipv4Addr::new(172, 20, 159, 255,)));
    }

    #[test]
    fn skips_slash_32_interface() {
        let broadcast = calculate_broadcast(
            Ipv4Addr::new(100, 73, 124, 26),
            Ipv4Addr::new(255, 255, 255, 255),
        );

        assert_eq!(broadcast, None);
    }

    #[test]
    fn detects_local_ipv4_source() {
        let mut local_addresses = HashSet::new();

        local_addresses.insert(Ipv4Addr::new(192, 168, 1, 10));

        assert!(is_local_source(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10,)),
            &local_addresses,
        ));

        assert!(!is_local_source(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20,)),
            &local_addresses,
        ));
    }

    #[test]
    fn ignores_udp_connection_reset() {
        let error = io::Error::new(io::ErrorKind::ConnectionReset, "peer has no UDP listener");

        assert!(is_ignorable_udp_error(&error,));
    }

    #[test]
    fn ignores_udp_connection_refused() {
        let error = io::Error::new(io::ErrorKind::ConnectionRefused, "peer refused UDP traffic");

        assert!(is_ignorable_udp_error(&error,));
    }

    #[test]
    fn does_not_ignore_other_udp_errors() {
        let error = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");

        assert!(!is_ignorable_udp_error(&error,));
    }
}
