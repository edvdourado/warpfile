use std::error::Error;
use std::fmt;
use std::str;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceAnnouncement {
    pub device_name: String,
    pub tcp_port: u16,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DiscoveryError {
    DeviceNameEmpty,
    DeviceNameTooLong(usize),
    InvalidUtf8,
    InvalidPayloadLength,
    InvalidTcpPort,
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeviceNameEmpty => formatter.write_str("device name cannot be empty"),

            Self::DeviceNameTooLong(length) => {
                write!(formatter, "device name is too long: {length} bytes")
            }

            Self::InvalidUtf8 => formatter.write_str("device name is not valid UTF-8"),

            Self::InvalidPayloadLength => formatter.write_str("invalid ANNOUNCE payload length"),

            Self::InvalidTcpPort => formatter.write_str("ANNOUNCE TCP port cannot be zero"),
        }
    }
}

impl Error for DiscoveryError {}

pub fn encode_announcement(announcement: &DeviceAnnouncement) -> Result<Vec<u8>, DiscoveryError> {
    let name_bytes = announcement.device_name.as_bytes();

    if name_bytes.is_empty() {
        return Err(DiscoveryError::DeviceNameEmpty);
    }

    if name_bytes.len() > u16::MAX as usize {
        return Err(DiscoveryError::DeviceNameTooLong(name_bytes.len()));
    }

    if announcement.tcp_port == 0 {
        return Err(DiscoveryError::InvalidTcpPort);
    }

    let name_length = name_bytes.len() as u16;

    let mut payload = Vec::with_capacity(2 + name_bytes.len() + 2);

    payload.extend_from_slice(&name_length.to_be_bytes());

    payload.extend_from_slice(name_bytes);

    payload.extend_from_slice(&announcement.tcp_port.to_be_bytes());

    Ok(payload)
}

pub fn decode_announcement(payload: &[u8]) -> Result<DeviceAnnouncement, DiscoveryError> {
    if payload.len() < 4 {
        return Err(DiscoveryError::InvalidPayloadLength);
    }

    let name_length = u16::from_be_bytes([payload[0], payload[1]]) as usize;

    if name_length == 0 {
        return Err(DiscoveryError::DeviceNameEmpty);
    }

    let expected_length = 2 + name_length + 2;

    if payload.len() != expected_length {
        return Err(DiscoveryError::InvalidPayloadLength);
    }

    let name_start = 2;

    let name_end = name_start + name_length;

    let device_name = str::from_utf8(&payload[name_start..name_end])
        .map_err(|_| DiscoveryError::InvalidUtf8)?
        .to_string();

    let tcp_port = u16::from_be_bytes([payload[name_end], payload[name_end + 1]]);

    if tcp_port == 0 {
        return Err(DiscoveryError::InvalidTcpPort);
    }

    Ok(DeviceAnnouncement {
        device_name,
        tcp_port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_announcement_exactly() {
        let announcement = DeviceAnnouncement {
            device_name: "edbook".to_string(),

            tcp_port: 42069,
        };

        let encoded = encode_announcement(&announcement).unwrap();

        assert_eq!(
            encoded,
            vec![0x00, 0x06, b'e', b'd', b'b', b'o', b'o', b'k', 0xA4, 0x55,]
        );
    }

    #[test]
    fn announcement_round_trip() {
        let original = DeviceAnnouncement {
            device_name: "ED-DESKTOP".to_string(),

            tcp_port: 42069,
        };

        let encoded = encode_announcement(&original).unwrap();

        let decoded = decode_announcement(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn supports_utf8_device_name() {
        let original = DeviceAnnouncement {
            device_name: "Edvaldo-PC-ação".to_string(),

            tcp_port: 42069,
        };

        let encoded = encode_announcement(&original).unwrap();

        let decoded = decode_announcement(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_empty_device_name() {
        let announcement = DeviceAnnouncement {
            device_name: String::new(),

            tcp_port: 42069,
        };

        assert_eq!(
            encode_announcement(&announcement),
            Err(DiscoveryError::DeviceNameEmpty)
        );
    }

    #[test]
    fn rejects_zero_tcp_port() {
        let announcement = DeviceAnnouncement {
            device_name: "edbook".to_string(),

            tcp_port: 0,
        };

        assert_eq!(
            encode_announcement(&announcement),
            Err(DiscoveryError::InvalidTcpPort)
        );
    }

    #[test]
    fn rejects_invalid_payload_length() {
        let payload = [0x00, 0x06, b'e', b'd'];

        assert_eq!(
            decode_announcement(&payload),
            Err(DiscoveryError::InvalidPayloadLength)
        );
    }

    #[test]
    fn rejects_invalid_utf8_device_name() {
        let payload = [0x00, 0x01, 0xFF, 0xA4, 0x55];

        assert_eq!(
            decode_announcement(&payload),
            Err(DiscoveryError::InvalidUtf8)
        );
    }

    #[test]
    fn rejects_zero_tcp_port_when_decoding() {
        let payload = [0x00, 0x01, b'A', 0x00, 0x00];

        assert_eq!(
            decode_announcement(&payload),
            Err(DiscoveryError::InvalidTcpPort)
        );
    }
}
