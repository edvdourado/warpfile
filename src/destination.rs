use std::error::Error;
use std::io;
use std::net::SocketAddr;

use crate::discovery::{DiscoveredDevice, discover_devices};

pub async fn resolve_destination(input: &str) -> Result<String, Box<dyn Error>> {
    if let Ok(address) = input.parse::<SocketAddr>() {
        return Ok(address.to_string());
    }

    let devices = discover_devices().await?;

    let device = resolve_device_name(input, &devices)?;

    Ok(device.address.to_string())
}

fn resolve_device_name<'a>(
    name: &str,
    devices: &'a [DiscoveredDevice],
) -> Result<&'a DiscoveredDevice, io::Error> {
    let name = name.trim();

    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "device name cannot be empty",
        ));
    }

    let matches: Vec<&DiscoveredDevice> = devices
        .iter()
        .filter(|device| device.device_name.eq_ignore_ascii_case(name))
        .collect();

    match matches.as_slice() {
        [] => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no WarpFile device named '{name}' was found",),
        )),

        [device] => Ok(*device),

        multiple => {
            let mut addresses: Vec<String> = multiple
                .iter()
                .map(|device| device.address.to_string())
                .collect();

            addresses.sort();
            addresses.dedup();

            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "multiple WarpFile devices named '{name}' were found: {}. Use an explicit address",
                    addresses.join(", "),
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn accepts_direct_socket_address() {
        let resolved = resolve_destination("100.68.8.15:42069").await.unwrap();

        assert_eq!(resolved, "100.68.8.15:42069");
    }

    #[test]
    fn resolves_device_name_case_insensitively() {
        let devices = vec![DiscoveredDevice {
            device_name: "EDBOOK".to_string(),

            address: "100.68.8.15:42069".parse().unwrap(),
        }];

        let device = resolve_device_name("edbook", &devices).unwrap();

        assert_eq!(device.device_name, "EDBOOK");

        assert_eq!(device.address, "100.68.8.15:42069".parse().unwrap());
    }

    #[test]
    fn rejects_unknown_device_name() {
        let devices = vec![DiscoveredDevice {
            device_name: "EDBOOK".to_string(),

            address: "100.68.8.15:42069".parse().unwrap(),
        }];

        let error = resolve_device_name("UNKNOWN-PC", &devices).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);

        assert!(error.to_string().contains("UNKNOWN-PC"));
    }

    #[test]
    fn rejects_ambiguous_device_name() {
        let devices = vec![
            DiscoveredDevice {
                device_name: "EDBOOK".to_string(),

                address: "192.168.1.20:42069".parse().unwrap(),
            },
            DiscoveredDevice {
                device_name: "edbook".to_string(),

                address: "100.68.8.15:42069".parse().unwrap(),
            },
        ];

        let error = resolve_device_name("EdBook", &devices).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let message = error.to_string();

        assert!(message.contains("192.168.1.20:42069"));

        assert!(message.contains("100.68.8.15:42069"));
    }
}
