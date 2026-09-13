use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use std::process::Command;

use serde_json::Value;

pub fn online_peer_ipv4_addresses() -> Vec<Ipv4Addr> {
    let output = match Command::new("tailscale")
        .args(["status", "--json"])
        .output()
    {
        Ok(output) => output,

        Err(_) => {
            return Vec::new();
        }
    };

    if !output.status.success() {
        return Vec::new();
    }

    parse_tailscale_status(&output.stdout)
}

fn parse_tailscale_status(json: &[u8]) -> Vec<Ipv4Addr> {
    let root: Value = match serde_json::from_slice(json) {
        Ok(root) => root,

        Err(_) => {
            return Vec::new();
        }
    };

    let Some(peers) = root.get("Peer").and_then(Value::as_object) else {
        return Vec::new();
    };

    let mut addresses = HashSet::new();

    for peer in peers.values() {
        let online = peer.get("Online").and_then(Value::as_bool).unwrap_or(false);

        if !online {
            continue;
        }

        let Some(tailscale_ips) = peer.get("TailscaleIPs").and_then(Value::as_array) else {
            continue;
        };

        for value in tailscale_ips {
            let Some(address) = value.as_str() else {
                continue;
            };

            let Ok(ip_address) = address.parse::<IpAddr>() else {
                continue;
            };

            if let IpAddr::V4(ipv4) = ip_address {
                addresses.insert(ipv4);
            }
        }
    }

    let mut addresses: Vec<Ipv4Addr> = addresses.into_iter().collect();

    addresses.sort_by_key(|address| u32::from(*address));

    addresses
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_online_ipv4_peers() {
        let json = br#"
        {
            "Peer": {
                "peer-a": {
                    "HostName": "Edbook",
                    "Online": true,
                    "TailscaleIPs": [
                        "100.68.8.15",
                        "fd7a:115c:a1e0::652f:810"
                    ]
                },

                "peer-b": {
                    "HostName": "motog7-debian",
                    "Online": true,
                    "TailscaleIPs": [
                        "100.95.151.44",
                        "fd7a:115c:a1e0::a52f:972d"
                    ]
                },

                "peer-c": {
                    "HostName": "old-edbook",
                    "Online": false,
                    "TailscaleIPs": [
                        "100.109.218.127"
                    ]
                }
            }
        }
        "#;

        let addresses = parse_tailscale_status(json);

        assert_eq!(
            addresses,
            vec![
                Ipv4Addr::new(100, 68, 8, 15,),
                Ipv4Addr::new(100, 95, 151, 44,),
            ]
        );
    }

    #[test]
    fn deduplicates_peer_addresses() {
        let json = br#"
        {
            "Peer": {
                "peer-a": {
                    "Online": true,
                    "TailscaleIPs": [
                        "100.68.8.15"
                    ]
                },

                "peer-b": {
                    "Online": true,
                    "TailscaleIPs": [
                        "100.68.8.15"
                    ]
                }
            }
        }
        "#;

        let addresses = parse_tailscale_status(json);

        assert_eq!(addresses, vec![Ipv4Addr::new(100, 68, 8, 15,)]);
    }

    #[test]
    fn invalid_json_returns_no_peers() {
        let addresses = parse_tailscale_status(b"this is not json");

        assert!(addresses.is_empty());
    }
}
