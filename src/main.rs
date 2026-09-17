use std::env;
use std::error::Error;
use std::path::Path;

use warpfile::destination::resolve_destination;
use warpfile::discovery::discover_devices;
use warpfile::receiver::run_receiver;
use warpfile::receiver_v03::run_receiver_v03;
use warpfile::sender::run_sender;
use warpfile::sender_v03::run_sender_v03;

const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:42069";
const DEFAULT_RECEIVE_DIRECTORY: &str = "received";
const VERSION_FLAG: &str = "--wfp-version";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WfpVersionChoice {
    V02,
    V03,
}

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Receive(WfpVersionChoice),
    Discover,
    Send {
        file: String,
        destination: String,
        version: WfpVersionChoice,
    },
}

fn parse_wfp_version(value: &str) -> Result<WfpVersionChoice, String> {
    match value {
        "0.2" => Ok(WfpVersionChoice::V02),
        "0.3" => Ok(WfpVersionChoice::V03),
        other => Err(format!(
            "unsupported WFP version \"{other}\"; expected \"0.2\" or \"0.3\""
        )),
    }
}

fn has_version_flag(arguments: &[String]) -> bool {
    arguments
        .iter()
        .skip(2)
        .any(|argument| argument.as_str() == VERSION_FLAG || argument.starts_with("--wfp-version="))
}

fn version_flag_value(arguments: &[String]) -> Result<WfpVersionChoice, String> {
    let mut remaining = arguments.iter().skip(2);

    while let Some(argument) = remaining.next() {
        if argument.as_str() == VERSION_FLAG {
            let value = remaining
                .next()
                .ok_or_else(|| "missing value for --wfp-version".to_string())?;

            return parse_wfp_version(value);
        }

        if let Some(value) = argument.strip_prefix("--wfp-version=") {
            return parse_wfp_version(value);
        }
    }

    Ok(WfpVersionChoice::V02)
}

fn parse_arguments(arguments: &[String]) -> Result<Command, String> {
    let Some(command) = arguments.get(1) else {
        return Err("missing command".to_string());
    };

    let positional: Vec<&str> = arguments[2..]
        .iter()
        .filter(|argument| !argument.starts_with("--"))
        .map(|argument| argument.as_str())
        .collect();

    match command.as_str() {
        "receive" => {
            if !positional.is_empty() {
                return Err("receive does not accept positional arguments".to_string());
            }

            Ok(Command::Receive(version_flag_value(arguments)?))
        }

        "discover" => {
            if has_version_flag(arguments) {
                return Err("discover does not support --wfp-version".to_string());
            }

            if !positional.is_empty() {
                return Err("discover does not accept positional arguments".to_string());
            }

            Ok(Command::Discover)
        }

        "send" => {
            if positional.len() != 2 {
                return Err("send requires <file> and <destination>".to_string());
            }

            Ok(Command::Send {
                file: positional[0].to_string(),
                destination: positional[1].to_string(),
                version: version_flag_value(arguments)?,
            })
        }

        other => Err(format!("unknown command \"{other}\"")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments: Vec<String> = env::args().collect();

    let command = match parse_arguments(&arguments) {
        Ok(command) => command,

        Err(error) => {
            eprintln!("Error: {error}");

            print_usage();

            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, error).into());
        }
    };

    match command {
        Command::Receive(version) => {
            run_receive(version).await?;
        }

        Command::Discover => {
            run_discover().await?;
        }

        Command::Send {
            file,
            destination,
            version,
        } => {
            let resolved = resolve_destination(&destination).await?;

            if resolved != destination {
                println!("Resolved {destination} -> {resolved}");
            }

            match version {
                WfpVersionChoice::V02 => run_sender(&file, &resolved).await?,
                WfpVersionChoice::V03 => run_sender_v03(Path::new(&file), &resolved).await?,
            }
        }
    }

    Ok(())
}

async fn run_receive(version: WfpVersionChoice) -> Result<(), Box<dyn Error>> {
    match version {
        WfpVersionChoice::V02 => run_receiver(DEFAULT_LISTEN_ADDRESS).await,
        WfpVersionChoice::V03 => {
            run_receiver_v03(DEFAULT_LISTEN_ADDRESS, Path::new(DEFAULT_RECEIVE_DIRECTORY)).await
        }
    }
}

async fn run_discover() -> Result<(), Box<dyn Error>> {
    println!("WarpFile Discovery");

    println!("Searching for devices...");

    let devices = discover_devices().await?;

    println!();

    if devices.is_empty() {
        println!("No WarpFile devices found.");

        return Ok(());
    }

    println!("WarpFile devices found:");

    println!();

    for (index, device) in devices.iter().enumerate() {
        println!("{}. {}", index + 1, device.device_name);

        println!("   {}", device.address);

        println!();
    }

    Ok(())
}

fn print_usage() {
    println!("WarpFile");

    println!();

    println!("Usage:");

    println!("  warpfile receive [--wfp-version <0.2|0.3>]");

    println!("  warpfile discover");

    println!("  warpfile send <file> <address-or-device> [--wfp-version <0.2|0.3>]");

    println!();

    println!("Examples:");

    println!("  warpfile discover");

    println!("  warpfile send .\\teste.txt EDBOOK");

    println!("  warpfile send .\\teste.txt 127.0.0.1:42069");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(arguments: &[&str]) -> Vec<String> {
        std::iter::once("warpfile")
            .chain(arguments.iter().copied())
            .map(String::from)
            .collect()
    }

    #[test]
    fn parses_wfp_version_02_default() {
        assert_eq!(
            parse_arguments(&args(&["receive"])).unwrap(),
            Command::Receive(WfpVersionChoice::V02)
        );

        assert_eq!(
            parse_arguments(&args(&["send", "file.bin", "127.0.0.1:42069"])).unwrap(),
            Command::Send {
                file: "file.bin".to_string(),
                destination: "127.0.0.1:42069".to_string(),
                version: WfpVersionChoice::V02,
            }
        );
    }

    #[test]
    fn parses_wfp_version_03() {
        assert_eq!(
            parse_arguments(&args(&["receive", "--wfp-version=0.3"])).unwrap(),
            Command::Receive(WfpVersionChoice::V03)
        );

        assert_eq!(
            parse_arguments(&args(&[
                "send",
                "--wfp-version=0.3",
                "file.bin",
                "127.0.0.1:42069"
            ]))
            .unwrap(),
            Command::Send {
                file: "file.bin".to_string(),
                destination: "127.0.0.1:42069".to_string(),
                version: WfpVersionChoice::V03,
            }
        );
    }

    #[test]
    fn rejects_unknown_wfp_version() {
        let error = parse_arguments(&args(&["receive", "--wfp-version=1.0"])).unwrap_err();

        assert!(error.contains("unsupported WFP version"));
    }

    #[test]
    fn rejects_wfp_version_on_discover() {
        let error = parse_arguments(&args(&["discover", "--wfp-version=0.3"])).unwrap_err();

        assert!(error.contains("discover does not support --wfp-version"));
    }
}
