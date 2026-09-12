use std::env;
use std::error::Error;

use warpfile::discovery::discover_devices;
use warpfile::receiver::run_receiver;
use warpfile::sender::run_sender;

const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:42069";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments: Vec<String> = env::args().collect();

    match arguments.as_slice() {
        [_, command] if command == "receive" => {
            run_receiver(DEFAULT_LISTEN_ADDRESS).await?;
        }

        [_, command] if command == "discover" => {
            run_discover().await?;
        }

        [_, command, file, address] if command == "send" => {
            run_sender(file, address).await?;
        }

        _ => {
            print_usage();
        }
    }

    Ok(())
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

    println!("  warpfile receive");

    println!("  warpfile discover");

    println!("  warpfile send <file> <address>");

    println!();

    println!("Examples:");

    println!("  warpfile discover");

    println!("  warpfile send .\\teste.txt 127.0.0.1:42069");
}
