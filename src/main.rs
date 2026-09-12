use std::env;
use std::error::Error;

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

        [_, command, address] if command == "send" => {
            run_sender(address).await?;
        }

        _ => {
            print_usage();
        }
    }

    Ok(())
}

fn print_usage() {
    println!("WarpFile");
    println!();
    println!("Usage:");
    println!("  warpfile receive");
    println!("  warpfile send <ADDRESS>");
    println!();
    println!("Example:");
    println!("  warpfile send 127.0.0.1:42069");
}
