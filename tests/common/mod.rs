use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use warpfile::protocol::frame::WFP_VERSION_V03;
use warpfile::protocol::{
    Frame, MessageType, ProtocolIoError, read_frame_for_version, write_frame,
};

pub async fn proxy_v03_session(
    listener: &TcpListener,
    receiver_address: &str,
    drop_verified: bool,
) -> Vec<Frame> {
    let (sender, _) = listener.accept().await.unwrap();
    let receiver = TcpStream::connect(receiver_address).await.unwrap();
    let (mut sender_read, mut sender_write) = sender.into_split();
    let (mut receiver_read, mut receiver_write) = receiver.into_split();
    let sender_to_receiver = tokio::spawn(async move {
        let mut frames = Vec::new();
        loop {
            match read_frame_for_version(&mut sender_read, WFP_VERSION_V03).await {
                Ok(frame) => {
                    write_frame(&mut receiver_write, &frame).await.unwrap();
                    frames.push(frame);
                }
                Err(ProtocolIoError::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("sender-to-receiver proxy failed: {error}"),
            }
        }
        frames
    });

    loop {
        let frame = read_frame_for_version(&mut receiver_read, WFP_VERSION_V03)
            .await
            .unwrap();
        let verified = frame.message_type == MessageType::Verified;
        if !verified || !drop_verified {
            write_frame(&mut sender_write, &frame).await.unwrap();
        }
        if verified {
            break;
        }
    }
    drop(sender_write);
    tokio::time::timeout(Duration::from_secs(2), sender_to_receiver)
        .await
        .unwrap()
        .unwrap()
}
