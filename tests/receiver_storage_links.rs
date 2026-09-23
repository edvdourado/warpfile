use std::fs;
use std::path::Path;

use tempfile::tempdir;
use warpfile::chunk::ChunkLayout;
use warpfile::chunk_state::{ChunkState, chunk_state_path, read_chunk_state, write_chunk_state};
use warpfile::completion_receipt::{
    CompletionReceipt, completion_receipt_path, read_completion_receipt, write_completion_receipt,
};
use warpfile::protocol::frame::WFP_VERSION;
use warpfile::protocol::{
    FileOffer, Frame, MessageType, TransferId, encode_offer, read_frame, write_frame,
};
use warpfile::receiver::receive_once;
use warpfile::receiver_paths::{internal_directory, partial_path, partials_directory};
use warpfile::receiver_v03::prepare_v03_partial_file;
use warpfile::transfer_metadata::{
    TransferMetadata, TransferState, read_transfer_metadata, transfer_metadata_path,
    write_transfer_metadata,
};

#[cfg(unix)]
fn link_file(target: &Path, link: &Path) -> bool {
    std::os::unix::fs::symlink(target, link).unwrap();
    true
}

#[cfg(windows)]
fn link_file(target: &Path, link: &Path) -> bool {
    match std::os::windows::fs::symlink_file(target, link) {
        Ok(()) => true,
        Err(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(1314) =>
        {
            false
        }
        Err(error) => panic!("could not create test symlink: {error}"),
    }
}

#[cfg(unix)]
fn link_dir(target: &Path, link: &Path) -> bool {
    std::os::unix::fs::symlink(target, link).unwrap();
    true
}

#[cfg(windows)]
fn link_dir(target: &Path, link: &Path) -> bool {
    match std::os::windows::fs::symlink_dir(target, link) {
        Ok(()) => true,
        Err(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(1314) =>
        {
            false
        }
        Err(error) => panic!("could not create test directory symlink: {error}"),
    }
}

fn id() -> TransferId {
    TransferId::from_bytes([7; 16])
}

async fn offer_fails(destination: &Path) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let receiver = receive_once(listener, destination);
    let sender = async {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        write_frame(
            &mut stream,
            &Frame::new(MessageType::Hello, vec![WFP_VERSION]),
        )
        .await
        .unwrap();
        assert_eq!(
            read_frame(&mut stream).await.unwrap().message_type,
            MessageType::HelloAck
        );
        let offer = FileOffer {
            transfer_id: id(),
            filename: "x".into(),
            file_size: 4,
        };
        write_frame(
            &mut stream,
            &Frame::new(MessageType::Offer, encode_offer(&offer).unwrap()),
        )
        .await
        .unwrap();
    };
    let (result, ()) = tokio::join!(receiver, sender);
    assert!(result.is_err(), "hostile storage must fail the session");
}

#[tokio::test]
async fn partial_link_does_not_modify_external_file() {
    let temp = tempdir().unwrap();
    let dest = temp.path().join("received");
    fs::create_dir_all(partials_directory(&dest)).unwrap();
    let sentinel = temp.path().join("sentinel");
    fs::write(&sentinel, b"keep").unwrap();
    if !link_file(&sentinel, &partial_path(&dest, "x")) {
        return;
    }

    offer_fails(&dest).await;
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
    assert!(
        prepare_v03_partial_file(&partial_path(&dest, "x"), 100)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
}

#[tokio::test]
async fn final_destination_link_is_rejected_without_touching_target() {
    let temp = tempdir().unwrap();
    let dest = temp.path().join("received");
    fs::create_dir(&dest).unwrap();
    let sentinel = temp.path().join("sentinel");
    fs::write(&sentinel, b"keep").unwrap();
    if !link_file(&sentinel, &dest.join("x")) {
        return;
    }

    offer_fails(&dest).await;
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
}

#[tokio::test]
async fn partial_directory_link_does_not_redirect_receiver() {
    let temp = tempdir().unwrap();
    let dest = temp.path().join("received");
    let outside = temp.path().join("outside");
    fs::create_dir_all(internal_directory(&dest)).unwrap();
    fs::create_dir(&outside).unwrap();
    if !link_dir(&outside, &partials_directory(&dest)) {
        return;
    }

    offer_fails(&dest).await;
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
}

#[tokio::test]
async fn internal_root_link_does_not_redirect_receiver() {
    let temp = tempdir().unwrap();
    let dest = temp.path().join("received");
    let outside = temp.path().join("outside");
    fs::create_dir(&dest).unwrap();
    fs::create_dir(&outside).unwrap();
    if !link_dir(&outside, &internal_directory(&dest)) {
        return;
    }

    offer_fails(&dest).await;
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
}

#[tokio::test]
async fn metadata_and_temporary_links_leave_sentinel_intact() {
    let temp = tempdir().unwrap();
    let partial = temp.path().join("x.part");
    let sentinel = temp.path().join("sentinel");
    fs::write(&sentinel, b"keep").unwrap();
    let metadata = TransferMetadata {
        transfer_id: id(),
        filename: "x".into(),
        file_size: 4,
        state: TransferState::Partial,
    };
    let sidecar = transfer_metadata_path(&partial);
    if !link_file(&sentinel, &sidecar) {
        return;
    }
    assert!(read_transfer_metadata(&partial).await.is_err());
    assert!(write_transfer_metadata(&partial, &metadata).await.is_err());
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
    fs::remove_file(&sidecar).unwrap();
    let temporary = sidecar.with_file_name("x.part.warpmeta.tmp");
    if !link_file(&sentinel, &temporary) {
        return;
    }
    assert!(write_transfer_metadata(&partial, &metadata).await.is_err());
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
}

#[tokio::test]
async fn chunk_state_and_temporary_links_leave_sentinel_intact() {
    let temp = tempdir().unwrap();
    let partial = temp.path().join("x.part");
    let sentinel = temp.path().join("sentinel");
    fs::write(&sentinel, b"keep").unwrap();
    let state = ChunkState::new(ChunkLayout::new(4, 4).unwrap(), vec![]).unwrap();
    let sidecar = chunk_state_path(&partial);
    if !link_file(&sentinel, &sidecar) {
        return;
    }
    assert!(read_chunk_state(&partial).await.is_err());
    assert!(write_chunk_state(&partial, &state).await.is_err());
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
    fs::remove_file(&sidecar).unwrap();
    let temporary = sidecar.with_file_name("x.part.warpchunks.tmp");
    if !link_file(&sentinel, &temporary) {
        return;
    }
    assert!(write_chunk_state(&partial, &state).await.is_err());
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
}

#[tokio::test]
async fn receipt_directory_and_path_links_leave_sentinel_intact() {
    let temp = tempdir().unwrap();
    let dest = temp.path().join("received");
    let outside = temp.path().join("outside");
    let sentinel = temp.path().join("sentinel");
    fs::create_dir_all(internal_directory(&dest)).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(&sentinel, b"keep").unwrap();
    let receipt = CompletionReceipt {
        transfer_id: id(),
        filename: "x".into(),
        file_size: 4,
        blake3: [0; 32],
    };
    let receipt_dir = internal_directory(&dest).join("receipts");
    if !link_dir(&outside, &receipt_dir) {
        return;
    }
    assert!(write_completion_receipt(&dest, &receipt).await.is_err());
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    #[cfg(unix)]
    fs::remove_file(&receipt_dir).unwrap();
    #[cfg(windows)]
    fs::remove_dir(&receipt_dir).unwrap();
    fs::create_dir(&receipt_dir).unwrap();
    let path = completion_receipt_path(&dest, id());
    if !link_file(&sentinel, &path) {
        return;
    }
    assert!(read_completion_receipt(&dest, id()).await.is_err());
    assert!(write_completion_receipt(&dest, &receipt).await.is_err());
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
    fs::remove_file(&path).unwrap();
    let temporary = path.with_extension("json.tmp");
    if !link_file(&sentinel, &temporary) {
        return;
    }
    assert!(write_completion_receipt(&dest, &receipt).await.is_err());
    assert_eq!(fs::read(&sentinel).unwrap(), b"keep");
}

#[cfg(windows)]
fn junction(target: &Path, link: &Path) {
    let output = std::process::Command::new("cmd")
        .arg("/C")
        .arg("mklink")
        .arg("/J")
        .arg(link)
        .arg(target)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "mklink /J failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(windows)]
#[tokio::test]
async fn junction_at_internal_root_is_rejected() {
    let temp = tempdir().unwrap();
    let dest = temp.path().join("received");
    let outside = temp.path().join("outside");
    fs::create_dir(&dest).unwrap();
    fs::create_dir(&outside).unwrap();
    junction(&outside, &internal_directory(&dest));

    offer_fails(&dest).await;
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
}

#[cfg(windows)]
#[tokio::test]
async fn junctions_at_partials_and_receipts_are_rejected() {
    let temp = tempdir().unwrap();
    let dest = temp.path().join("received");
    let outside = temp.path().join("outside");
    fs::create_dir_all(internal_directory(&dest)).unwrap();
    fs::create_dir(&outside).unwrap();
    junction(&outside, &partials_directory(&dest));
    offer_fails(&dest).await;
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    fs::remove_dir(partials_directory(&dest)).unwrap();
    let receipt_dir = internal_directory(&dest).join("receipts");
    junction(&outside, &receipt_dir);
    let receipt = CompletionReceipt {
        transfer_id: id(),
        filename: "x".into(),
        file_size: 4,
        blake3: [0; 32],
    };
    assert!(write_completion_receipt(&dest, &receipt).await.is_err());
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
}
