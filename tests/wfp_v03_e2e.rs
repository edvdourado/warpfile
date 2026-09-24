#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::time::Duration;

use tempfile::tempdir;
mod common;
use common::proxy_v03_session;
use tokio::net::{TcpListener, TcpStream};

use warpfile::chunk_state::decode_chunk_state;
use warpfile::completion_receipt::decode_completion_receipt;
use warpfile::protocol::frame::WFP_VERSION_V03;
use warpfile::protocol::{
    Frame, MessageType, ProtocolIoError, decode_chunk_start_v03, decode_data_v03, decode_offer_v03,
    read_frame_for_version, write_frame,
};
use warpfile::receiver_v03::run_receiver_v03;
use warpfile::sender_v03::run_sender_v03;

fn assert_promoted_partial(partial: &std::path::Path, final_path: &std::path::Path) {
    assert!(final_path.is_file());
    #[cfg(windows)]
    assert!(!partial.exists());
    #[cfg(target_os = "linux")]
    {
        let partial_metadata = std::fs::metadata(partial).unwrap();
        let final_metadata = std::fs::metadata(final_path).unwrap();
        assert_eq!(partial_metadata.dev(), final_metadata.dev());
        assert_eq!(partial_metadata.ino(), final_metadata.ino());
        assert_eq!(
            std::fs::read(partial).unwrap(),
            std::fs::read(final_path).unwrap()
        );
    }
}

async fn proxy_v03_drop_after_chunk_persisted(
    listener: &TcpListener,
    receiver_address: &str,
    chunk_state_path: &std::path::Path,
) -> Vec<Frame> {
    let (sender, _) = listener.accept().await.unwrap();
    let receiver = TcpStream::connect(receiver_address).await.unwrap();
    let (mut sender_read, mut sender_write) = sender.into_split();
    let (mut receiver_read, mut receiver_write) = receiver.into_split();
    let chunk_state_path = chunk_state_path.to_path_buf();

    let sender_to_receiver = tokio::spawn(async move {
        let mut frames = Vec::new();
        let mut first_chunk_data_seen = false;
        loop {
            match read_frame_for_version(&mut sender_read, WFP_VERSION_V03).await {
                Ok(frame) => {
                    let completes_first_chunk = frame.message_type == MessageType::Data
                        && decode_data_v03(&frame.payload).is_ok_and(|data| {
                            data.absolute_offset + data.data.len() as u64 == 1024 * 1024
                        });
                    write_frame(&mut receiver_write, &frame).await.unwrap();
                    frames.push(frame);

                    if completes_first_chunk {
                        first_chunk_data_seen = true;
                        tokio::time::timeout(Duration::from_secs(5), async {
                            loop {
                                if let Ok(bytes) = tokio::fs::read(&chunk_state_path).await
                                    && let Ok(state) = decode_chunk_state(&bytes)
                                    && state.hash(0).is_some()
                                {
                                    break;
                                }
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .expect("receiver did not persist chunk 0 after its complete DATA frame");
                        break;
                    }
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
        drop(receiver_write);
        (frames, first_chunk_data_seen)
    });

    loop {
        match read_frame_for_version(&mut receiver_read, WFP_VERSION_V03).await {
            Ok(frame) => {
                write_frame(&mut sender_write, &frame).await.unwrap();
                if frame.message_type == MessageType::Verified {
                    panic!("first session unexpectedly completed before interruption");
                }
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
            Err(error) => panic!("receiver-to-sender proxy failed: {error}"),
        }
    }
    drop(sender_write);
    let (frames, first_chunk_data_seen) = sender_to_receiver.await.unwrap();
    assert!(first_chunk_data_seen, "first chunk DATA was not forwarded");
    frames
}

#[tokio::test]
async fn wfp_v03_transfers_a_small_file_end_to_end() {
    let temp = tempdir().unwrap();

    let source_dir = temp.path().join("source");
    let dest_dir = temp.path().join("dest");

    std::fs::create_dir_all(&source_dir).unwrap();

    let contents: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
    let source_path = source_dir.join("payload.bin");
    std::fs::write(&source_path, &contents).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);

    let dest_clone = dest_dir.clone();
    let receiver_address = address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_address, &dest_clone).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    run_sender_v03(&source_path, &address).await.unwrap();

    let final_path = dest_dir.join("payload.bin");
    let wait = async {
        for _ in 0..50 {
            if final_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap();

    receiver.abort();
    let _ = receiver.await;

    assert!(final_path.exists(), "destination file must exist");

    let received = std::fs::read(&final_path).unwrap();
    assert_eq!(received, contents, "payload bytes must match");
    let partial = dest_dir.join(".warpfile/partials/payload.bin.part");
    assert_promoted_partial(&partial, &final_path);
    assert!(
        !dest_dir
            .join(".warpfile/partials/payload.bin.part.warpchunks")
            .exists()
    );

    let receipts_dir = dest_dir.join(".warpfile").join("receipts");
    assert!(receipts_dir.is_dir(), "receipts directory must exist");

    let mut matched_receipt = false;
    for entry in std::fs::read_dir(&receipts_dir).unwrap() {
        let path = entry.unwrap().path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let receipt = decode_completion_receipt(&std::fs::read(&path).unwrap()).unwrap();

        if receipt.filename == "payload.bin" && receipt.file_size == contents.len() as u64 {
            matched_receipt = true;
        }
    }

    assert!(
        matched_receipt,
        "a completion receipt for payload.bin must be persisted"
    );
}

#[tokio::test]
async fn wfp_v03_resumes_from_preseeded_partial() {
    use std::io::ErrorKind;
    use std::num::NonZeroU64;

    use warpfile::chunk::ChunkLayout;
    use warpfile::chunk_manifest::ChunkHash;
    use warpfile::chunk_state::{ChunkState, RecordedChunk, encode_chunk_state};
    use warpfile::protocol::TransferId;
    use warpfile::sender_v03::{SenderV03SessionError, send_session_v03};

    let temp = tempdir().unwrap();

    let dest_dir = temp.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();

    let contents: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
    let chunk_size: u64 = 1024;
    let layout = ChunkLayout::new(4096, chunk_size).unwrap();

    // Preseed the first two chunks as verified: [0..1024] and [1024..2048].
    let mut records = Vec::new();
    for index in 0..2u64 {
        let range = layout.range(index).unwrap();
        let start = range.offset as usize;
        let end = (range.offset + range.length) as usize;
        records.push(RecordedChunk {
            index,
            hash: ChunkHash::from_bytes(*blake3::hash(&contents[start..end]).as_bytes()),
        });
    }
    let state = ChunkState::new(layout, records).unwrap();

    let partial = dest_dir.join(".warpfile/partials/payload.bin.part");

    std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
    std::fs::write(&partial, &contents[..2048]).unwrap();
    std::fs::write(
        dest_dir.join(".warpfile/partials/payload.bin.part.warpchunks"),
        encode_chunk_state(&state).unwrap(),
    )
    .unwrap();

    // The sender derives the destination filename from the source path,
    // so the source must share the preseeded partial's basename.
    let source_path = temp.path().join("payload.bin");
    std::fs::write(&source_path, &contents).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);

    let dest_clone = dest_dir.clone();
    let receiver_address = address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_address, &dest_clone).await;
    });

    let transfer_id = TransferId::generate().unwrap();

    // send_session_v03 is single-shot, so retry while the receiver is
    // still binding; only ConnectionRefused is expected during startup.
    let mut connected = false;
    for _ in 0..50 {
        match send_session_v03(
            &source_path,
            &address,
            transfer_id,
            NonZeroU64::new(chunk_size).unwrap(),
        )
        .await
        {
            Ok(()) => {
                connected = true;
                break;
            }
            Err(SenderV03SessionError::Connect(error))
                if error.kind() == ErrorKind::ConnectionRefused =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("WFP/0.3 session failed: {error}"),
        }
    }
    assert!(connected, "receiver never started listening");

    receiver.abort();
    let _ = receiver.await;

    let final_path = dest_dir.join("payload.bin");
    assert_eq!(std::fs::read(&final_path).unwrap(), contents);
    assert_promoted_partial(&partial, &final_path);
    assert!(
        !dest_dir
            .join(".warpfile/partials/payload.bin.part.warpchunks")
            .exists(),
        ".warpchunks must be removed after finalize"
    );

    let receipts_dir = dest_dir.join(".warpfile").join("receipts");
    assert!(receipts_dir.is_dir(), "receipts directory must exist");

    let mut matched_receipt = false;
    for entry in std::fs::read_dir(&receipts_dir).unwrap() {
        let path = entry.unwrap().path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let receipt = decode_completion_receipt(&std::fs::read(&path).unwrap()).unwrap();

        if receipt.filename == "payload.bin" && receipt.file_size == 4096 {
            matched_receipt = true;
        }
    }

    assert!(
        matched_receipt,
        "a completion receipt for payload.bin must be persisted"
    );
}

#[tokio::test]
async fn wfp_v03_reuses_sparse_physically_valid_chunks_on_wire() {
    use std::num::NonZeroU64;

    use warpfile::chunk::ChunkLayout;
    use warpfile::chunk_manifest::ChunkHash;
    use warpfile::chunk_state::{ChunkState, RecordedChunk, encode_chunk_state};
    use warpfile::protocol::{TransferId, decode_chunk_start_v03, decode_data_v03};
    use warpfile::sender_v03::send_session_v03;

    let temp = tempdir().unwrap();
    let source_path = temp.path().join("payload.bin");
    let dest_dir = temp.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();
    let contents: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
    std::fs::write(&source_path, &contents).unwrap();

    let chunk_size = 1024u64;
    let layout = ChunkLayout::new(contents.len() as u64, chunk_size).unwrap();
    let mut records = Vec::new();
    for index in [0, 2] {
        let range = layout.range(index).unwrap();
        let start = range.offset as usize;
        let end = (range.offset + range.length) as usize;
        records.push(RecordedChunk {
            index,
            hash: ChunkHash::from_bytes(*blake3::hash(&contents[start..end]).as_bytes()),
        });
    }

    // Keep chunk 2 at its absolute file offset; a sparse file represents the hole for chunk 1.
    let partial = dest_dir.join(".warpfile/partials/payload.bin.part");
    std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
    let mut partial_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial)
        .unwrap();
    partial_file.set_len(contents.len() as u64).unwrap();
    use std::io::{Seek, SeekFrom, Write};
    for index in [0, 2] {
        let range = layout.range(index).unwrap();
        let start = range.offset as usize;
        let end = (range.offset + range.length) as usize;
        partial_file.seek(SeekFrom::Start(range.offset)).unwrap();
        partial_file.write_all(&contents[start..end]).unwrap();
    }
    drop(partial_file);
    let state = ChunkState::new(layout, records).unwrap();
    std::fs::write(
        dest_dir.join(".warpfile/partials/payload.bin.part.warpchunks"),
        encode_chunk_state(&state).unwrap(),
    )
    .unwrap();

    let receiver_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let receiver_address = receiver_probe.local_addr().unwrap().to_string();
    drop(receiver_probe);
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap().to_string();

    let receiver_dest = dest_dir.clone();
    let receiver_bind = receiver_address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_bind, &receiver_dest).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let proxied_receiver = receiver_address.clone();
    let proxy_task =
        tokio::spawn(async move { proxy_v03_session(&proxy, &proxied_receiver, false).await });
    send_session_v03(
        &source_path,
        &proxy_address,
        TransferId::generate().unwrap(),
        NonZeroU64::new(chunk_size).unwrap(),
    )
    .await
    .unwrap();
    let frames = proxy_task.await.unwrap();
    receiver.abort();
    let _ = receiver.await;

    let chunk_starts: Vec<u64> = frames
        .iter()
        .filter(|frame| frame.message_type == MessageType::ChunkStart)
        .map(|frame| decode_chunk_start_v03(&frame.payload).unwrap().chunk_index)
        .collect();
    assert_eq!(chunk_starts, vec![1, 3]);
    let useful_data_bytes: usize = frames
        .iter()
        .filter(|frame| frame.message_type == MessageType::Data)
        .map(|frame| decode_data_v03(&frame.payload).unwrap().data.len())
        .sum();
    assert_eq!(useful_data_bytes, 2048);

    let final_path = dest_dir.join("payload.bin");
    assert_eq!(std::fs::read(&final_path).unwrap(), contents);
    assert_promoted_partial(&partial, &final_path);
    assert!(
        dest_dir.join(".warpfile/receipts").is_dir(),
        "completion receipt directory must exist after verified completion"
    );
}

#[tokio::test]
async fn wfp_v03_revalidates_corrupted_sparse_chunk_before_reuse() {
    use std::io::{Seek, SeekFrom, Write};
    use std::num::NonZeroU64;

    use warpfile::chunk::ChunkLayout;
    use warpfile::chunk_manifest::ChunkHash;
    use warpfile::chunk_state::{
        ChunkState, RecordedChunk, decode_chunk_state, encode_chunk_state,
    };
    use warpfile::protocol::{TransferId, decode_chunk_start_v03, decode_data_v03};
    use warpfile::sender_v03::send_session_v03;

    let temp = tempdir().unwrap();
    let source_path = temp.path().join("payload.bin");
    let dest_dir = temp.path().join("dest");
    std::fs::create_dir_all(&dest_dir).unwrap();
    let contents: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
    std::fs::write(&source_path, &contents).unwrap();

    let chunk_size = 1024u64;
    let layout = ChunkLayout::new(4096, chunk_size).unwrap();
    let records: Vec<_> = [0, 2]
        .into_iter()
        .map(|index| {
            let range = layout.range(index).unwrap();
            let start = range.offset as usize;
            let end = (range.offset + range.length) as usize;
            RecordedChunk {
                index,
                hash: ChunkHash::from_bytes(*blake3::hash(&contents[start..end]).as_bytes()),
            }
        })
        .collect();
    let state = ChunkState::new(layout, records).unwrap();

    let partial = dest_dir.join(".warpfile/partials/payload.bin.part");

    std::fs::create_dir_all(partial.parent().unwrap()).unwrap();
    let mut partial_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partial)
        .unwrap();
    partial_file.set_len(contents.len() as u64).unwrap();
    for index in [0, 2] {
        let range = layout.range(index).unwrap();
        let start = range.offset as usize;
        let end = (range.offset + range.length) as usize;
        partial_file.seek(SeekFrom::Start(range.offset)).unwrap();
        partial_file.write_all(&contents[start..end]).unwrap();
    }
    drop(partial_file);

    let metadata_path = dest_dir.join(".warpfile/partials/payload.bin.part.warpchunks");

    let encoded_state = encode_chunk_state(&state).unwrap();
    std::fs::write(&metadata_path, &encoded_state).unwrap();
    let persisted = decode_chunk_state(&std::fs::read(&metadata_path).unwrap()).unwrap();
    assert_eq!(
        persisted
            .recorded_chunks()
            .iter()
            .map(|chunk| chunk.index)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );

    partial_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&partial)
        .unwrap();
    partial_file.seek(SeekFrom::Start(2 * chunk_size)).unwrap();
    partial_file
        .write_all(&vec![0xA5; chunk_size as usize])
        .unwrap();
    drop(partial_file);

    let receiver_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let receiver_address = receiver_probe.local_addr().unwrap().to_string();
    drop(receiver_probe);
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap().to_string();
    let receiver_dest = dest_dir.clone();
    let receiver_bind = receiver_address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_bind, &receiver_dest).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let proxied_receiver = receiver_address.clone();
    let proxy_task =
        tokio::spawn(async move { proxy_v03_session(&proxy, &proxied_receiver, false).await });
    send_session_v03(
        &source_path,
        &proxy_address,
        TransferId::generate().unwrap(),
        NonZeroU64::new(chunk_size).unwrap(),
    )
    .await
    .unwrap();
    let frames = proxy_task.await.unwrap();
    receiver.abort();
    let _ = receiver.await;

    let chunk_starts: Vec<u64> = frames
        .iter()
        .filter(|frame| frame.message_type == MessageType::ChunkStart)
        .map(|frame| decode_chunk_start_v03(&frame.payload).unwrap().chunk_index)
        .collect();
    assert_eq!(chunk_starts, vec![1, 2, 3]);
    assert!(!chunk_starts.contains(&0));
    let useful_data_bytes: usize = frames
        .iter()
        .filter(|frame| frame.message_type == MessageType::Data)
        .map(|frame| decode_data_v03(&frame.payload).unwrap().data.len())
        .sum();
    assert_eq!(useful_data_bytes, 3072);

    assert_eq!(
        std::fs::read(dest_dir.join("payload.bin")).unwrap(),
        contents
    );
    assert_promoted_partial(&partial, &dest_dir.join("payload.bin"));
}

#[tokio::test]
async fn wfp_v03_reconciles_after_the_first_verified_is_lost() {
    let temp = tempdir().unwrap();
    let source_path = temp.path().join("payload.bin");
    let dest_dir = temp.path().join("dest");
    let contents: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
    std::fs::write(&source_path, &contents).unwrap();

    let receiver_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let receiver_address = receiver_probe.local_addr().unwrap().to_string();
    drop(receiver_probe);

    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap().to_string();

    let receiver_dest = dest_dir.clone();
    let receiver_bind = receiver_address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_bind, &receiver_dest).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let proxied_receiver = receiver_address.clone();
    let proxy_task = tokio::spawn(async move {
        let first = proxy_v03_session(&proxy, &proxied_receiver, true).await;
        let second = proxy_v03_session(&proxy, &proxied_receiver, false).await;

        assert!(
            tokio::time::timeout(Duration::from_millis(1500), proxy.accept())
                .await
                .is_err(),
            "sender opened an unexpected third session"
        );

        [first, second]
    });

    let sender_result = run_sender_v03(&source_path, &proxy_address).await;
    let [first, second] = proxy_task.await.unwrap();

    receiver.abort();
    let _ = receiver.await;

    sender_result.unwrap();
    assert_eq!(
        std::fs::read(dest_dir.join("payload.bin")).unwrap(),
        contents
    );

    let first_offer = first
        .iter()
        .find(|frame| frame.message_type == MessageType::Offer)
        .map(|frame| decode_offer_v03(&frame.payload).unwrap())
        .unwrap();
    let second_offer = second
        .iter()
        .find(|frame| frame.message_type == MessageType::Offer)
        .map(|frame| decode_offer_v03(&frame.payload).unwrap())
        .unwrap();

    assert_eq!(first_offer.transfer_id, second_offer.transfer_id);
    assert!(
        first
            .iter()
            .any(|frame| frame.message_type == MessageType::Data),
        "first session must transfer file data"
    );
    assert_eq!(
        second
            .iter()
            .map(|frame| frame.message_type)
            .collect::<Vec<_>>(),
        vec![MessageType::Hello, MessageType::Offer],
        "reconciled session must stop after OFFER"
    );
}

#[tokio::test]
async fn wfp_v03_retries_after_a_persisted_chunk_without_retransmitting_it() {
    let temp = tempdir().unwrap();
    let source_path = temp.path().join("payload.bin");
    let dest_dir = temp.path().join("dest");
    let contents: Vec<u8> = (0..4 * 1024 * 1024u32).map(|n| (n % 251) as u8).collect();
    std::fs::write(&source_path, &contents).unwrap();

    let receiver_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let receiver_address = receiver_probe.local_addr().unwrap().to_string();
    drop(receiver_probe);
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap().to_string();
    let partial = dest_dir.join(".warpfile/partials/payload.bin.part");
    let chunk_state_path = dest_dir.join(".warpfile/partials/payload.bin.part.warpchunks");

    let receiver_dest = dest_dir.clone();
    let receiver_bind = receiver_address.clone();
    let receiver = tokio::spawn(async move {
        let _ = run_receiver_v03(&receiver_bind, &receiver_dest).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let proxied_receiver = receiver_address.clone();
    let proxy_task = tokio::spawn(async move {
        let first =
            proxy_v03_drop_after_chunk_persisted(&proxy, &proxied_receiver, &chunk_state_path)
                .await;
        assert!(
            chunk_state_path.exists(),
            "snapshot must exist after interruption"
        );
        let second = proxy_v03_session(&proxy, &proxied_receiver, false).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), proxy.accept())
                .await
                .is_err(),
            "sender opened an unexpected third session"
        );
        [first, second]
    });

    let sender_result = run_sender_v03(&source_path, &proxy_address).await;
    let [first, second] = proxy_task.await.unwrap();
    receiver.abort();
    let _ = receiver.await;
    sender_result.unwrap();

    let chunk_starts = |frames: &[Frame]| {
        frames
            .iter()
            .filter(|frame| frame.message_type == MessageType::ChunkStart)
            .map(|frame| decode_chunk_start_v03(&frame.payload).unwrap().chunk_index)
            .collect::<Vec<_>>()
    };
    let useful_data_bytes = |frames: &[Frame]| {
        frames
            .iter()
            .filter(|frame| frame.message_type == MessageType::Data)
            .map(|frame| decode_data_v03(&frame.payload).unwrap().data.len())
            .sum::<usize>()
    };
    let offer_id = |frames: &[Frame]| {
        frames
            .iter()
            .find(|frame| frame.message_type == MessageType::Offer)
            .map(|frame| decode_offer_v03(&frame.payload).unwrap().transfer_id)
            .unwrap()
    };

    assert_eq!(chunk_starts(&first), vec![0]);
    assert_eq!(chunk_starts(&second), vec![1, 2, 3]);
    assert_eq!(useful_data_bytes(&first), 1024 * 1024);
    assert_eq!(useful_data_bytes(&second), 3 * 1024 * 1024);
    assert_eq!(offer_id(&first), offer_id(&second));
    assert!(
        second
            .iter()
            .any(|frame| frame.message_type == MessageType::Complete),
        "second session must send COMPLETE before run_sender_v03 returns success"
    );

    assert_eq!(
        std::fs::read(dest_dir.join("payload.bin")).unwrap(),
        contents
    );
    assert_promoted_partial(&partial, &dest_dir.join("payload.bin"));
    assert!(
        !dest_dir
            .join(".warpfile/partials/payload.bin.part.warpchunks")
            .exists(),
        ".warpchunks must be removed after finalize"
    );
}
