use std::fs;

use tempfile::tempdir;
use tokio::net::TcpListener;

use warpfile::receiver::receive_loop;
use warpfile::sender::run_sender;

#[tokio::test]
async fn receiver_stays_alive_for_multiple_transfers() {
    let temp = tempdir().unwrap();

    let source_directory = temp.path().join("source");

    let destination_directory = temp.path().join("received");

    fs::create_dir_all(&source_directory).unwrap();

    let first_source = source_directory.join("first.txt");

    let second_source = source_directory.join("second.txt");

    let first_data = b"first persistent transfer";

    let second_data = b"second persistent transfer";

    fs::write(&first_source, first_data).unwrap();

    fs::write(&second_source, second_data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap().to_string();

    let first_source = first_source.to_string_lossy().into_owned();

    let second_source = second_source.to_string_lossy().into_owned();

    let receiver = receive_loop(listener, &destination_directory);

    let send_two_files = async {
        let first_result = run_sender(&first_source, &address).await;

        assert!(
            first_result.is_ok(),
            "first transfer failed: {:?}",
            first_result.err()
        );

        let second_result = run_sender(&second_source, &address).await;

        assert!(
            second_result.is_ok(),
            "second transfer failed: {:?}",
            second_result.err()
        );
    };

    tokio::pin!(receiver);

    tokio::pin!(send_two_files);

    tokio::select! {
        receiver_result =
            &mut receiver =>
        {
            panic!(
                "persistent receiver stopped unexpectedly: {:?}",
                receiver_result.err()
            );
        }

        _ =
            &mut send_two_files =>
        {
        }
    }

    let first_received = fs::read(destination_directory.join("first.txt")).unwrap();

    let second_received = fs::read(destination_directory.join("second.txt")).unwrap();

    assert_eq!(first_received, first_data,);

    assert_eq!(second_received, second_data,);
}
