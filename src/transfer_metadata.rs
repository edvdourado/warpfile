use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use crate::receiver_storage;
use serde_json::{Value, json};
#[cfg(test)]
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::protocol::{TRANSFER_ID_LENGTH, TransferId};

pub const TRANSFER_METADATA_FORMAT_VERSION: u64 = 1;

const TRANSFER_METADATA_SUFFIX: &str = ".warpmeta";
const TRANSFER_METADATA_TEMP_SUFFIX: &str = ".tmp";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferState {
    Partial,
}

impl TransferState {
    const fn as_str(self) -> &'static str {
        match self {
            TransferState::Partial => "partial",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferMetadata {
    pub transfer_id: TransferId,
    pub filename: String,
    pub file_size: u64,
    pub state: TransferState,
}

#[derive(Debug)]
pub enum TransferMetadataError {
    Io(io::Error),
    Json(serde_json::Error),
    RootNotObject,
    MissingField(&'static str),
    InvalidField(&'static str),
    UnsupportedFormatVersion(u64),
    UnsupportedState(String),
    InvalidTransferId,
}

impl fmt::Display for TransferMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransferMetadataError::Io(error) => {
                write!(formatter, "transfer metadata I/O error: {error}")
            }

            TransferMetadataError::Json(error) => {
                write!(formatter, "invalid transfer metadata JSON: {error}")
            }

            TransferMetadataError::RootNotObject => {
                write!(formatter, "transfer metadata root must be a JSON object")
            }

            TransferMetadataError::MissingField(field) => {
                write!(formatter, "transfer metadata is missing field: {field}")
            }

            TransferMetadataError::InvalidField(field) => {
                write!(
                    formatter,
                    "transfer metadata contains invalid field: {field}"
                )
            }

            TransferMetadataError::UnsupportedFormatVersion(version) => {
                write!(
                    formatter,
                    "unsupported transfer metadata format version: {version}"
                )
            }

            TransferMetadataError::UnsupportedState(state) => {
                write!(formatter, "unsupported transfer metadata state: {state}")
            }

            TransferMetadataError::InvalidTransferId => {
                write!(
                    formatter,
                    "transfer metadata contains an invalid transfer ID"
                )
            }
        }
    }
}

impl Error for TransferMetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TransferMetadataError::Io(error) => Some(error),
            TransferMetadataError::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for TransferMetadataError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for TransferMetadataError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub fn transfer_metadata_path(partial_path: &Path) -> PathBuf {
    append_suffix(partial_path, TRANSFER_METADATA_SUFFIX)
}

pub async fn write_transfer_metadata(
    partial_path: &Path,
    metadata: &TransferMetadata,
) -> Result<(), TransferMetadataError> {
    let metadata_path = transfer_metadata_path(partial_path);

    let temporary_path = transfer_metadata_temporary_path(partial_path);

    let encoded = encode_transfer_metadata(metadata)?;

    /*
     * A previous crash may have left a stale
     * .warpmeta.tmp behind.
     *
     * It is never considered valid metadata,
     * so it is safe to discard before starting
     * a new write.
     */
    remove_if_exists(&temporary_path).await?;

    let write_result = async {
        let mut file =
            receiver_storage::open_promotable(&temporary_path, false, true, false, true)?;

        file.write_all(&encoded).await?;

        file.flush().await?;

        /*
         * flush() moves buffered bytes toward
         * the operating system.
         *
         * sync_all() asks the operating system
         * to persist the file contents before
         * we make this metadata visible under
         * its final filename.
         */
        file.sync_all().await?;

        let source = receiver_storage::pin(file);

        /*
         * Windows does not reliably allow a
         * rename to replace an existing target.
         *
         * Remove the old metadata first, then
         * promote the fully written temporary
         * file.
         *
         * If the process dies in this small
         * window, the metadata may be missing,
         * but the .part remains recoverable by
         * verified prefix adoption.
         */
        remove_if_exists_io(&metadata_path).await?;

        receiver_storage::promote_rename(&source, &temporary_path, &metadata_path)?;

        Ok::<(), io::Error>(())
    }
    .await;

    if let Err(error) = write_result {
        // The temp name may now refer to another object. Leave any residue;
        // the next write checks for a stale temp before creating a new one.
        return Err(TransferMetadataError::Io(error));
    }

    Ok(())
}

pub async fn read_transfer_metadata(
    partial_path: &Path,
) -> Result<TransferMetadata, TransferMetadataError> {
    let metadata_path = transfer_metadata_path(partial_path);

    let bytes = receiver_storage::read(&metadata_path).await?;

    decode_transfer_metadata(&bytes)
}

pub async fn remove_transfer_metadata(partial_path: &Path) -> Result<(), TransferMetadataError> {
    let metadata_path = transfer_metadata_path(partial_path);

    let temporary_path = transfer_metadata_temporary_path(partial_path);

    remove_if_exists(&metadata_path).await?;

    remove_if_exists(&temporary_path).await?;

    Ok(())
}

pub fn encode_transfer_metadata(
    metadata: &TransferMetadata,
) -> Result<Vec<u8>, TransferMetadataError> {
    let value = json!({
        "format_version": TRANSFER_METADATA_FORMAT_VERSION,
        "transfer_id": metadata.transfer_id.to_string(),
        "filename": metadata.filename.as_str(),
        "file_size": metadata.file_size,
        "state": metadata.state.as_str(),
    });

    Ok(serde_json::to_vec_pretty(&value)?)
}

pub fn decode_transfer_metadata(bytes: &[u8]) -> Result<TransferMetadata, TransferMetadataError> {
    let value: Value = serde_json::from_slice(bytes)?;

    let object = value
        .as_object()
        .ok_or(TransferMetadataError::RootNotObject)?;

    let format_version = object
        .get("format_version")
        .ok_or(TransferMetadataError::MissingField("format_version"))?
        .as_u64()
        .ok_or(TransferMetadataError::InvalidField("format_version"))?;

    if format_version != TRANSFER_METADATA_FORMAT_VERSION {
        return Err(TransferMetadataError::UnsupportedFormatVersion(
            format_version,
        ));
    }

    let transfer_id_text = object
        .get("transfer_id")
        .ok_or(TransferMetadataError::MissingField("transfer_id"))?
        .as_str()
        .ok_or(TransferMetadataError::InvalidField("transfer_id"))?;

    let transfer_id = parse_transfer_id(transfer_id_text)?;

    let filename = object
        .get("filename")
        .ok_or(TransferMetadataError::MissingField("filename"))?
        .as_str()
        .ok_or(TransferMetadataError::InvalidField("filename"))?
        .to_string();

    if filename.is_empty() {
        return Err(TransferMetadataError::InvalidField("filename"));
    }

    let file_size = object
        .get("file_size")
        .ok_or(TransferMetadataError::MissingField("file_size"))?
        .as_u64()
        .ok_or(TransferMetadataError::InvalidField("file_size"))?;

    let state_text = object
        .get("state")
        .ok_or(TransferMetadataError::MissingField("state"))?
        .as_str()
        .ok_or(TransferMetadataError::InvalidField("state"))?;

    let state = match state_text {
        "partial" => TransferState::Partial,

        other => {
            return Err(TransferMetadataError::UnsupportedState(other.to_string()));
        }
    };

    Ok(TransferMetadata {
        transfer_id,
        filename,
        file_size,
        state,
    })
}

fn transfer_metadata_temporary_path(partial_path: &Path) -> PathBuf {
    let metadata_path = transfer_metadata_path(partial_path);

    append_suffix(&metadata_path, TRANSFER_METADATA_TEMP_SUFFIX)
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_os_string();

    value.push(suffix);

    PathBuf::from(value)
}

async fn remove_if_exists(path: &Path) -> Result<(), TransferMetadataError> {
    remove_if_exists_io(path).await?;

    Ok(())
}

async fn remove_if_exists_io(path: &Path) -> Result<(), io::Error> {
    match receiver_storage::remove_file(path) {
        Ok(()) => Ok(()),

        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),

        Err(error) => Err(error),
    }
}

fn parse_transfer_id(text: &str) -> Result<TransferId, TransferMetadataError> {
    if text.len() != TRANSFER_ID_LENGTH * 2 {
        return Err(TransferMetadataError::InvalidTransferId);
    }

    let mut bytes = [0u8; TRANSFER_ID_LENGTH];

    for (index, byte) in bytes.iter_mut().enumerate() {
        let high_index = index * 2;
        let low_index = high_index + 1;

        let high = decode_hex_digit(text.as_bytes()[high_index])
            .ok_or(TransferMetadataError::InvalidTransferId)?;

        let low = decode_hex_digit(text.as_bytes()[low_index])
            .ok_or(TransferMetadataError::InvalidTransferId)?;

        *byte = (high << 4) | low;
    }

    Ok(TransferId::from_bytes(bytes))
}

const fn decode_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    fn test_metadata() -> TransferMetadata {
        TransferMetadata {
            transfer_id: TransferId::from_bytes([
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
                0xEE, 0xFF,
            ]),
            filename: "arquivo.bin".to_string(),
            file_size: 123_456_789,
            state: TransferState::Partial,
        }
    }

    fn replacement_metadata() -> TransferMetadata {
        TransferMetadata {
            transfer_id: TransferId::from_bytes([
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                0x1E, 0x1F,
            ]),
            filename: "arquivo.bin".to_string(),
            file_size: 987_654_321,
            state: TransferState::Partial,
        }
    }

    #[test]
    fn transfer_metadata_round_trip() {
        let original = test_metadata();

        let encoded = encode_transfer_metadata(&original).unwrap();

        let decoded = decode_transfer_metadata(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn encodes_transfer_id_as_readable_hex() {
        let metadata = test_metadata();

        let encoded = encode_transfer_metadata(&metadata).unwrap();

        let value: Value = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(value["transfer_id"], "00112233445566778899aabbccddeeff");

        assert_eq!(value["format_version"], TRANSFER_METADATA_FORMAT_VERSION);

        assert_eq!(value["state"], "partial");
    }

    #[test]
    fn rejects_unsupported_format_version() {
        let bytes = br#"
        {
            "format_version": 99,
            "transfer_id": "00112233445566778899aabbccddeeff",
            "filename": "arquivo.bin",
            "file_size": 100,
            "state": "partial"
        }
        "#;

        let error = decode_transfer_metadata(bytes).unwrap_err();

        assert!(matches!(
            error,
            TransferMetadataError::UnsupportedFormatVersion(99)
        ));
    }

    #[test]
    fn rejects_invalid_transfer_id() {
        let bytes = br#"
        {
            "format_version": 1,
            "transfer_id": "isso-nao-e-um-transfer-id",
            "filename": "arquivo.bin",
            "file_size": 100,
            "state": "partial"
        }
        "#;

        let error = decode_transfer_metadata(bytes).unwrap_err();

        assert!(matches!(error, TransferMetadataError::InvalidTransferId));
    }

    #[test]
    fn rejects_unknown_state() {
        let bytes = br#"
        {
            "format_version": 1,
            "transfer_id": "00112233445566778899aabbccddeeff",
            "filename": "arquivo.bin",
            "file_size": 100,
            "state": "teleported"
        }
        "#;

        let error = decode_transfer_metadata(bytes).unwrap_err();

        assert!(matches!(
            error,
            TransferMetadataError::UnsupportedState(ref state)
                if state == "teleported"
        ));
    }

    #[test]
    fn derives_metadata_path_from_partial_path() {
        let partial_path = Path::new("received").join("video.mkv.part");

        let metadata_path = transfer_metadata_path(&partial_path);

        assert_eq!(
            metadata_path,
            Path::new("received").join("video.mkv.part.warpmeta")
        );
    }

    #[tokio::test]
    async fn writes_and_reads_transfer_metadata_file() {
        let temp = tempdir().unwrap();

        let partial_path = temp.path().join("arquivo.bin.part");

        let original = test_metadata();

        write_transfer_metadata(&partial_path, &original)
            .await
            .unwrap();

        let metadata_path = transfer_metadata_path(&partial_path);

        assert!(metadata_path.exists(), "metadata file was not created");

        let loaded = read_transfer_metadata(&partial_path).await.unwrap();

        assert_eq!(loaded, original);
    }

    #[tokio::test]
    async fn replaces_existing_transfer_metadata_file() {
        let temp = tempdir().unwrap();

        let partial_path = temp.path().join("arquivo.bin.part");

        let first = test_metadata();

        let replacement = replacement_metadata();

        write_transfer_metadata(&partial_path, &first)
            .await
            .unwrap();

        write_transfer_metadata(&partial_path, &replacement)
            .await
            .unwrap();

        let loaded = read_transfer_metadata(&partial_path).await.unwrap();

        assert_eq!(loaded, replacement);
    }

    #[tokio::test]
    async fn removes_transfer_metadata_file_and_stale_temporary_file() {
        let temp = tempdir().unwrap();

        let partial_path = temp.path().join("arquivo.bin.part");

        let metadata = test_metadata();

        write_transfer_metadata(&partial_path, &metadata)
            .await
            .unwrap();

        let metadata_path = transfer_metadata_path(&partial_path);

        let temporary_path = transfer_metadata_temporary_path(&partial_path);

        fs::write(&temporary_path, b"stale temporary metadata")
            .await
            .unwrap();

        assert!(metadata_path.exists());

        assert!(temporary_path.exists());

        remove_transfer_metadata(&partial_path).await.unwrap();

        assert!(
            !metadata_path.exists(),
            "metadata file remained after removal"
        );

        assert!(
            !temporary_path.exists(),
            "temporary metadata file remained after removal"
        );
    }

    #[tokio::test]
    async fn rejects_corrupted_transfer_metadata_file() {
        let temp = tempdir().unwrap();

        let partial_path = temp.path().join("arquivo.bin.part");

        let metadata_path = transfer_metadata_path(&partial_path);

        fs::write(&metadata_path, b"{ this is not valid JSON")
            .await
            .unwrap();

        let error = read_transfer_metadata(&partial_path).await.unwrap_err();

        assert!(matches!(error, TransferMetadataError::Json(_)));
    }
}
