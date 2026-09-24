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

pub const COMPLETION_RECEIPT_FORMAT_VERSION: u64 = 1;

pub const BLAKE3_HASH_LENGTH: usize = 32;

const RECEIPTS_DIRECTORY: &str = "receipts";
const RECEIPT_EXTENSION: &str = "json";
const TEMPORARY_SUFFIX: &str = ".tmp";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionReceipt {
    pub transfer_id: TransferId,
    pub filename: String,
    pub file_size: u64,
    pub blake3: [u8; BLAKE3_HASH_LENGTH],
}

#[derive(Debug)]
pub enum CompletionReceiptError {
    Io(io::Error),
    Json(serde_json::Error),
    RootNotObject,
    MissingField(&'static str),
    InvalidField(&'static str),
    UnsupportedFormatVersion(u64),
    InvalidTransferId,
    InvalidBlake3,
    ConflictingReceipt,
}

impl fmt::Display for CompletionReceiptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompletionReceiptError::Io(error) => {
                write!(formatter, "completion receipt I/O error: {error}")
            }

            CompletionReceiptError::Json(error) => {
                write!(formatter, "invalid completion receipt JSON: {error}")
            }

            CompletionReceiptError::RootNotObject => {
                write!(formatter, "completion receipt root must be a JSON object")
            }

            CompletionReceiptError::MissingField(field) => {
                write!(formatter, "completion receipt is missing field: {field}")
            }

            CompletionReceiptError::InvalidField(field) => {
                write!(
                    formatter,
                    "completion receipt contains invalid field: {field}"
                )
            }

            CompletionReceiptError::UnsupportedFormatVersion(version) => {
                write!(
                    formatter,
                    "unsupported completion receipt format version: {version}"
                )
            }

            CompletionReceiptError::InvalidTransferId => {
                write!(
                    formatter,
                    "completion receipt contains an invalid transfer ID"
                )
            }

            CompletionReceiptError::InvalidBlake3 => {
                write!(
                    formatter,
                    "completion receipt contains an invalid BLAKE3 hash"
                )
            }

            CompletionReceiptError::ConflictingReceipt => {
                write!(
                    formatter,
                    "a different completion receipt already exists for this transfer ID"
                )
            }
        }
    }
}

impl Error for CompletionReceiptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CompletionReceiptError::Io(error) => Some(error),
            CompletionReceiptError::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for CompletionReceiptError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for CompletionReceiptError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub fn completion_receipt_path(destination_directory: &Path, transfer_id: TransferId) -> PathBuf {
    crate::receiver_paths::internal_directory(destination_directory)
        .join(RECEIPTS_DIRECTORY)
        .join(format!("{transfer_id}.{RECEIPT_EXTENSION}"))
}

pub async fn write_completion_receipt(
    destination_directory: &Path,
    receipt: &CompletionReceipt,
) -> Result<(), CompletionReceiptError> {
    let receipt_path = completion_receipt_path(destination_directory, receipt.transfer_id);

    let receipt_directory = receipt_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "completion receipt path has no parent directory",
        )
    })?;

    receiver_storage::ensure_directory(receipt_directory)?;

    /*
     * Completion receipts are immutable.
     *
     * If this transfer ID already has a receipt,
     * the only valid case is that it describes
     * exactly the same completed transfer.
     */
    if receiver_storage::exists(&receipt_path)? {
        let existing = read_completion_receipt(destination_directory, receipt.transfer_id).await?;

        if existing == *receipt {
            return Ok(());
        }

        return Err(CompletionReceiptError::ConflictingReceipt);
    }

    let temporary_path =
        completion_receipt_temporary_path(destination_directory, receipt.transfer_id);

    remove_if_exists(&temporary_path).await?;

    let encoded = encode_completion_receipt(receipt)?;

    let mut file = receiver_storage::open_promotable(&temporary_path, false, true, false, true)?;
    file.write_all(&encoded).await?;
    file.flush().await?;

    /*
     * The receipt must be durable before it
     * becomes visible under its final name.
     */
    file.sync_all().await?;

    let source = receiver_storage::pin(file);
    publish_completion_receipt(&source, &temporary_path, destination_directory, receipt).await
}

async fn publish_completion_receipt(
    source: &receiver_storage::PinnedFile,
    temporary_path: &Path,
    destination_directory: &Path,
    receipt: &CompletionReceipt,
) -> Result<(), CompletionReceiptError> {
    let receipt_path = completion_receipt_path(destination_directory, receipt.transfer_id);
    match receiver_storage::promote_rename_no_replace(source, temporary_path, &receipt_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let existing =
                read_completion_receipt(destination_directory, receipt.transfer_id).await?;
            if existing == *receipt {
                Ok(())
            } else {
                Err(CompletionReceiptError::ConflictingReceipt)
            }
        }
        Err(error) => Err(CompletionReceiptError::Io(error)),
    }
}

pub async fn read_completion_receipt(
    destination_directory: &Path,
    transfer_id: TransferId,
) -> Result<CompletionReceipt, CompletionReceiptError> {
    let receipt_path = completion_receipt_path(destination_directory, transfer_id);

    let bytes = receiver_storage::read(&receipt_path).await?;

    let receipt = decode_completion_receipt(&bytes)?;

    if receipt.transfer_id != transfer_id {
        return Err(CompletionReceiptError::InvalidTransferId);
    }

    Ok(receipt)
}

pub async fn remove_completion_receipt(
    destination_directory: &Path,
    transfer_id: TransferId,
) -> Result<(), CompletionReceiptError> {
    let receipt_path = completion_receipt_path(destination_directory, transfer_id);

    let temporary_path = completion_receipt_temporary_path(destination_directory, transfer_id);

    remove_if_exists(&receipt_path).await?;

    remove_if_exists(&temporary_path).await?;

    Ok(())
}

pub fn encode_completion_receipt(
    receipt: &CompletionReceipt,
) -> Result<Vec<u8>, CompletionReceiptError> {
    let value = json!({
        "format_version": COMPLETION_RECEIPT_FORMAT_VERSION,
        "transfer_id": receipt.transfer_id.to_string(),
        "filename": receipt.filename.as_str(),
        "file_size": receipt.file_size,
        "blake3": encode_hex(&receipt.blake3),
    });

    Ok(serde_json::to_vec_pretty(&value)?)
}

pub fn decode_completion_receipt(
    bytes: &[u8],
) -> Result<CompletionReceipt, CompletionReceiptError> {
    let value: Value = serde_json::from_slice(bytes)?;

    let object = value
        .as_object()
        .ok_or(CompletionReceiptError::RootNotObject)?;

    let format_version = object
        .get("format_version")
        .ok_or(CompletionReceiptError::MissingField("format_version"))?
        .as_u64()
        .ok_or(CompletionReceiptError::InvalidField("format_version"))?;

    if format_version != COMPLETION_RECEIPT_FORMAT_VERSION {
        return Err(CompletionReceiptError::UnsupportedFormatVersion(
            format_version,
        ));
    }

    let transfer_id_text = object
        .get("transfer_id")
        .ok_or(CompletionReceiptError::MissingField("transfer_id"))?
        .as_str()
        .ok_or(CompletionReceiptError::InvalidField("transfer_id"))?;

    let transfer_id = parse_transfer_id(transfer_id_text)?;

    let filename = object
        .get("filename")
        .ok_or(CompletionReceiptError::MissingField("filename"))?
        .as_str()
        .ok_or(CompletionReceiptError::InvalidField("filename"))?
        .to_string();

    if filename.is_empty() {
        return Err(CompletionReceiptError::InvalidField("filename"));
    }

    let file_size = object
        .get("file_size")
        .ok_or(CompletionReceiptError::MissingField("file_size"))?
        .as_u64()
        .ok_or(CompletionReceiptError::InvalidField("file_size"))?;

    let blake3_text = object
        .get("blake3")
        .ok_or(CompletionReceiptError::MissingField("blake3"))?
        .as_str()
        .ok_or(CompletionReceiptError::InvalidField("blake3"))?;

    let blake3 = parse_blake3(blake3_text)?;

    Ok(CompletionReceipt {
        transfer_id,
        filename,
        file_size,
        blake3,
    })
}

fn completion_receipt_temporary_path(
    destination_directory: &Path,
    transfer_id: TransferId,
) -> PathBuf {
    let receipt_path = completion_receipt_path(destination_directory, transfer_id);

    append_suffix(&receipt_path, TEMPORARY_SUFFIX)
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_os_string();

    value.push(suffix);

    PathBuf::from(value)
}

async fn remove_if_exists(path: &Path) -> Result<(), CompletionReceiptError> {
    match receiver_storage::remove_file(path) {
        Ok(()) => Ok(()),

        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),

        Err(error) => Err(CompletionReceiptError::Io(error)),
    }
}

fn parse_transfer_id(text: &str) -> Result<TransferId, CompletionReceiptError> {
    let bytes =
        parse_hex::<TRANSFER_ID_LENGTH>(text).ok_or(CompletionReceiptError::InvalidTransferId)?;

    Ok(TransferId::from_bytes(bytes))
}

fn parse_blake3(text: &str) -> Result<[u8; BLAKE3_HASH_LENGTH], CompletionReceiptError> {
    parse_hex::<BLAKE3_HASH_LENGTH>(text).ok_or(CompletionReceiptError::InvalidBlake3)
}

fn parse_hex<const LENGTH: usize>(text: &str) -> Option<[u8; LENGTH]> {
    if text.len() != LENGTH * 2 {
        return None;
    }

    let text_bytes = text.as_bytes();

    let mut bytes = [0u8; LENGTH];

    for (index, output_byte) in bytes.iter_mut().enumerate() {
        let high_index = index * 2;

        let low_index = high_index + 1;

        let high = decode_hex_digit(text_bytes[high_index])?;

        let low = decode_hex_digit(text_bytes[low_index])?;

        *output_byte = (high << 4) | low;
    }

    Some(bytes)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

    let mut output = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        output.push(HEX_DIGITS[(byte >> 4) as usize] as char);

        output.push(HEX_DIGITS[(byte & 0x0F) as usize] as char);
    }

    output
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

    fn test_transfer_id() -> TransferId {
        TransferId::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ])
    }

    fn test_receipt() -> CompletionReceipt {
        CompletionReceipt {
            transfer_id: test_transfer_id(),

            filename: "arquivo.bin".to_string(),

            file_size: 123_456_789,

            blake3: [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B,
                0x1C, 0x1D, 0x1E, 0x1F,
            ],
        }
    }

    fn conflicting_receipt() -> CompletionReceipt {
        let mut receipt = test_receipt();

        receipt.file_size += 1;

        receipt
    }

    async fn staged_receipt(
        destination_directory: &Path,
        receipt: &CompletionReceipt,
    ) -> (PathBuf, receiver_storage::PinnedFile) {
        let temporary_path =
            completion_receipt_temporary_path(destination_directory, receipt.transfer_id);
        receiver_storage::ensure_directory(temporary_path.parent().unwrap()).unwrap();
        let mut file =
            receiver_storage::open_promotable(&temporary_path, false, true, false, true).unwrap();
        file.write_all(&encode_completion_receipt(receipt).unwrap())
            .await
            .unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
        (temporary_path, receiver_storage::pin(file))
    }

    #[test]
    fn completion_receipt_round_trip() {
        let original = test_receipt();

        let encoded = encode_completion_receipt(&original).unwrap();

        let decoded = decode_completion_receipt(&encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn encodes_transfer_id_and_blake3_as_lowercase_hex() {
        let receipt = test_receipt();

        let encoded = encode_completion_receipt(&receipt).unwrap();

        let value: Value = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(value["transfer_id"], "00112233445566778899aabbccddeeff");

        assert_eq!(
            value["blake3"],
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );

        assert_eq!(value["format_version"], COMPLETION_RECEIPT_FORMAT_VERSION);
    }

    #[test]
    fn rejects_unsupported_format_version() {
        let bytes = br#"
        {
            "format_version": 99,
            "transfer_id": "00112233445566778899aabbccddeeff",
            "filename": "arquivo.bin",
            "file_size": 100,
            "blake3": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        }
        "#;

        let error = decode_completion_receipt(bytes).unwrap_err();

        assert!(matches!(
            error,
            CompletionReceiptError::UnsupportedFormatVersion(99)
        ));
    }

    #[test]
    fn rejects_invalid_transfer_id() {
        let bytes = br#"
        {
            "format_version": 1,
            "transfer_id": "not-a-transfer-id",
            "filename": "arquivo.bin",
            "file_size": 100,
            "blake3": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        }
        "#;

        let error = decode_completion_receipt(bytes).unwrap_err();

        assert!(matches!(error, CompletionReceiptError::InvalidTransferId));
    }

    #[test]
    fn rejects_invalid_blake3_hash() {
        let bytes = br#"
        {
            "format_version": 1,
            "transfer_id": "00112233445566778899aabbccddeeff",
            "filename": "arquivo.bin",
            "file_size": 100,
            "blake3": "not-a-blake3-hash"
        }
        "#;

        let error = decode_completion_receipt(bytes).unwrap_err();

        assert!(matches!(error, CompletionReceiptError::InvalidBlake3));
    }

    #[test]
    fn derives_completion_receipt_path() {
        let destination = Path::new("received");

        let path = completion_receipt_path(destination, test_transfer_id());

        assert_eq!(
            path,
            Path::new("received")
                .join(".warpfile")
                .join("receipts")
                .join("00112233445566778899aabbccddeeff.json")
        );
    }

    #[tokio::test]
    async fn writes_and_reads_completion_receipt() {
        let temp = tempdir().unwrap();

        let receipt = test_receipt();

        write_completion_receipt(temp.path(), &receipt)
            .await
            .unwrap();

        let path = completion_receipt_path(temp.path(), receipt.transfer_id);

        assert!(path.exists(), "completion receipt was not created");
        assert!(!completion_receipt_temporary_path(temp.path(), receipt.transfer_id).exists());

        let loaded = read_completion_receipt(temp.path(), receipt.transfer_id)
            .await
            .unwrap();

        assert_eq!(loaded, receipt);
    }

    #[tokio::test]
    async fn writing_same_receipt_is_idempotent() {
        let temp = tempdir().unwrap();

        let receipt = test_receipt();

        write_completion_receipt(temp.path(), &receipt)
            .await
            .unwrap();

        write_completion_receipt(temp.path(), &receipt)
            .await
            .unwrap();

        let loaded = read_completion_receipt(temp.path(), receipt.transfer_id)
            .await
            .unwrap();

        assert_eq!(loaded, receipt);
    }

    #[tokio::test]
    async fn rejects_conflicting_existing_receipt() {
        let temp = tempdir().unwrap();

        let receipt = test_receipt();

        let conflicting = conflicting_receipt();

        write_completion_receipt(temp.path(), &receipt)
            .await
            .unwrap();

        let error = write_completion_receipt(temp.path(), &conflicting)
            .await
            .unwrap_err();

        assert!(matches!(error, CompletionReceiptError::ConflictingReceipt));

        let loaded = read_completion_receipt(temp.path(), receipt.transfer_id)
            .await
            .unwrap();

        assert_eq!(loaded, receipt);
    }

    #[tokio::test]
    async fn publication_race_with_identical_receipt_is_idempotent() {
        let temp = tempdir().unwrap();
        let receipt = test_receipt();
        let (temporary_path, source) = staged_receipt(temp.path(), &receipt).await;
        let receipt_path = completion_receipt_path(temp.path(), receipt.transfer_id);
        let encoded = encode_completion_receipt(&receipt).unwrap();
        fs::write(&receipt_path, &encoded).await.unwrap();
        let original_identity = receiver_storage::promoted_identity(&receipt_path).unwrap();

        publish_completion_receipt(&source, &temporary_path, temp.path(), &receipt)
            .await
            .unwrap();

        assert_eq!(
            receiver_storage::promoted_identity(&receipt_path).unwrap(),
            original_identity
        );
        assert_eq!(fs::read(&receipt_path).await.unwrap(), encoded);
        assert_eq!(fs::read(&temporary_path).await.unwrap(), encoded);
    }

    #[tokio::test]
    async fn publication_race_with_conflicting_receipt_preserves_existing() {
        let temp = tempdir().unwrap();
        let receipt = test_receipt();
        let (temporary_path, source) = staged_receipt(temp.path(), &receipt).await;
        let receipt_path = completion_receipt_path(temp.path(), receipt.transfer_id);
        let existing = encode_completion_receipt(&conflicting_receipt()).unwrap();
        fs::write(&receipt_path, &existing).await.unwrap();
        let original_identity = receiver_storage::promoted_identity(&receipt_path).unwrap();

        let error = publish_completion_receipt(&source, &temporary_path, temp.path(), &receipt)
            .await
            .unwrap_err();

        assert!(matches!(error, CompletionReceiptError::ConflictingReceipt));
        assert_eq!(
            receiver_storage::promoted_identity(&receipt_path).unwrap(),
            original_identity
        );
        assert_eq!(fs::read(&receipt_path).await.unwrap(), existing);
        assert_eq!(
            fs::read(&temporary_path).await.unwrap(),
            encode_completion_receipt(&receipt).unwrap()
        );
    }

    #[tokio::test]
    async fn publication_race_with_malformed_receipt_preserves_existing() {
        let temp = tempdir().unwrap();
        let receipt = test_receipt();
        let (temporary_path, source) = staged_receipt(temp.path(), &receipt).await;
        let receipt_path = completion_receipt_path(temp.path(), receipt.transfer_id);
        let malformed = b"{ this is not valid JSON";
        fs::write(&receipt_path, malformed).await.unwrap();
        let original_identity = receiver_storage::promoted_identity(&receipt_path).unwrap();

        let error = publish_completion_receipt(&source, &temporary_path, temp.path(), &receipt)
            .await
            .unwrap_err();

        assert!(matches!(error, CompletionReceiptError::Json(_)));
        assert_eq!(
            receiver_storage::promoted_identity(&receipt_path).unwrap(),
            original_identity
        );
        assert_eq!(fs::read(&receipt_path).await.unwrap(), malformed);
        assert_eq!(
            fs::read(&temporary_path).await.unwrap(),
            encode_completion_receipt(&receipt).unwrap()
        );
    }

    #[tokio::test]
    async fn removes_receipt_and_stale_temporary_file() {
        let temp = tempdir().unwrap();

        let receipt = test_receipt();

        write_completion_receipt(temp.path(), &receipt)
            .await
            .unwrap();

        let receipt_path = completion_receipt_path(temp.path(), receipt.transfer_id);

        let temporary_path = completion_receipt_temporary_path(temp.path(), receipt.transfer_id);

        fs::write(&temporary_path, b"stale receipt").await.unwrap();

        assert!(receipt_path.exists());

        assert!(temporary_path.exists());

        remove_completion_receipt(temp.path(), receipt.transfer_id)
            .await
            .unwrap();

        assert!(
            !receipt_path.exists(),
            "completion receipt remained after removal"
        );

        assert!(
            !temporary_path.exists(),
            "temporary completion receipt remained after removal"
        );
    }

    #[tokio::test]
    async fn rejects_corrupted_receipt_file() {
        let temp = tempdir().unwrap();

        let transfer_id = test_transfer_id();

        let receipt_path = completion_receipt_path(temp.path(), transfer_id);

        let parent = receipt_path.parent().unwrap();

        fs::create_dir_all(parent).await.unwrap();

        fs::write(&receipt_path, b"{ this is not valid JSON")
            .await
            .unwrap();

        let error = read_completion_receipt(temp.path(), transfer_id)
            .await
            .unwrap_err();

        assert!(matches!(error, CompletionReceiptError::Json(_)));
    }
}
