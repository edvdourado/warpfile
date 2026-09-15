use crate::chunk::ChunkLayout;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkHash([u8; 32]);

impl ChunkHash {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkManifest {
    layout: ChunkLayout,
    hashes: Vec<ChunkHash>,
}

impl ChunkManifest {
    pub const fn layout(&self) -> ChunkLayout {
        self.layout
    }

    pub fn chunk_count(&self) -> u64 {
        self.layout.chunk_count()
    }

    pub fn hash(&self, index: u64) -> Option<ChunkHash> {
        let index = usize::try_from(index).ok()?;
        self.hashes.get(index).copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkManifestError {
    IncompleteData { expected: u64, actual: u64 },
    TooMuchData { expected: u64, attempted: u64 },
}

impl fmt::Display for ChunkManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteData { expected, actual } => write!(
                formatter,
                "incomplete data for chunk manifest: expected {expected} bytes, received {actual}"
            ),
            Self::TooMuchData {
                expected,
                attempted,
            } => write!(
                formatter,
                "too much data for chunk manifest: expected {expected} bytes, attempted {attempted}"
            ),
        }
    }
}

impl Error for ChunkManifestError {}

pub struct ChunkManifestBuilder {
    layout: ChunkLayout,
    hashes: Vec<ChunkHash>,
    current_hasher: blake3::Hasher,
    current_chunk_length: u64,
    bytes_received: u64,
}

impl ChunkManifestBuilder {
    pub fn new(layout: ChunkLayout) -> Self {
        Self {
            layout,
            hashes: Vec::new(),
            current_hasher: blake3::Hasher::new(),
            current_chunk_length: 0,
            bytes_received: 0,
        }
    }

    pub fn update(&mut self, mut data: &[u8]) -> Result<(), ChunkManifestError> {
        let input_length =
            u64::try_from(data.len()).expect("slice length must fit into u64 on supported targets");

        let attempted = self.bytes_received.saturating_add(input_length);

        if input_length > self.layout.file_size() - self.bytes_received {
            return Err(ChunkManifestError::TooMuchData {
                expected: self.layout.file_size(),
                attempted,
            });
        }

        while !data.is_empty() {
            let chunk_index = u64::try_from(self.hashes.len())
                .expect("chunk count must fit into u64 on supported targets");

            let range = self
                .layout
                .range(chunk_index)
                .expect("validated input must belong to an existing chunk");

            let remaining_in_chunk = range.length - self.current_chunk_length;

            let take = usize::try_from(remaining_in_chunk)
                .unwrap_or(usize::MAX)
                .min(data.len());

            let take_u64 =
                u64::try_from(take).expect("slice length must fit into u64 on supported targets");

            self.current_hasher.update(&data[..take]);
            self.current_chunk_length += take_u64;
            self.bytes_received += take_u64;

            if self.current_chunk_length == range.length {
                let digest = self.current_hasher.finalize();

                self.hashes.push(ChunkHash::from_bytes(*digest.as_bytes()));

                self.current_hasher = blake3::Hasher::new();
                self.current_chunk_length = 0;
            }

            data = &data[take..];
        }

        Ok(())
    }

    pub fn finish(self) -> Result<ChunkManifest, ChunkManifestError> {
        if self.bytes_received != self.layout.file_size() {
            return Err(ChunkManifestError::IncompleteData {
                expected: self.layout.file_size(),
                actual: self.bytes_received,
            });
        }

        Ok(ChunkManifest {
            layout: self.layout,
            hashes: self.hashes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk_hash(data: &[u8]) -> ChunkHash {
        ChunkHash::from_bytes(*blake3::hash(data).as_bytes())
    }

    #[test]
    fn empty_file_produces_empty_manifest() {
        let layout = ChunkLayout::new(0, 4).unwrap();
        let builder = ChunkManifestBuilder::new(layout);

        let manifest = builder.finish().unwrap();

        assert_eq!(manifest.layout(), layout);
        assert_eq!(manifest.chunk_count(), 0);
        assert_eq!(manifest.hash(0), None);
    }

    #[test]
    fn single_chunk_hash_matches_blake3() {
        let data = b"hello";
        let layout = ChunkLayout::new(5, 16).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        builder.update(data).unwrap();
        let manifest = builder.finish().unwrap();

        assert_eq!(manifest.chunk_count(), 1);
        assert_eq!(manifest.hash(0), Some(chunk_hash(data)));
    }

    #[test]
    fn multiple_chunks_have_independent_hashes() {
        let data = b"abcdefgh";
        let layout = ChunkLayout::new(8, 4).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        builder.update(data).unwrap();
        let manifest = builder.finish().unwrap();

        assert_eq!(manifest.chunk_count(), 2);
        assert_eq!(manifest.hash(0), Some(chunk_hash(b"abcd")));
        assert_eq!(manifest.hash(1), Some(chunk_hash(b"efgh")));
    }

    #[test]
    fn last_partial_chunk_is_hashed_correctly() {
        let data = b"abcdef";
        let layout = ChunkLayout::new(6, 4).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        builder.update(data).unwrap();
        let manifest = builder.finish().unwrap();

        assert_eq!(manifest.chunk_count(), 2);
        assert_eq!(manifest.hash(0), Some(chunk_hash(b"abcd")));
        assert_eq!(manifest.hash(1), Some(chunk_hash(b"ef")));
    }

    #[test]
    fn arbitrary_update_boundaries_do_not_change_manifest() {
        let data = b"ABCDEFGHIJ";
        let layout = ChunkLayout::new(10, 4).unwrap();

        let mut single_update = ChunkManifestBuilder::new(layout);
        single_update.update(data).unwrap();
        let single_manifest = single_update.finish().unwrap();

        let mut split_updates = ChunkManifestBuilder::new(layout);
        split_updates.update(&data[..2]).unwrap();
        split_updates.update(&data[2..7]).unwrap();
        split_updates.update(&data[7..9]).unwrap();
        split_updates.update(&data[9..]).unwrap();
        let split_manifest = split_updates.finish().unwrap();

        assert_eq!(split_manifest, single_manifest);
        assert_eq!(split_manifest.hash(0), Some(chunk_hash(b"ABCD")));
        assert_eq!(split_manifest.hash(1), Some(chunk_hash(b"EFGH")));
        assert_eq!(split_manifest.hash(2), Some(chunk_hash(b"IJ")));
    }

    #[test]
    fn finish_rejects_incomplete_input() {
        let layout = ChunkLayout::new(10, 4).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        builder.update(b"12345678").unwrap();

        assert_eq!(
            builder.finish(),
            Err(ChunkManifestError::IncompleteData {
                expected: 10,
                actual: 8,
            })
        );
    }

    #[test]
    fn update_rejects_data_beyond_file_size() {
        let layout = ChunkLayout::new(10, 4).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        assert_eq!(
            builder.update(b"12345678901"),
            Err(ChunkManifestError::TooMuchData {
                expected: 10,
                attempted: 11,
            })
        );

        assert_eq!(
            builder.finish(),
            Err(ChunkManifestError::IncompleteData {
                expected: 10,
                actual: 0,
            })
        );
    }

    #[test]
    fn rejected_extra_data_does_not_corrupt_completed_manifest() {
        let layout = ChunkLayout::new(10, 4).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        builder.update(b"ABCDEFGHIJ").unwrap();

        assert_eq!(
            builder.update(b"K"),
            Err(ChunkManifestError::TooMuchData {
                expected: 10,
                attempted: 11,
            })
        );

        let manifest = builder.finish().unwrap();

        assert_eq!(manifest.chunk_count(), 3);
        assert_eq!(manifest.hash(0), Some(chunk_hash(b"ABCD")));
        assert_eq!(manifest.hash(1), Some(chunk_hash(b"EFGH")));
        assert_eq!(manifest.hash(2), Some(chunk_hash(b"IJ")));
    }

    #[test]
    fn manifest_preserves_layout() {
        let layout = ChunkLayout::new(10, 4).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        builder.update(b"1234567890").unwrap();
        let manifest = builder.finish().unwrap();

        assert_eq!(manifest.layout(), layout);
        assert_eq!(manifest.layout().file_size(), 10);
        assert_eq!(manifest.layout().chunk_size(), 4);
        assert_eq!(manifest.chunk_count(), 3);
    }

    #[test]
    fn out_of_range_hash_returns_none() {
        let layout = ChunkLayout::new(6, 4).unwrap();
        let mut builder = ChunkManifestBuilder::new(layout);

        builder.update(b"abcdef").unwrap();
        let manifest = builder.finish().unwrap();

        assert_eq!(manifest.hash(0), Some(chunk_hash(b"abcd")));
        assert_eq!(manifest.hash(1), Some(chunk_hash(b"ef")));
        assert_eq!(manifest.hash(2), None);
        assert_eq!(manifest.hash(u64::MAX), None);
    }
}
