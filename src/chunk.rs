use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLayout {
    file_size: u64,
    chunk_size: NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkRange {
    pub index: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkLayoutError {
    ZeroChunkSize,
}

impl ChunkLayout {
    pub fn new(file_size: u64, chunk_size: u64) -> Result<Self, ChunkLayoutError> {
        let chunk_size = NonZeroU64::new(chunk_size).ok_or(ChunkLayoutError::ZeroChunkSize)?;

        Ok(Self {
            file_size,
            chunk_size,
        })
    }

    pub const fn file_size(&self) -> u64 {
        self.file_size
    }

    pub const fn chunk_size(&self) -> u64 {
        self.chunk_size.get()
    }

    pub fn chunk_count(&self) -> u64 {
        let chunk_size = self.chunk_size.get();
        let quotient = self.file_size / chunk_size;
        let remainder = self.file_size % chunk_size;

        if remainder == 0 {
            quotient
        } else {
            quotient + 1
        }
    }

    pub fn range(&self, index: u64) -> Option<ChunkRange> {
        if index >= self.chunk_count() {
            return None;
        }

        let chunk_size = self.chunk_size.get();
        let offset = index.checked_mul(chunk_size)?;
        let remaining = self.file_size.checked_sub(offset)?;
        let length = chunk_size.min(remaining);

        Some(ChunkRange {
            index,
            offset,
            length,
        })
    }
}

impl fmt::Display for ChunkLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroChunkSize => write!(formatter, "chunk size must be greater than zero"),
        }
    }
}

impl Error for ChunkLayoutError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_chunk_size() {
        let result = ChunkLayout::new(100, 0);

        assert_eq!(result, Err(ChunkLayoutError::ZeroChunkSize));
    }

    #[test]
    fn empty_file_has_no_chunks() {
        let layout = ChunkLayout::new(0, 4).unwrap();

        assert_eq!(layout.file_size(), 0);
        assert_eq!(layout.chunk_size(), 4);
        assert_eq!(layout.chunk_count(), 0);
        assert_eq!(layout.range(0), None);
    }

    #[test]
    fn file_smaller_than_chunk_has_one_partial_chunk() {
        let layout = ChunkLayout::new(2, 4).unwrap();

        assert_eq!(layout.chunk_count(), 1);
        assert_eq!(
            layout.range(0),
            Some(ChunkRange {
                index: 0,
                offset: 0,
                length: 2,
            })
        );
    }

    #[test]
    fn exact_multiple_has_no_empty_trailing_chunk() {
        let layout = ChunkLayout::new(8, 4).unwrap();

        assert_eq!(layout.chunk_count(), 2);
        assert_eq!(
            layout.range(0),
            Some(ChunkRange {
                index: 0,
                offset: 0,
                length: 4,
            })
        );
        assert_eq!(
            layout.range(1),
            Some(ChunkRange {
                index: 1,
                offset: 4,
                length: 4,
            })
        );
        assert_eq!(layout.range(2), None);
    }

    #[test]
    fn last_chunk_contains_only_remaining_bytes() {
        let layout = ChunkLayout::new(10, 4).unwrap();

        assert_eq!(layout.chunk_count(), 3);
        assert_eq!(
            layout.range(2),
            Some(ChunkRange {
                index: 2,
                offset: 8,
                length: 2,
            })
        );
    }

    #[test]
    fn chunk_larger_than_file_covers_whole_file() {
        let layout = ChunkLayout::new(10, 100).unwrap();

        assert_eq!(layout.chunk_count(), 1);
        assert_eq!(
            layout.range(0),
            Some(ChunkRange {
                index: 0,
                offset: 0,
                length: 10,
            })
        );
    }

    #[test]
    fn out_of_range_index_returns_none() {
        let layout = ChunkLayout::new(10, 4).unwrap();

        assert_eq!(layout.range(3), None);
        assert_eq!(layout.range(u64::MAX), None);
    }

    #[test]
    fn supports_u64_max_file_size_without_overflow() {
        let layout = ChunkLayout::new(u64::MAX, 4).unwrap();

        assert_eq!(layout.chunk_count(), 4_611_686_018_427_387_904);

        let last_index = layout.chunk_count() - 1;

        assert_eq!(
            layout.range(last_index),
            Some(ChunkRange {
                index: last_index,
                offset: 18_446_744_073_709_551_612,
                length: 3,
            })
        );
    }
}
