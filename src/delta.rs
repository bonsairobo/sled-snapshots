use crate::usize_from_be_slice;

use std::io;
use std::mem;
use std::ops::{Deref, Range};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Delta<B> {
    /// Insert `(key, value)`.
    Insert(B, B),
    /// Remove `key`.
    Remove(B),
}

impl<B> Delta<B>
where
    B: Deref<Target = [u8]>,
{
    pub fn encode(&self, writer: &mut impl io::Write) -> io::Result<()> {
        match self {
            Delta::Insert(key, value) => {
                writer.write_all(&key.len().to_be_bytes())?;
                writer.write_all(&value.len().to_be_bytes())?;
                writer.write_all(&key)?;
                writer.write_all(&value)?;
            }
            Delta::Remove(key) => {
                writer.write_all(&key.len().to_be_bytes())?;
                writer.write_all(&0u64.to_be_bytes())?; // 0 num_value_bytes implies Remove
                writer.write_all(&key)?;
            }
        }
        Ok(())
    }

    pub fn encoded_size(&self) -> usize {
        match self {
            Delta::Insert(key, value) => 2 * mem::size_of::<u64>() + key.len() + value.len(),
            Delta::Remove(key) => 2 * mem::size_of::<u64>() + key.len(),
        }
    }

    pub fn map<T>(&self, f: impl Fn(&B) -> T) -> Delta<T> {
        match self {
            Delta::Insert(key, value) => Delta::Insert(f(key), f(value)),
            Delta::Remove(key) => Delta::Remove(f(key)),
        }
    }
}

impl<'a, B> From<&'a RawDelta<B>> for Delta<&'a [u8]>
where
    B: Deref<Target = [u8]>,
{
    fn from(raw: &'a RawDelta<B>) -> Self {
        if raw.num_value_bytes() == 0 {
            Delta::Remove(raw.key_slice())
        } else {
            Delta::Insert(raw.key_slice(), raw.value_slice())
        }
    }
}

/// A wrapper around a byte slice used for decoding a `Delta`.
///
/// The on-disk encoding is:
///
/// 0. `num_key_bytes`: `8` bytes (big endian u64)
/// 1. `num_value_bytes`: `8` bytes (big endian u64)
/// 2. `key_bytes`: `num_key_bytes` bytes (arbitrary)
/// 3. `value_bytes`: `num_value_bytes` bytes (arbitrary)
///
/// If `num_value_bytes == 0`, then this is a `Delta::Remove`.
#[derive(Clone)]
pub struct RawDelta<B> {
    bytes: B,
}

impl<B> RawDelta<B>
where
    B: Deref<Target = [u8]>,
{
    pub fn new(bytes: B) -> Self {
        Self { bytes }
    }

    /// Returns the key value as a byte slice.
    pub fn key_slice(&self) -> &[u8] {
        &self.bytes[self.key_range()]
    }

    fn value_slice(&self) -> &[u8] {
        &self.bytes[self.value_range()]
    }

    fn key_range(&self) -> Range<usize> {
        let start = num_value_bytes_range().end;
        start..start + self.num_key_bytes()
    }

    fn value_range(&self) -> Range<usize> {
        let start = self.key_range().end;
        start..start + self.num_value_bytes()
    }

    pub fn range(&self) -> Range<usize> {
        let end = num_value_bytes_range().end + self.num_key_bytes() + self.num_value_bytes();
        0..end
    }

    fn num_key_bytes(&self) -> usize {
        usize_from_be_slice(&self.bytes[num_key_bytes_range()])
    }

    fn num_value_bytes(&self) -> usize {
        usize_from_be_slice(&self.bytes[num_value_bytes_range()])
    }
}

const fn num_key_bytes_range() -> Range<usize> {
    0..mem::size_of::<u64>()
}

const fn num_value_bytes_range() -> Range<usize> {
    let start = num_key_bytes_range().end;
    start..start + mem::size_of::<u64>()
}
