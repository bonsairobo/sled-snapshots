use crate::{delta_set::RawDeltaSet, u64_from_be_slice, version_node::NULL_VERSION, Delta};

use sled::IVec;
use std::io;
use std::mem;
use std::ops::{Deref, DerefMut, Range, RangeFrom};

/// Always the first node in a delta list. Doesn't contain any deltas.
#[derive(Clone)]
pub struct HeadDeltaNode {
    /// `None` means there are no deltas in this list.
    pub next_key: Option<u64>,
    /// Used for appending. Only valid when `next_key` is `Some`.
    pub tail_key: u64,
}

impl HeadDeltaNode {
    pub fn new_empty() -> Self {
        Self {
            next_key: None,
            tail_key: NULL_VERSION,
        }
    }

    pub fn new(next_key: u64, tail_key: u64) -> Self {
        Self {
            next_key: Some(next_key),
            tail_key,
        }
    }

    pub fn encode(&self, mut writer: impl io::Write) -> io::Result<()> {
        self.encode_next_key(&mut writer)?;
        self.encode_tail_key(writer)
    }

    pub fn encode_next_key(&self, writer: impl io::Write) -> io::Result<()> {
        encode_next_key(self.next_key, writer)
    }

    pub fn encode_tail_key(&self, mut writer: impl io::Write) -> io::Result<()> {
        writer.write_all(&self.tail_key.to_be_bytes())
    }

    pub fn encoded_size(&self) -> usize {
        2 * mem::size_of::<u64>()
    }
}

impl From<&HeadDeltaNode> for IVec {
    fn from(node: &HeadDeltaNode) -> Self {
        let mut bytes = Vec::with_capacity(node.encoded_size());
        node.encode(&mut bytes).unwrap();
        bytes.into()
    }
}

/// A wrapper around a byte slice used for decoding a `HeadDeltaNode`.
///
/// The on-disk encoding is:
///
/// 0. `next_key`: `8` bytes (big endian u64)
/// 1. `tail_key`: `8` bytes (big endian u64)
#[derive(Clone)]
pub struct RawHeadDeltaNode<B> {
    bytes: B,
}

impl<B> RawHeadDeltaNode<B>
where
    B: Deref<Target = [u8]>,
{
    pub fn new(bytes: B) -> Self {
        Self { bytes }
    }

    pub fn next_key(&self) -> Option<u64> {
        decode_next_key(&self.bytes)
    }

    pub fn tail_key(&self) -> Option<u64> {
        // Tail key is only valid if there is at least a next key.
        self.next_key().map(|_| self.raw_tail_key())
    }

    fn raw_tail_key(&self) -> u64 {
        u64_from_be_slice(&self.bytes[tail_key_range()])
    }
}

pub fn encode_delta_node<B>(next_key: Option<u64>, deltas: &[Delta<B>]) -> IVec
where
    B: Deref<Target = [u8]>,
{
    let mut node_bytes = Vec::new();
    encode_next_key(next_key, &mut node_bytes).unwrap();
    for delta in deltas.iter() {
        delta.encode(&mut node_bytes).unwrap();
    }
    node_bytes.into()
}

impl<B> RawDeltaNode<B>
where
    B: DerefMut<Target = [u8]>,
{
    pub fn set_next_key(&mut self, next_key: Option<u64>) {
        encode_next_key(next_key, self.bytes.deref_mut()).unwrap()
    }
}

/// A wrapper around a byte slice used for decoding a `HeadDeltaNode`.
///
/// The on-disk encoding is:
///
/// 0. `next_key`: `8` bytes (big endian u64)
/// 1. `deltas`: [RawDeltaSet](crate::delta_set::RawDeltaSet)
#[derive(Clone)]
pub struct RawDeltaNode<B> {
    bytes: B,
}

impl<B> RawDeltaNode<B>
where
    B: Deref<Target = [u8]>,
{
    pub fn new(bytes: B) -> Self {
        Self { bytes }
    }

    pub fn take_bytes(self) -> B {
        self.bytes
    }

    pub fn next_key(&self) -> Option<u64> {
        decode_next_key(&self.bytes)
    }

    pub fn deltas(&self) -> RawDeltaSet<&[u8]> {
        RawDeltaSet::new(&self.bytes[delta_set_range()])
    }
}

fn encode_next_key(next_key: Option<u64>, mut writer: impl io::Write) -> io::Result<()> {
    if let Some(next_key) = next_key {
        writer.write_all(&next_key.to_be_bytes())
    } else {
        writer.write_all(&NULL_VERSION.to_be_bytes())
    }
}

fn decode_next_key(bytes: &[u8]) -> Option<u64> {
    let version = u64_from_be_slice(&bytes[next_key_range()]);
    if version == NULL_VERSION {
        None
    } else {
        Some(version)
    }
}

const fn next_key_range() -> Range<usize> {
    0..mem::size_of::<u64>()
}

const fn tail_key_range() -> Range<usize> {
    let start = next_key_range().end;
    start..start + mem::size_of::<u64>()
}

const fn delta_set_range() -> RangeFrom<usize> {
    next_key_range().end..
}
