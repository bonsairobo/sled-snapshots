use crate::{u64_from_be_slice, usize_from_be_slice};

use sled::IVec;
use std::io;
use std::mem;
use std::ops::{Deref, Range};

pub struct VersionNode {
    pub parent: Option<u64>,
    pub children: Vec<u64>,
}

impl VersionNode {
    pub fn new_orphan() -> Self {
        Self {
            parent: None,
            children: Vec::new(),
        }
    }

    pub fn new_with_parent(parent: u64) -> Self {
        assert_ne!(parent, NULL_VERSION);

        Self {
            parent: Some(parent),
            children: Vec::new(),
        }
    }

    pub fn new_maybe_with_parent(parent: Option<u64>) -> Self {
        if let Some(parent) = parent {
            Self::new_with_parent(parent)
        } else {
            Self::new_orphan()
        }
    }

    pub fn encode(&self, writer: &mut impl io::Write) -> io::Result<()> {
        self.encode_parent(writer)?;
        self.encode_children(writer)
    }

    pub fn encode_parent(&self, writer: &mut impl io::Write) -> io::Result<()> {
        writer.write_all(&self.parent_be_bytes())
    }

    pub fn parent_be_bytes(&self) -> [u8; 8] {
        self.parent.unwrap_or(NULL_VERSION).to_be_bytes()
    }

    pub fn encode_children(&self, writer: &mut impl io::Write) -> io::Result<()> {
        writer.write_all(&self.children.len().to_be_bytes())?;
        for child in self.children.iter() {
            writer.write_all(&child.to_be_bytes())?;
        }
        Ok(())
    }

    pub fn encoded_size(&self) -> usize {
        mem::size_of::<u64>() * (2 + self.children.len())
    }
}

impl From<&VersionNode> for IVec {
    fn from(node: &VersionNode) -> Self {
        let mut bytes = Vec::with_capacity(node.encoded_size());
        node.encode(&mut bytes).unwrap();
        bytes.into()
    }
}

impl<B> From<RawVersionNode<B>> for VersionNode
where
    B: Deref<Target = [u8]>,
{
    fn from(raw_node: RawVersionNode<B>) -> Self {
        Self {
            parent: raw_node.parent(),
            children: raw_node.iter_children().collect(),
        }
    }
}

/// A wrapper around a byte slice used for decoding a version node.
///
/// The on-disk encoding is:
///
/// 0. `parent`: `8` bytes (big endian u64)
/// 1. `num_children`: `8` bytes (big endian u64)
/// 2. `children`: `num_children * 8` bytes (sequence of big endian u64)
///
/// `parent == NULL_VERSION` means the snapshot is an orphan, i.e. it is the first version in this tree.
#[derive(Clone)]
pub struct RawVersionNode<B> {
    bytes: B,
}

impl<B> RawVersionNode<B>
where
    B: Deref<Target = [u8]>,
{
    pub fn new(bytes: B) -> Self {
        Self { bytes }
    }

    /// The parent version of this snapshot, i.e. the version that came immediately before this one.
    pub fn parent(&self) -> Option<u64> {
        let parent = u64_from_be_slice(&self.bytes[parent_range()]);
        if parent == NULL_VERSION {
            None
        } else {
            Some(parent)
        }
    }

    /// Needs to be a `usize` for use as an index.
    ///
    /// # Panics
    /// If the encoded value is greater than `usize::MAX`. This can only happen on a 32-bit target.
    pub fn num_children(&self) -> usize {
        usize_from_be_slice(&self.bytes[num_children_range()])
    }

    /// Returns an iterator over all children versions.
    pub fn iter_children(&self) -> impl '_ + Iterator<Item = u64> {
        self.bytes[self.children_range()]
            .chunks(mem::size_of::<u64>())
            .map(u64_from_be_slice)
    }

    pub fn range(&self) -> Range<usize> {
        0..self.children_range().end
    }

    fn children_range(&self) -> Range<usize> {
        let start = num_children_range().end;
        start..start + self.num_children() * mem::size_of::<u64>()
    }
}

const fn parent_range() -> Range<usize> {
    0..mem::size_of::<u64>()
}

const fn num_children_range() -> Range<usize> {
    let start = parent_range().end;
    start..start + mem::size_of::<u64>()
}

/// A version that's never valid because it has a special purpose internally.
pub const NULL_VERSION: u64 = u64::MAX;
