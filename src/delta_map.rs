use crate::{
    delta::Delta,
    delta_node::{encode_delta_node, HeadDeltaNode, RawDeltaNode, RawHeadDeltaNode},
};

use sled::{
    transaction::{
        abort, ConflictableTransactionResult, TransactionalTree, UnabortableTransactionError,
    },
    IVec, Tree,
};
use std::ops::Deref;

// PERF: try pointing to deltas from the linked list nodes instead of serializing them inline; probably need a benchmark to
// see if it makes a difference

/// A [sled::Tree] that maps each `u64` version to a set of deltas.
///
/// # Implementation
///
/// Each set of deltas is stored as a singly linked list of deltas. It only needs to support prepending and appending.
///
/// A key in a `DeltaMap` is either a snapshot version or another globally unique ID being used as a linked list pointer. Values
/// of the map are nodes in a linked list, each node containing a sequence of deltas.
pub struct DeltaMap(pub Tree);

impl Deref for DeltaMap {
    type Target = Tree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Same as [DeltaMap] but used in transactions.
#[derive(Clone, Copy)]
pub struct TransactionalDeltaMap<'a>(pub &'a TransactionalTree);

impl<'a> Deref for TransactionalDeltaMap<'a> {
    type Target = TransactionalTree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> TransactionalDeltaMap<'a> {
    pub(crate) fn create_empty_version(
        &self,
        version: u64,
    ) -> Result<(), UnabortableTransactionError> {
        self.insert(&version.to_be_bytes(), &HeadDeltaNode::new_empty())?;
        Ok(())
    }

    pub(crate) fn create_version_with_deltas(
        &self,
        version: u64,
        deltas: Vec<Delta<IVec>>,
    ) -> Result<(), UnabortableTransactionError> {
        let tail_key = self.create_node_with_deltas(None, &deltas)?;
        self.insert(
            &version.to_be_bytes(),
            &HeadDeltaNode::new(tail_key, tail_key),
        )?;
        Ok(())
    }

    /// Returns `true` iff `version` is the (unique) current version in its tree.
    pub fn is_current_version(&self, version: u64) -> ConflictableTransactionResult<bool> {
        Ok(self.get_delta_list_head(version)?.is_none())
    }

    pub(crate) fn get_delta_list_head(
        &self,
        version: u64,
    ) -> Result<Option<RawHeadDeltaNode<IVec>>, UnabortableTransactionError> {
        self.get(version.to_be_bytes())
            .map(|result| result.map(RawHeadDeltaNode::new))
    }

    /// Removes all deltas for `version`.
    pub(crate) fn remove_version(
        &self,
        version: u64,
    ) -> Result<Option<Vec<RawDeltaNode<IVec>>>, UnabortableTransactionError> {
        if let Some(head_bytes) = self.remove(&version.to_be_bytes())? {
            let mut all_delta_nodes = Vec::new();
            let head = RawHeadDeltaNode::new(head_bytes);
            let mut maybe_next_key = head.next_key();
            while let Some(next_key) = maybe_next_key {
                let node = RawDeltaNode::new(
                    self.get(&next_key.to_be_bytes())?
                        .expect("Inconsistent linked list: followed pointer to missing key"),
                );
                maybe_next_key = node.next_key();
                all_delta_nodes.push(node);
            }
            Ok(Some(all_delta_nodes))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn append_deltas<B>(
        &self,
        version: u64,
        new_deltas: &[Delta<B>],
    ) -> ConflictableTransactionResult<()>
    where
        B: Deref<Target = [u8]>,
    {
        if new_deltas.is_empty() {
            return Ok(());
        }

        if let Some(head) = self.get_delta_list_head(version)? {
            // Write a new delta node.
            let tail_key = self.create_node_with_deltas(None, new_deltas)?;
            // Append the new node to the list.
            if let Some(tail_key) = head.tail_key() {
                let mut tail_node = self.get_list_node(tail_key)?;
                tail_node.set_next_key(Some(tail_key));
                self.insert(&tail_key.to_be_bytes(), tail_node.take_bytes())?;
            }
            let new_head_node = HeadDeltaNode::new(head.next_key().unwrap_or(tail_key), tail_key);
            self.insert(&version.to_be_bytes(), &new_head_node)?;

            Ok(())
        } else {
            // Can't append to an entry that does not exist.
            abort(())
        }
    }

    /// # Panics
    /// If `version` is missing. Internal users already followed a pointer to get to this version.
    pub(crate) fn prepend_deltas<B>(
        &self,
        version: u64,
        new_deltas: &[Delta<B>],
    ) -> ConflictableTransactionResult<()>
    where
        B: Deref<Target = [u8]>,
    {
        if new_deltas.is_empty() {
            return Ok(());
        }

        if let Some(head) = self.get_delta_list_head(version)? {
            // Write a new delta node.
            let new_next_key = self.create_node_with_deltas(head.next_key(), new_deltas)?;
            let new_head_node =
                HeadDeltaNode::new(new_next_key, head.tail_key().unwrap_or(new_next_key));
            self.insert(&version.to_be_bytes(), &new_head_node)?;

            Ok(())
        } else {
            // Can't append to an entry that does not exist.
            abort(())
        }
    }

    /// Takes some pre-existing delta nodes and modifies them so they can occupy new entries at the front of the list for
    /// `version`.
    ///
    /// # Panics
    /// If `version` is missing. Internal users know `version` must have an entry in this transaction.
    pub(crate) fn prepend_raw_delta_nodes(
        &self,
        version: u64,
        raw_delta_nodes: Vec<RawDeltaNode<IVec>>,
    ) -> ConflictableTransactionResult<()> {
        if raw_delta_nodes.is_empty() {
            return Ok(());
        }

        let version_head = self
            .get_delta_list_head(version)?
            .expect("Inconsistent forest: followed pointer to missing version");

        let (head, tail) = self.recreate_sublist(raw_delta_nodes, version_head.tail_key())?;

        let new_version_head = HeadDeltaNode::new(head, version_head.tail_key().unwrap_or(tail));
        self.insert(&version.to_be_bytes(), &new_version_head)?;

        Ok(())
    }

    /// Returns `(head, tail)` of the new list.
    fn recreate_sublist(
        &self,
        raw_delta_nodes: Vec<RawDeltaNode<IVec>>,
        tail_next_key: Option<u64>,
    ) -> ConflictableTransactionResult<(u64, u64)> {
        assert!(!raw_delta_nodes.is_empty());
        let num_nodes = raw_delta_nodes.len();

        let mut keys = Vec::with_capacity(num_nodes);
        for _ in 0..num_nodes {
            keys.push(self.generate_id()?);
        }

        for (i, mut raw_node) in raw_delta_nodes.into_iter().enumerate() {
            let next_i = i + 1;
            if next_i < num_nodes {
                raw_node.set_next_key(Some(keys[next_i]));
            } else {
                raw_node.set_next_key(tail_next_key);
            }
            self.insert(&keys[i].to_be_bytes(), raw_node.take_bytes())?;
        }

        Ok((keys[0], *keys.last().unwrap()))
    }

    fn get_list_node(
        &self,
        node_key: u64,
    ) -> Result<RawDeltaNode<IVec>, UnabortableTransactionError> {
        Ok(RawDeltaNode::new(self.get(node_key.to_be_bytes())?.expect(
            "Inconsistent linked list: followed pointer to missing key",
        )))
    }

    fn create_node_with_deltas<B>(
        &self,
        next_key: Option<u64>,
        deltas: &[Delta<B>],
    ) -> Result<u64, UnabortableTransactionError>
    where
        B: Deref<Target = [u8]>,
    {
        let deltas_key = self.generate_id()?;
        self.insert(
            &deltas_key.to_be_bytes(),
            encode_delta_node(next_key, deltas),
        )?;
        Ok(deltas_key)
    }
}
