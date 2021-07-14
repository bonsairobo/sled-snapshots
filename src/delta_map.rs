use crate::{delta::Delta, delta_set::RawDeltaSet};

use sled::{
    transaction::{ConflictableTransactionResult, TransactionalTree, UnabortableTransactionError},
    IVec, Tree,
};
use std::ops::Deref;

/// A [sled::Tree] that maps each `u64` version to a set of deltas.
pub struct DeltaMap(pub Tree);

impl Deref for DeltaMap {
    type Target = Tree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Same as [DeltaMap], but used in transactions.
#[derive(Clone, Copy)]
pub struct TransactionalDeltaMap<'a>(pub &'a TransactionalTree);

impl<'a> Deref for TransactionalDeltaMap<'a> {
    type Target = TransactionalTree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> TransactionalDeltaMap<'a> {
    pub(crate) fn get_version(
        &self,
        version: u64,
    ) -> Result<Option<RawDeltaSet<IVec>>, UnabortableTransactionError> {
        self.get(version.to_be_bytes())
            .map(|result| result.map(RawDeltaSet::new))
    }

    pub(crate) fn remove_version(
        &self,
        version: u64,
    ) -> Result<Option<RawDeltaSet<IVec>>, UnabortableTransactionError> {
        self.remove(&version.to_be_bytes())
            .map(|result| result.map(RawDeltaSet::new))
    }

    /// Returns `true` iff `version` is the (unique) current version in its tree.
    pub fn is_current_version(&self, version: u64) -> ConflictableTransactionResult<bool> {
        if let Some(current_deltas) = self.get_version(version)? {
            Ok(current_deltas.is_empty())
        } else {
            // Version doesn't even exist.
            Ok(false)
        }
    }

    pub(crate) fn write_deltas<'b, B: 'b>(
        &self,
        version: u64,
        deltas: impl Iterator<Item = &'b Delta<B>>,
    ) -> ConflictableTransactionResult<()>
    where
        B: Deref<Target = [u8]>,
    {
        let mut delta_bytes = Vec::new();
        for delta in deltas {
            delta.encode(&mut delta_bytes).unwrap();
        }
        self.insert(&version.to_be_bytes(), delta_bytes)?;
        Ok(())
    }

    pub(crate) fn prepend_deltas<B>(
        &self,
        version: u64,
        deltas: RawDeltaSet<B>,
    ) -> ConflictableTransactionResult<()>
    where
        B: Deref<Target = [u8]>,
    {
        let version_deltas = self
            .get_version(version)?
            .expect("Inconsistent forest: followed pointer to missing version");

        // PERF: for each key, we could compact duplicate deltas
        let deltas_in_order = deltas.iter_deltas().chain(version_deltas.iter_deltas());

        let mut delta_bytes = Vec::new();
        for raw_delta in deltas_in_order {
            Delta::from(&raw_delta).encode(&mut delta_bytes).unwrap();
        }
        self.insert(&version.to_be_bytes(), delta_bytes)?;

        Ok(())
    }
}
