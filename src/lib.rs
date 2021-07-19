//! Make versioned updates to a [sled::Tree], leaving behind incremental backups called "snapshots."
//!
//! # Usage Overview
//!
//! All functionality is provided by a persistent data structure called a "snapshot forest". Each tree in the forest represents
//! multiple branching timelines of changes made to a single [sled::Tree] called the "data tree". Create a new tree in the
//! forest with [create_snapshot_tree](crate::transactions::create_snapshot_tree).
//!
//! You are free to provide any [sled::Tree] as the root version of a tree, but once more snapshots are created, you must use
//! one of the functions in the [transactions] module to update your data tree; **manual updates to your data tree void the
//! warranty** (your data tree will get out of sync with the snapshot tree).
//!
//! Each snapshot tree has a "current" version which indicates the current state of your data tree. By calling
//! [set_current_version](crate::transactions::set_current_version), you can restore the state of your data tree to that of any
//! snapshot. If the current version has no children, you can modify it as much as you want with
//! [modify_current_leaf_snapshot](crate::transactions::modify_current_leaf_snapshot). Once you want to freeze the state of the
//! current version, create a child snapshot with [create_child_snapshot](crate::transactions::create_child_snapshot).
//!
//! All operations on the forest are transactional. See the [transactions] module for all supported operations on a snapshot
//! forest. Note that none of these operations will flush for you!
//!
//! # Implementation
//!
//! The snapshot forest is implemented on top of two [sled::Tree]s. One is the [VersionForest] which stores the version [u64] of
//! every snapshot as a vertex in a bidirectional graph, specifically a tree. The other is the [DeltaMap], which stores a set of
//! deltas for each snapshot. This enables snapshots to take up relatively little space, only remembering what changes between
//! each version.
//!
//! # Example
//!
//! ```rust
//! # fn run_demo() -> sled::transaction::TransactionResult<()> {
//! use sled::{IVec, Transactional};
//! use sled_snapshots::{transactions::*, *};
//!
//! let config = sled::Config::new().temporary(true);
//! let db = config.open()?;
//!
//! let data_tree = db.open_tree("data")?;
//! data_tree.insert(b"key0", b"value0")?;
//!
//! let (forest, delta_map) = open_snapshot_forest(&db, "snaps")?;
//!
//! let (v0, v1) = (&data_tree, &*forest, &*delta_map)
//!     .transaction(|(data_tree, forest, delta_map)| {
//!         let forest = TransactionalVersionForest(forest);
//!         let delta_map = TransactionalDeltaMap(delta_map);
//!
//!         // We need a new snapshot tree specifically for `data_map`.
//!         let v0 = create_snapshot_tree(forest)?;
//!
//!         let deltas = [
//!             Delta::Remove(IVec::from(b"key0")),
//!             Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1")),
//!         ];
//!         let v1 = create_child_snapshot_with_deltas(v0, forest, delta_map, data_tree, &deltas)?;
//!
//!         Ok((v0, v1))
//!     })?;
//!
//! // Deltas were applied.
//! let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>()?;
//! assert_eq!(kvs, vec![(IVec::from(b"key1"), IVec::from(b"value1"))]);
//!
//! // And we now have two snapshots/versions.
//! assert_eq!(forest.collect_versions(), Ok(vec![v0, v1]));
//!
//! // Restore the state of v0.
//! (&data_tree, &*forest, &*delta_map)
//!     .transaction(|(data_tree, forest, delta_map)| {
//!         set_current_version(
//!             v1,
//!             v0,
//!             TransactionalVersionForest(forest),
//!             TransactionalDeltaMap(delta_map),
//!             data_tree,
//!         )
//!     })?;
//!
//! // Back to the state at v0.
//! let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>()?;
//! assert_eq!(kvs, vec![(IVec::from(b"key0"), IVec::from(b"value0"))]);
//! # Ok(()) }
//! # run_demo().unwrap();
//! ```

use sled::Db;

mod delta;
mod delta_map;
mod delta_node;
mod delta_set;
mod version_forest;
mod version_node;

pub mod transactions;

pub use delta::Delta;
pub use delta_map::*;
pub use version_forest::*;

/// Opens two `sled::Tree`s in `db` which represent a "snapshot forest."
///
/// This doesn't actually insert anything into the `sled::Tree`s. It's just for convenience and a little extra type safety.
///
/// The `VersionForest` will be called `"${name}-versions"`, and it stores the version forest, i.e. a set of versions where each
/// version is a node in some tree. The `DeltaMap` will be called `"${name}-deltas"`, and it stores a set of deltas for each
/// version.
pub fn open_snapshot_forest(db: &Db, name: &str) -> sled::Result<(VersionForest, DeltaMap)> {
    let version_forest = db.open_tree(format!("{}-versions", name))?;
    let delta_map = db.open_tree(format!("{}-deltas", name))?;
    Ok((VersionForest(version_forest), DeltaMap(delta_map)))
}

fn u64_from_be_slice(s: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(s);
    u64::from_be_bytes(bytes)
}

fn usize_from_be_slice(s: &[u8]) -> usize {
    let x = u64_from_be_slice(s);
    assert!(x <= usize::MAX as u64);
    x as usize
}
