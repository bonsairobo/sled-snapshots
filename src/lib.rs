//! Make versioned updates to a [sled::Tree] called "snapshots." Restore the state of any snapshot.
//!
//! All functionality is provided by a persistent data structure called a "snapshot forest". All operations on the forest are
//! transactional. See the [transactions] module for the most common operations on a snapshot forest.
//!
//! The snapshot forest is implemented on top of two [sled::Tree]s. One is the [VersionForest] which stores the version [u64] of
//! every snapshot as a vertex in a bidirectional graph, specifically a tree. The other is the [DeltaMap], which stores a set of
//! deltas for each snapshot. This enables snapshots to take up relatively little space, only remembering what changes between
//! each version.
//!
//! # Example
//!
//! ```rust
//! use sled::{Db, Transactional, IVec};
//! use sled_snapshots::{*, transactions::*};
//! use tempdir::TempDir;
//!
//! let tmp = TempDir::new("sled-snapshots-demo").unwrap();
//! let db = sled::open(&tmp).unwrap();
//!
//! // The actual application data.
//! let data_tree = db.open_tree("data").unwrap();
//! // Metadata for managing snapshots.
//! let (forest, delta_map) = open_snapshot_forest(&db, "snaps").unwrap();
//!
//! let (root_version, new_version) = (&data_tree, &*forest, &*delta_map)
//!     .transaction(|(data_tree, forest, delta_map)| {
//!         let forest = TransactionalVersionForest(forest);
//!         let delta_map = TransactionalDeltaMap(delta_map);
//!
//!         // We need a new snapshot tree specifically for `data_map`.
//!         let root_version = create_snapshot_tree(forest, delta_map)?;
//!
//!         // All updates to `data_tree` (after the root version) must be done by applying `Delta`s via `create_snapshot`.
//!         let deltas = [
//!             Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1")),
//!             Delta::Insert(IVec::from(b"key2"), IVec::from(b"value2")),
//!         ];
//!         let new_version =
//!             create_snapshot(root_version, forest, delta_map, data_tree, &deltas)?;
//!
//!         Ok((root_version, new_version))
//!     })
//!     .unwrap();
//!
//! // Deltas were applied.
//! let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>().unwrap();
//! assert_eq!(
//!     kvs,
//!     vec![
//!         (IVec::from(b"key1"), IVec::from(b"value1")),
//!         (IVec::from(b"key2"), IVec::from(b"value2")),
//!     ]
//! );
//!
//! // And we now have two snapshots/versions.
//! assert_eq!(forest.collect_versions(), Ok(vec![root_version, new_version]));
//!
//! // Restore the state of the root snapshot.
//! (&data_tree, &*forest, &*delta_map)
//!     .transaction(|(data_tree, forest, delta_map)| {
//!         restore_snapshot(
//!             new_version,
//!             root_version,
//!             TransactionalVersionForest(forest),
//!             TransactionalDeltaMap(delta_map),
//!             data_tree
//!         )
//!     })
//!     .unwrap();
//!
//! // Deltas were reversed.
//! assert!(data_tree.is_empty());
//! ```

use sled::Db;

mod delta;
mod delta_map;
mod delta_set;
mod version_forest;
mod version_node;

pub mod transactions;

pub use delta::Delta;
pub use delta_map::*;
pub use version_forest::*;

/// Opens two `sled::Tree`s in `db` which represent a "snapshot forest."
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
