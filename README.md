# sled-snapshots

Make versioned updates to a [sled::Tree], leaving behind incremental backups called "snapshots."

## Usage Overview

All functionality is provided by a persistent data structure called a "snapshot forest". Each tree in the forest represents
multiple branching timelines of changes made to a single [sled::Tree] called the "data tree". Create a new tree in the
forest with [create_snapshot_tree](crate::transactions::create_snapshot_tree).

You are free to provide any [sled::Tree] as the root version of a tree, but once more snapshots are created, you must use
one of the functions in the [transactions] module to update your data tree; **manual updates to your data tree void the
warranty** (your data tree will get out of sync with the snapshot tree).

Each snapshot tree has a "current" version which indicates the current state of your data tree. By calling
[set_current_version](crate::transactions::set_current_version), you can restore the state of your data tree to that of any
snapshot. If the current version has no children, you can modify it as much as you want with
[modify_current_leaf_snapshot](crate::transactions::modify_current_leaf_snapshot). Once you want to freeze the state of the
current version, create a child snapshot with [create_child_snapshot](crate::transactions::create_child_snapshot).

All operations on the forest are transactional. See the [transactions] module for all supported operations on a snapshot
forest.

## Implementation

The snapshot forest is implemented on top of two [sled::Tree]s. One is the [VersionForest] which stores the version [u64] of
every snapshot as a vertex in a bidirectional graph, specifically a tree. The other is the [DeltaMap], which stores a set of
deltas for each snapshot. This enables snapshots to take up relatively little space, only remembering what changes between
each version.

## Example

```rust
use sled::{IVec, Transactional};
use sled_snapshots::{transactions::*, *};

let config = sled::Config::new().temporary(true);
let db = config.open()?;

// The actual application data.
let data_tree = db.open_tree("data")?;
data_tree.insert(b"key0", b"value0")?;

// Metadata for managing snapshots.
let (forest, delta_map) = open_snapshot_forest(&db, "snaps")?;

let (v0, v1) = (&data_tree, &*forest, &*delta_map)
    .transaction(|(data_tree, forest, delta_map)| {
        let forest = TransactionalVersionForest(forest);
        let delta_map = TransactionalDeltaMap(delta_map);

        // We need a new snapshot tree specifically for `data_map`.
        let v0 = create_snapshot_tree(forest)?;

        // All updates to `data_tree` (after v0) must be done by applying `Delta`s
        // via `create_snapshot`, `modify_current_leaf_snapshot`, or `modify_leaf_snapshot`.
        let deltas = [
            Delta::Remove(IVec::from(b"key0")),
            Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1")),
        ];
        let v1 = create_child_snapshot_with_deltas(v0, forest, delta_map, data_tree, &deltas)?;

        Ok((v0, v1))
    })?;

// Deltas were applied.
let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>()?;
assert_eq!(kvs, vec![(IVec::from(b"key1"), IVec::from(b"value1"))]);

// And we now have two snapshots/versions.
assert_eq!(forest.collect_versions(), Ok(vec![v0, v1]));

// Restore the state of v0.
(&data_tree, &*forest, &*delta_map)
    .transaction(|(data_tree, forest, delta_map)| {
        set_current_version(
            v1,
            v0,
            TransactionalVersionForest(forest),
            TransactionalDeltaMap(delta_map),
            data_tree,
        )
    })?;

// Back to the state at v0.
let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>()?;
assert_eq!(kvs, vec![(IVec::from(b"key0"), IVec::from(b"value0"))]);
```

License: MIT
