# sled-snapshots

Make versioned updates to a [sled::Tree], leaving behind incremental backups called "snapshots." Restore the state of any
snapshot.

All functionality is provided by a persistent data structure called a "snapshot forest". All operations on the forest are
transactional. See the [transactions] module for the most common operations on a snapshot forest.

The snapshot forest is implemented on top of two [sled::Tree]s. One is the [VersionForest] which stores the version [u64] of
every snapshot as a vertex in a bidirectional graph, specifically a tree. The other is the [DeltaMap], which stores a set of
deltas for each snapshot. This enables snapshots to take up relatively little space, only remembering what changes between
each version.

## Example

```rust
use sled::{Db, IVec, Transactional};
use sled_snapshots::{transactions::*, *};
use tempdir::TempDir;

let tmp = TempDir::new("sled-snapshots-demo").unwrap();
let db = sled::open(&tmp).unwrap();

// The actual application data.
let data_tree = db.open_tree("data").unwrap();
data_tree.insert(b"key0", b"value0").unwrap();

// Metadata for managing snapshots.
let (forest, delta_map) = open_snapshot_forest(&db, "snaps").unwrap();

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
        let v1 = create_snapshot(v0, forest, delta_map, data_tree, &deltas)?;

        Ok((v0, v1))
    })
    .unwrap();

// Deltas were applied.
let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>().unwrap();
assert_eq!(kvs, vec![(IVec::from(b"key1"), IVec::from(b"value1"))]);

// And we now have two snapshots/versions.
assert_eq!(forest.collect_versions(), Ok(vec![v0, v1]));

// Restore the state of v0.
(&data_tree, &*forest, &*delta_map)
    .transaction(|(data_tree, forest, delta_map)| {
        restore_snapshot(
            v1,
            v0,
            TransactionalVersionForest(forest),
            TransactionalDeltaMap(delta_map),
            data_tree,
        )
    })
    .unwrap();

// Back to the state at v0.
let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>().unwrap();
assert_eq!(kvs, vec![(IVec::from(b"key0"), IVec::from(b"value0"))]);
```

License: MIT
