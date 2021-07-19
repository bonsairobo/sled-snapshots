//! Each function in this module is implemented as a single `sled` transaction.

use crate::{delta::Delta, TransactionalDeltaMap, TransactionalVersionForest, VersionPath};

use itertools::Itertools;
use sled::{
    transaction::{
        abort, ConflictableTransactionResult, TransactionalTree, UnabortableTransactionError,
    },
    IVec,
};

// TODO: for versioning multiple trees at a time, we can have another "data tree" that actually stores sets of versions of other
// data trees

/// Creates a new tree in the snapshot forest and returns the root version.
///
/// The created root version automatically becomes the current version, as it is the only version in its tree.
///
/// # Panics
/// If `sled` runs out of IDs.
pub fn create_snapshot_tree(
    forest: TransactionalVersionForest,
) -> ConflictableTransactionResult<u64> {
    forest.create_version(None)
}

/// Creates a child of `parent_version` and returns the version. The new snapshot is identical to the parent, i.e. there are no
/// deltas yet.
///
/// This freezes the parent snapshot so no further changes can be made to it, and the corresponding data tree can always be
/// restored to that state until the snapshot is deleted.
///
/// For convenience, if you set `make_current = true`, then the new snapshot will be made the current version. This only works
/// if `parent_version` is already the current version, otherwise the transaction is aborted.
///
/// If `parent_version` does not exist, then the transaction is aborted.
///
/// # Panics
/// If `sled` runs out of IDs.
pub fn create_child_snapshot(
    parent_version: u64,
    make_current: bool,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
) -> ConflictableTransactionResult<u64> {
    if make_current && !delta_map.is_current_version(parent_version)? {
        return abort(());
    }

    let child_version = forest.create_version(Some(parent_version))?;

    if make_current {
        delta_map.create_empty_version(parent_version)?;
    } else {
        delta_map.create_empty_version(child_version)?;
    }

    Ok(child_version)
}

/// Append deltas to a non-current leaf snapshot.
///
/// The snapshot must be a leaf in the tree in order to preserve the state of other snapshots. The snapshot must not be current
/// because then the data tree would get out of sync. If `version` is the current version or it is not a leaf, then the
/// transaction is aborted.
pub fn modify_leaf_snapshot(
    version: u64,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
    deltas: &[Delta<&[u8]>],
) -> ConflictableTransactionResult<()> {
    if !forest.is_leaf(version)? || delta_map.is_current_version(version)? {
        return abort(());
    }
    delta_map.append_deltas(version, deltas)
}

/// Applies `deltas` directly to `data_tree` at the current version.
///
/// The current version must be a leaf in the tree in order to preserve the state of other snapshots. If `current_version` is
/// not a leaf or it is not actually a current version (as tracked by the containing snapshot tree), then the transaction is
/// aborted.
///
/// # Panics
/// - If `current_version` is `NULL_VERSION` or `sled` runs out of IDs.
///
/// # Implementation Details
///
/// This involves a single transaction which:
/// 1. Replaces data in `data_tree` with the key-value pairs from `deltas`, remembering any old values.
/// 2. Writes the old values into the previously empty delta set for `current_version`.
/// 3. Creates a new empty version node as a child of `current_version`.
pub fn modify_current_leaf_snapshot(
    current_version: u64,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
    data_tree: &TransactionalTree,
    deltas: &[Delta<IVec>],
) -> ConflictableTransactionResult<()> {
    if !forest.is_leaf(current_version)? || !delta_map.is_current_version(current_version)? {
        return abort(());
    }
    if let Some(parent_version) = forest.parent_of(current_version)? {
        let mut reverse_deltas = Vec::new();
        apply_deltas(deltas.iter().cloned(), data_tree, &mut reverse_deltas)?;
        reverse_deltas.reverse();
        delta_map.prepend_deltas(parent_version, &reverse_deltas)?;
    }

    Ok(())
}

/// This is equivalent to calling `create_child_snapshot` followed by `modify_current_leaf_snapshot`.
pub fn create_child_snapshot_with_deltas(
    current_version: u64,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
    data_tree: &TransactionalTree,
    deltas: &[Delta<IVec>],
) -> ConflictableTransactionResult<u64> {
    if !delta_map.is_current_version(current_version)? {
        return abort(());
    }

    let child_version = forest.create_version(Some(current_version))?;

    let mut reverse_deltas = Vec::new();
    apply_deltas(deltas.iter().cloned(), data_tree, &mut reverse_deltas)?;
    reverse_deltas.reverse();
    delta_map.create_version_with_deltas(current_version, reverse_deltas)?;

    Ok(child_version)
}

/// Given a `data_tree` at `current_version`, restores `data_tree` to the state of the `target_version` snapshot.
///
/// Aborts the transaction if:
/// - `current_version` is not actually the current version (as tracked by the snapshot trees)
/// - `current_version` does not exist
/// - `target_version` does not exist
///
/// # Panics
/// If no path exists between `current_version` and `target_version`. This is only possible if these versions belong to
/// different trees in the forest.
///
/// # Implementation Details
///
/// Once the current version is found, we need to trace the version path between `current_version` and `target_version`. For
/// example:
///
/// ```text
///            v1
///          /    \
///       v2        v3
///       ^          ^
///    current     target
/// ```
///
/// We first transitions from `v2` to `v1`, then from `v1` to `v3`. Each step `A -> B`, involves:
///
/// 1. Pops all deltas from the snapshot at `B`.
/// 2. Applies those deltas to `data_tree`, keeping the old values as reverse deltas.
/// 3. Inserts the reverse deltas from `data_tree` into the previously empty snapshot at `A`.
pub fn set_current_version(
    current_version: u64,
    target_version: u64,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
    data_tree: &TransactionalTree,
) -> ConflictableTransactionResult<()> {
    // Make sure this is actually the current version.
    if !delta_map.is_current_version(current_version)? {
        return abort(());
    }

    match forest.find_path_between_versions(current_version, target_version)? {
        VersionPath::PathExists(path) => {
            for (v1, v2) in path.into_iter().tuple_windows() {
                nudge_version(v1, v2, delta_map, data_tree)?;
            }
        }
        VersionPath::NoPathExists => {
            panic!(
                "No path exists between versions: current={} target={}",
                current_version, target_version
            );
        }
    }

    Ok(())
}

fn nudge_version(
    current_version: u64,
    target_version: u64,
    delta_map: TransactionalDeltaMap,
    data_tree: &TransactionalTree,
) -> ConflictableTransactionResult<()> {
    // Gather up all of the raw deltas in the target version.
    let raw_delta_nodes = delta_map
        .remove_version(target_version)?
        .expect("Version already found in transaction");
    let mut deltas = Vec::new();
    for node in raw_delta_nodes.iter() {
        let delta_set = node.deltas();
        for delta in delta_set.iter_deltas() {
            deltas.push(delta);
        }
    }

    let mut reverse_deltas = Vec::new();
    apply_deltas(
        deltas.iter().map(|raw| Delta::<IVec>::from(raw)),
        data_tree,
        &mut reverse_deltas,
    )?;
    reverse_deltas.reverse();
    delta_map.create_version_with_deltas(current_version, reverse_deltas)?;
    Ok(())
}

/// Applies `deltas` to `data_tree` and adds the corresponding reverse deltas to `reverse_deltas`. Note that this only reverses
/// each individual delta, but the order of the deltas stays the same. You may need to reverse the order of the deltas depending
/// on the situation.
fn apply_deltas(
    deltas: impl Iterator<Item = Delta<IVec>>,
    data_tree: &TransactionalTree,
    reverse_deltas: &mut Vec<Delta<IVec>>,
) -> Result<(), UnabortableTransactionError> {
    for delta in deltas {
        let (key, old_value) = match delta {
            Delta::Insert(key, value) => (key.clone(), data_tree.insert(key, value)?),
            Delta::Remove(key) => (key.clone(), data_tree.remove(key)?),
        };
        if let Some(old_value) = old_value {
            reverse_deltas.push(Delta::Insert(key.clone(), old_value));
        } else {
            reverse_deltas.push(Delta::Remove(key.clone()));
        }
    }
    Ok(())
}

/// Deletes the snapshot at `version`.
///
/// Deleting the current version or any root version is forbidden; any attempt to do so will abort the transaction. If
/// necessary, you can delete an entire snapshot tree with `delete_snapshot_tree`.
///
/// # Implementation Details
///
/// The deltas from `version` might need to be preserved somewhere, and we need a process to determine which version(s) will
/// receive those deltas. We will argue that, if the deltas must be preserved, they should be moved "away" from the current
/// version.
///
/// Consider a simple algebra where `A * deltas(B)` means "`A` after applying the deltas from version `B`". This is a
/// non-commutative operation.
///
/// Suppose that `C != D != root` and there is some version `V` between the current version `C` and the deleted version `D`,
/// like so: `C <--> V <--> D`. (The direction of the edges does not matter for this argument). We cannot move `deltas(D)` to
/// `V`, because `V = C * deltas(V) != C * deltas(V) * deltas(D)`. There are similar arguments where any sequence of versions
/// takes the place of `V`. Therefore we either delete `deltas(D)` entirely or move them away from `C`.
///
/// There are two cases to consider:
///
/// 1. if `C` is an ancestor of `D`
///     - Move `deltas(D)` to all children of `D`
///     - Deltas are dropped if `D` has no children
/// 2. else `C` is a descendent of `D`
///     - Move `deltas(D)` to the parent of `D`
pub fn delete_snapshot(
    version: u64,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
) -> ConflictableTransactionResult<()> {
    // Make sure we don't delete the current version.
    if delta_map.is_current_version(version)? {
        return abort(());
    }

    // See if the current version is an ancestor.
    let mut current_is_ancestor = false;
    let path_to_root = forest.find_path_to_root(version)?;
    for &v in &path_to_root[1..] {
        if delta_map.is_current_version(v)? {
            current_is_ancestor = true;
            break;
        }
    }

    // Delete the version.
    let rm_node = forest
        .remove_version(version)?
        .expect("Version already found in transaction");

    // Move the deltas.
    let raw_delta_nodes = delta_map
        .remove_version(version)?
        .expect("Version already found in transaction");

    if current_is_ancestor {
        // Move the deltas to every child.
        let node_clones = vec![raw_delta_nodes; rm_node.children.len()];
        for (&child, raw_delta_nodes) in rm_node.children.iter().zip(node_clones.into_iter()) {
            delta_map.prepend_raw_delta_nodes(child, raw_delta_nodes)?;
        }
    } else {
        // Move the deltas to the parent.
        delta_map.prepend_raw_delta_nodes(
            rm_node.parent.expect("Deleting a root is forbidden"),
            raw_delta_nodes,
        )?;
    }

    Ok(())
}

/// Deletes `root` snapshot and all snapshots that have `root` as an ancestor.
pub fn delete_snapshot_tree(
    root: u64,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
) -> ConflictableTransactionResult<()> {
    forest.delete_tree(root, |deleted_version| {
        delta_map.remove_version(deleted_version)?;
        Ok(())
    })
}

// ████████╗███████╗███████╗████████╗
// ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝
//    ██║   █████╗  ███████╗   ██║
//    ██║   ██╔══╝  ╚════██║   ██║
//    ██║   ███████╗███████║   ██║
//    ╚═╝   ╚══════╝╚══════╝   ╚═╝
#[cfg(test)]
mod test {
    use super::*;
    use crate::{open_snapshot_forest, DeltaMap, VersionForest};

    use sled::{transaction::TransactionError, Transactional};

    #[test]
    fn initial_snapshot_tree_has_only_v0() {
        let fixture = Fixture::open();
        let (forest, _delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();

        let v0 = forest
            .transaction(|forest| create_snapshot_tree(TransactionalVersionForest(forest)))
            .unwrap();

        assert_eq!(forest.collect_versions(), Ok(vec![v0]));
    }

    #[test]
    fn delete_current_version_aborts() {
        let fixture = Fixture::open();
        let (forest, delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();
        let data_tree = fixture.db.open_tree("data").unwrap();

        let result =
            (&data_tree, &*forest, &*delta_map).transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                let v0 = create_snapshot_tree(forest)?;

                let deltas = [Delta::Insert(IVec::from(b"key"), IVec::from(b"value"))];
                let v1 =
                    create_child_snapshot_with_deltas(v0, forest, delta_map, data_tree, &deltas)?;

                delete_snapshot(v1, forest, delta_map)
            });

        assert_eq!(result, Err(TransactionError::Abort(())));
    }

    #[test]
    fn set_current_version_reverses_noncommutative_deltas_same_key() {
        let fixture = Fixture::open();
        let (forest, delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();
        let data_tree = fixture.db.open_tree("data").unwrap();

        let (v0, v1) = (&data_tree, &*forest, &*delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                let v0 = create_snapshot_tree(forest)?;

                let deltas = [
                    Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1")),
                    Delta::Remove(IVec::from(b"key1")),
                ];
                let v1 =
                    create_child_snapshot_with_deltas(v0, forest, delta_map, data_tree, &deltas)?;

                Ok((v0, v1))
            })
            .unwrap();

        // Deltas were applied.
        assert!(data_tree.is_empty());

        (&data_tree, &*forest, &*delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                set_current_version(v1, v0, forest, delta_map, data_tree)
            })
            .unwrap();

        // Deltas were reversed.
        assert!(data_tree.is_empty());
    }

    #[test]
    fn delete_v1_while_v2_and_restore() {
        let fixture = Fixture::open();
        let (v0, v1, v2) = fixture.create_three_snapshots();

        let (forest, delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();

        let data_tree = fixture.db.open_tree("data").unwrap();

        // Delete v1 while current version is v2.
        (&*forest, &*delta_map)
            .transaction(|(forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);

                delete_snapshot(v1, forest, delta_map)
            })
            .unwrap();

        // Expect state at v2.
        assert_contents(
            &data_tree,
            vec![
                (IVec::from(b"key0"), IVec::from(b"value0")),
                (IVec::from(b"key1"), IVec::from(b"value1")),
                (IVec::from(b"key2"), IVec::from(b"value2")),
            ],
        );

        // Restore v0.
        restore(v2, v0, &data_tree, &forest, &delta_map);
        // Expect state at v0.
        assert_contents(
            &data_tree,
            vec![(IVec::from(b"key0"), IVec::from(b"value0"))],
        );

        // Restore v2.
        restore(v0, v2, &data_tree, &forest, &delta_map);
        // Expect state at v2.
        assert_contents(
            &data_tree,
            vec![
                (IVec::from(b"key0"), IVec::from(b"value0")),
                (IVec::from(b"key1"), IVec::from(b"value1")),
                (IVec::from(b"key2"), IVec::from(b"value2")),
            ],
        );
    }

    #[test]
    fn delete_v1_while_v0_and_restore() {
        let fixture = Fixture::open();
        let (v0, v1, v2) = fixture.create_three_snapshots();

        let (forest, delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();

        let data_tree = fixture.db.open_tree("data").unwrap();

        // Delete v1 while current version is v2.
        (&data_tree, &*forest, &*delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);

                set_current_version(v2, v0, forest, delta_map, data_tree)?;

                delete_snapshot(v1, forest, delta_map)
            })
            .unwrap();

        // Expect state at v0.
        assert_contents(
            &data_tree,
            vec![(IVec::from(b"key0"), IVec::from(b"value0"))],
        );

        // Restore v2.
        restore(v0, v2, &data_tree, &forest, &delta_map);
        // Expect state at v2.
        assert_contents(
            &data_tree,
            vec![
                (IVec::from(b"key0"), IVec::from(b"value0")),
                (IVec::from(b"key1"), IVec::from(b"value1")),
                (IVec::from(b"key2"), IVec::from(b"value2")),
            ],
        );

        // Restore v0.
        restore(v2, v0, &data_tree, &forest, &delta_map);
        // Expect state at v0.
        assert_contents(
            &data_tree,
            vec![(IVec::from(b"key0"), IVec::from(b"value0"))],
        );
    }

    fn restore(
        current_version: u64,
        target_version: u64,
        data_tree: &sled::Tree,
        forest: &VersionForest,
        delta_map: &DeltaMap,
    ) {
        (data_tree, &**forest, &**delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                set_current_version(
                    current_version,
                    target_version,
                    forest,
                    delta_map,
                    data_tree,
                )
            })
            .unwrap();
    }

    fn assert_contents(data_tree: &sled::Tree, expected_kvs: Vec<(IVec, IVec)>) {
        let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>().unwrap();
        for (key, value) in kvs.iter() {
            println!(
                "{}: {}",
                std::str::from_utf8(key).unwrap(),
                std::str::from_utf8(value).unwrap()
            );
        }
        assert_eq!(kvs, expected_kvs);
    }

    struct Fixture {
        pub db: sled::Db,
    }

    impl Fixture {
        pub fn open() -> Self {
            let config = sled::Config::new().temporary(true);
            let db = config.open().unwrap();
            Self { db }
        }

        fn create_three_snapshots(&self) -> (u64, u64, u64) {
            let (forest, delta_map) = open_snapshot_forest(&self.db, "snaps").unwrap();

            // Start with some initial data set.
            let data_tree = self.db.open_tree("data").unwrap();
            data_tree.insert(b"key0", b"value0").unwrap();

            (&data_tree, &*forest, &*delta_map)
                .transaction(|(data_tree, forest, delta_map)| {
                    let forest = TransactionalVersionForest(forest);
                    let delta_map = TransactionalDeltaMap(delta_map);
                    let v0 = create_snapshot_tree(forest)?;

                    let v1_deltas = [Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1"))];
                    let v1 = create_child_snapshot_with_deltas(
                        v0, forest, delta_map, data_tree, &v1_deltas,
                    )?;

                    let v2_deltas = [Delta::Insert(IVec::from(b"key2"), IVec::from(b"value2"))];
                    let v2 = create_child_snapshot_with_deltas(
                        v1, forest, delta_map, data_tree, &v2_deltas,
                    )?;

                    Ok((v0, v1, v2))
                })
                .unwrap()
        }
    }
}
