//! Each function in this module is implemented as a single `sled` transaction.

use crate::{delta::Delta, TransactionalDeltaMap, TransactionalVersionForest, VersionPath};

use itertools::Itertools;
use sled::{
    transaction::{abort, ConflictableTransactionResult, TransactionalTree},
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
    delta_map: TransactionalDeltaMap,
) -> ConflictableTransactionResult<u64> {
    create_empty_snapshot(forest, delta_map, None)
}

/// Applies `deltas` to `data_tree`, returning the new current version. The old state of `data_tree` is preserved in a new
/// snapshot at `current_version`.
///
/// ```text
///   BEFORE            AFTER
///
///     v1           v1 -----> v2
///      ^            ^         ^
///   current     snapshot    current
/// ```
///
/// If `current_version` is not actually a current version (as tracked by the containing snapshot tree), then the transaction is
/// aborted.
///
/// # Panics
/// - If `deltas` is empty. A snapshot tree must uphold the invariant that only the current version has no deltas.
/// - If `current_version` is `NULL_VERSION` or `sled` runs out of IDs.
///
/// # Implementation Details
///
/// This involves a single transaction which:
/// 1. Replaces data in `data_tree` with the key-value pairs from `deltas`, remembering any old values.
/// 2. Writes the old values into the previously empty delta set for `current_version`.
/// 3. Creates a new empty version node as a child of `current_version`.
pub fn create_snapshot(
    current_version: u64,
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
    data_tree: &TransactionalTree,
    deltas: &[Delta<IVec>],
) -> ConflictableTransactionResult<u64> {
    assert!(
        !deltas.is_empty(),
        "Cannot create new version without deltas"
    );

    // Make sure this is actually the current version.
    if !delta_map.is_current_version(current_version)? {
        return abort(());
    }

    let mut reverse_deltas = Vec::with_capacity(deltas.len());
    apply_deltas(deltas.iter().cloned(), data_tree, &mut reverse_deltas)?;
    delta_map.write_deltas(current_version, reverse_deltas.iter())?;

    create_empty_snapshot(forest, delta_map, Some(current_version))
}

fn create_empty_snapshot(
    forest: TransactionalVersionForest,
    delta_map: TransactionalDeltaMap,
    parent_version: Option<u64>,
) -> ConflictableTransactionResult<u64> {
    let new_version = forest.create_version(parent_version)?;
    delta_map.insert(&new_version.to_be_bytes(), &[])?; // No deltas implies current version.
    Ok(new_version)
}

/// Given a `data_tree` at `current_version`, restores `data_tree` to the state of the `target_version` snapshot.
///
/// Aborts the transaction of:
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
pub fn restore_snapshot(
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
    let deltas = delta_map.remove_version(target_version)?.unwrap();
    let mut reverse_deltas = Vec::new();
    apply_deltas(
        deltas.iter_deltas_into_ivecs(),
        data_tree,
        &mut reverse_deltas,
    )?;
    delta_map.write_deltas(current_version, reverse_deltas.iter())
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
    let deltas = delta_map
        .remove_version(version)?
        .expect("Version already found in transaction");

    if current_is_ancestor {
        // Move the deltas to every child.
        for &child in rm_node.children.iter() {
            delta_map.prepend_deltas(child, deltas.clone())?;
        }
    } else {
        // Move the deltas to the parent.
        delta_map.prepend_deltas(
            rm_node.parent.expect("Deleting a root is forbidden"),
            deltas,
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

// Applies `deltas` to `data_tree` and adds the corresponding reverse deltas to `reverse_deltas`.
fn apply_deltas(
    deltas: impl Iterator<Item = Delta<IVec>>,
    data_tree: &TransactionalTree,
    reverse_deltas: &mut Vec<Delta<IVec>>,
) -> ConflictableTransactionResult<()> {
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

// ████████╗███████╗███████╗████████╗
// ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝
//    ██║   █████╗  ███████╗   ██║
//    ██║   ██╔══╝  ╚════██║   ██║
//    ██║   ███████╗███████║   ██║
//    ╚═╝   ╚══════╝╚══════╝   ╚═╝
#[cfg(test)]
mod test {
    use super::*;
    use crate::open_snapshot_forest;

    use sled::{transaction::TransactionError, Transactional};
    use tempdir::TempDir;

    #[test]
    fn initial_snapshot_tree_has_only_root_version() {
        let fixture = Fixture::open();
        let (forest, delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();

        let root_version = (&*forest, &*delta_map)
            .transaction(|(forest, delta_map)| {
                create_snapshot_tree(
                    TransactionalVersionForest(forest),
                    TransactionalDeltaMap(delta_map),
                )
            })
            .unwrap();

        assert_eq!(forest.collect_versions(), Ok(vec![root_version]));
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
                let root_version = create_snapshot_tree(forest, delta_map)?;

                let deltas = [Delta::Insert(IVec::from(b"key"), IVec::from(b"value"))];
                let new_version =
                    create_snapshot(root_version, forest, delta_map, data_tree, &deltas)?;

                delete_snapshot(new_version, forest, delta_map)
            });

        assert_eq!(result, Err(TransactionError::Abort(())));
    }

    #[test]
    fn restore_snapshot_reverses_deltas() {
        let fixture = Fixture::open();
        let (forest, delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();
        let data_tree = fixture.db.open_tree("data").unwrap();

        let (root_version, new_version) = (&data_tree, &*forest, &*delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                let root_version = create_snapshot_tree(forest, delta_map)?;

                let deltas = [
                    Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1")),
                    Delta::Insert(IVec::from(b"key2"), IVec::from(b"value2")),
                ];
                let new_version =
                    create_snapshot(root_version, forest, delta_map, data_tree, &deltas)?;

                Ok((root_version, new_version))
            })
            .unwrap();

        // Deltas were applied.
        let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            kvs,
            vec![
                (IVec::from(b"key1"), IVec::from(b"value1")),
                (IVec::from(b"key2"), IVec::from(b"value2")),
            ]
        );

        (&data_tree, &*forest, &*delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                restore_snapshot(new_version, root_version, forest, delta_map, data_tree)
            })
            .unwrap();

        // Deltas were reversed.
        assert!(data_tree.is_empty());
    }

    #[test]
    fn restore_after_deleting_intervening_snapshot() {
        let fixture = Fixture::open();
        let (forest, delta_map) = open_snapshot_forest(&fixture.db, "snaps").unwrap();

        // Start with some initial data set.
        let data_tree = fixture.db.open_tree("data").unwrap();
        data_tree.insert(b"key0", b"value0").unwrap();

        let (root_version, v2) = (&data_tree, &*forest, &*delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                let root_version = create_snapshot_tree(forest, delta_map)?;

                let v1_deltas = [Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1"))];
                let v1 = create_snapshot(root_version, forest, delta_map, data_tree, &v1_deltas)?;

                let v2_deltas = [Delta::Insert(IVec::from(b"key2"), IVec::from(b"value2"))];
                let v2 = create_snapshot(v1, forest, delta_map, data_tree, &v2_deltas)?;

                delete_snapshot(v1, forest, delta_map)?;

                Ok((root_version, v2))
            })
            .unwrap();

        // Expect state at v3.
        let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            kvs,
            vec![
                (IVec::from(b"key0"), IVec::from(b"value0")),
                (IVec::from(b"key1"), IVec::from(b"value1")),
                (IVec::from(b"key2"), IVec::from(b"value2")),
            ]
        );

        (&data_tree, &*forest, &*delta_map)
            .transaction(|(data_tree, forest, delta_map)| {
                let forest = TransactionalVersionForest(forest);
                let delta_map = TransactionalDeltaMap(delta_map);
                restore_snapshot(v2, root_version, forest, delta_map, data_tree)
            })
            .unwrap();

        // Expect state at root version.
        let kvs = data_tree.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(kvs, vec![(IVec::from(b"key0"), IVec::from(b"value0")),]);
    }

    struct Fixture {
        _tmp: TempDir, // Just here to own the TempDir so it isn't dropped until after the test.
        pub db: sled::Db,
    }

    impl Fixture {
        pub fn open() -> Self {
            let tmp = TempDir::new("sled-snapshots-test").unwrap();
            let db = sled::open(&tmp).unwrap();

            Self { _tmp: tmp, db }
        }
    }
}
