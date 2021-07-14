use crate::{
    u64_from_be_slice,
    version_node::{RawVersionNode, VersionNode, NULL_VERSION},
};

use sled::{
    transaction::{
        abort, ConflictableTransactionResult, TransactionalTree, UnabortableTransactionError,
    },
    IVec, Tree,
};
use std::ops::Deref;

/// A [sled::Tree] that stores a set of versions, each of which is a node in some tree.
pub struct VersionForest(pub Tree);

impl Deref for VersionForest {
    type Target = Tree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl VersionForest {
    /// Returns an iterator over all versions in the forest.
    pub fn iter_versions(&self) -> impl Iterator<Item = sled::Result<u64>> {
        self.iter()
            .map(|kv_result| kv_result.map(|(k, _v)| u64_from_be_slice(&k)))
    }

    /// Collects all versions into a `Vec`.
    pub fn collect_versions(&self) -> sled::Result<Vec<u64>> {
        self.iter_versions().collect()
    }
}

/// Same as [VersionForest], but used in transactions.
#[derive(Clone, Copy)]
pub struct TransactionalVersionForest<'a>(pub &'a TransactionalTree);

impl<'a> Deref for TransactionalVersionForest<'a> {
    type Target = TransactionalTree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> TransactionalVersionForest<'a> {
    pub(crate) fn get_version(
        &self,
        version: u64,
    ) -> Result<Option<RawVersionNode<IVec>>, UnabortableTransactionError> {
        self.get(version.to_be_bytes())
            .map(|result| result.map(RawVersionNode::new))
    }

    pub(crate) fn create_version(
        &self,
        parent_version: Option<u64>,
    ) -> ConflictableTransactionResult<u64> {
        let new_version = self.generate_id()?;
        assert_ne!(new_version, NULL_VERSION);
        let new_version_bytes = new_version.to_be_bytes();

        let new_node = VersionNode::new_maybe_with_parent(parent_version);
        self.insert(&new_version_bytes, &new_node)?;

        if new_node.parent.is_some() {
            // We also need to add this version as a child in the parent's node.
            let parent_bytes = new_node.parent_be_bytes();
            if let Some(parent_node_ivec) = self.get(parent_bytes)? {
                // PERF: can we avoid read-modify-write?
                let mut parent_node = VersionNode::from(RawVersionNode::new(parent_node_ivec));
                parent_node.children.push(new_version);
                self.insert(&parent_bytes, &parent_node)?;

                Ok(new_version)
            } else {
                // Abort so we don't create a dangling pointer in the tree.
                abort(())
            }
        } else {
            Ok(new_version)
        }
    }

    /// Deletes `root` version and all versions that have `root` as an ancestor.
    pub(crate) fn delete_tree(
        &self,
        root: u64,
        mut deleted_version_rx: impl FnMut(u64) -> ConflictableTransactionResult<()>,
    ) -> ConflictableTransactionResult<()> {
        let mut delete_queue = vec![root];
        while let Some(version) = delete_queue.pop() {
            if let Some(node) = self.remove(&version.to_be_bytes())? {
                deleted_version_rx(version)?;
                let node = RawVersionNode::new(node);
                delete_queue.extend(node.iter_children());
            }
        }
        Ok(())
    }

    /// Deletes `version`. This preserves the "ancestor of" relation.
    ///
    /// You cannot delete a root version. This will result in an aborted transaction. If necessary, you can delete an entire
    /// tree with [VersionForest::delete_tree].
    ///
    /// If `version` has any children, then they will be re-parented to the parent of `version`. If `version` does not exist,
    /// then nothing happens.
    pub(crate) fn remove_version(
        &self,
        version: u64,
    ) -> ConflictableTransactionResult<Option<VersionNode>> {
        // Remove version.
        let rm_node = if let Some(node_ivec) = self.remove(&version.to_be_bytes())? {
            VersionNode::from(RawVersionNode::new(node_ivec))
        } else {
            // Nothing to do.
            return Ok(None);
        };

        // Cannot delete the root version.
        if rm_node.parent.is_none() {
            return abort(());
        }

        // Re-parent the children.
        // PERF: avoid read-modify-write?
        for &child in rm_node.children.iter() {
            let child_key_bytes = child.to_be_bytes();
            let mut child_node =
                VersionNode::from(RawVersionNode::new(self.get(child_key_bytes)?.unwrap()));
            child_node.parent = rm_node.parent;
            self.insert(&child_key_bytes, &child_node)?;
        }
        let new_parent_key_bytes = rm_node.parent_be_bytes();
        if let Some(new_parent_node_ivec) = self.get(new_parent_key_bytes)? {
            let mut new_parent_node = VersionNode::from(RawVersionNode::new(new_parent_node_ivec));
            for &child in rm_node.children.iter() {
                new_parent_node.children.push(child);
            }
            self.insert(&new_parent_key_bytes, &new_parent_node)?;
        }

        Ok(Some(rm_node))
    }

    pub fn find_path_to_root(&self, version: u64) -> ConflictableTransactionResult<Vec<u64>> {
        let mut node = if let Some(node) = self.get_version(version)? {
            node
        } else {
            return abort(());
        };

        let mut path = vec![version];
        while let Some(parent_version) = node.parent() {
            path.push(parent_version);
            node = self
                .get_version(parent_version)?
                .expect("Inconsistent forest: followed pointer to version");
        }

        Ok(path)
    }

    pub fn find_path_between_versions(
        &self,
        start: u64,
        finish: u64,
    ) -> ConflictableTransactionResult<VersionPath> {
        let start_to_root = self.find_path_to_root(start)?;
        let mut finish_to_root = self.find_path_to_root(finish)?;

        if start_to_root.last() != finish_to_root.last() {
            return Ok(VersionPath::NoPathExists);
        }

        let mut start_join = 0;
        let mut finish_join = 0;
        for ((i1, v1), (i2, v2)) in start_to_root
            .iter()
            .enumerate()
            .rev()
            .zip(finish_to_root.iter().enumerate().rev())
        {
            if v1 != v2 {
                // The previous index held the nearest common ancestor.
                break;
            }
            start_join = i1;
            finish_join = i2;
        }

        let mut path = start_to_root[..=start_join].to_vec();
        let further_slice = &mut finish_to_root[..finish_join];
        further_slice.reverse();
        path.extend_from_slice(further_slice);
        Ok(VersionPath::PathExists(path))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VersionPath {
    PathExists(Vec<u64>),
    NoPathExists,
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

    use sled::transaction::TransactionError;
    use tempdir::TempDir;

    #[test]
    fn delete_root_version_aborts() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        let result = vtree.transaction(|t| {
            let forest = TransactionalVersionForest(t);
            let root = forest.create_version(None)?;
            forest.remove_version(root)
        });
        assert!(matches!(result, Err(TransactionError::Abort(()))));
    }

    #[test]
    fn path_to_missing_version_aborts() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        let result = vtree.transaction(|t| {
            let t = TransactionalVersionForest(t);
            let root = t.create_version(None)?;

            let _path = t.find_path_between_versions(root, 666)?;

            Ok(())
        });

        assert_eq!(result, Err(TransactionError::Abort(())));
    }

    #[test]
    fn path_from_missing_version_aborts() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        let result = vtree.transaction(|t| {
            let t = TransactionalVersionForest(t);
            let root = t.create_version(None)?;

            let _path = t.find_path_between_versions(666, root)?;

            Ok(())
        });

        assert_eq!(result, Err(TransactionError::Abort(())));
    }

    #[test]
    fn path_from_root_to_root() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        vtree
            .transaction(|t| {
                let t = TransactionalVersionForest(t);
                let root = t.create_version(None)?;

                let path = t.find_path_between_versions(root, root)?;
                assert_eq!(path, VersionPath::PathExists(vec![root]));

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn path_from_child_to_root() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        vtree
            .transaction(|t| {
                let t = TransactionalVersionForest(t);
                let root = t.create_version(None)?;
                let child = t.create_version(Some(root))?;

                let path = t.find_path_between_versions(child, root)?;
                assert_eq!(path, VersionPath::PathExists(vec![child, root]));
                let path = t.find_path_between_versions(root, child)?;
                assert_eq!(path, VersionPath::PathExists(vec![root, child]));

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn path_between_children() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        vtree
            .transaction(|t| {
                let t = TransactionalVersionForest(t);
                let root = t.create_version(None)?;
                let c1 = t.create_version(Some(root))?;
                let c2 = t.create_version(Some(root))?;

                let path = t.find_path_between_versions(c1, c2)?;
                assert_eq!(path, VersionPath::PathExists(vec![c1, root, c2]));
                let path = t.find_path_between_versions(c2, c1)?;
                assert_eq!(path, VersionPath::PathExists(vec![c2, root, c1]));

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn path_between_nested_children() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        vtree
            .transaction(|t| {
                let t = TransactionalVersionForest(t);
                let root = t.create_version(None)?;
                let c1 = t.create_version(Some(root))?;
                let c2 = t.create_version(Some(c1))?;
                let c3 = t.create_version(Some(root))?;

                let path = t.find_path_between_versions(c2, c3)?;
                assert_eq!(path, VersionPath::PathExists(vec![c2, c1, root, c3]));
                let path = t.find_path_between_versions(c3, c2)?;
                assert_eq!(path, VersionPath::PathExists(vec![c3, root, c1, c2]));

                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn path_between_disconnected_versions_does_not_exist() {
        let fixture = Fixture::open();
        let vtree = fixture.open_version_forest();

        vtree
            .transaction(|t| {
                let t = TransactionalVersionForest(t);
                let root1 = t.create_version(None)?;
                let root2 = t.create_version(None)?;

                let path = t.find_path_between_versions(root1, root2)?;
                assert_eq!(path, VersionPath::NoPathExists);
                let path = t.find_path_between_versions(root2, root1)?;
                assert_eq!(path, VersionPath::NoPathExists);

                Ok(())
            })
            .unwrap();
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

        pub fn open_version_forest(&self) -> VersionForest {
            VersionForest(self.db.open_tree("versions").unwrap())
        }
    }
}
