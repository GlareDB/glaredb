use datafusion::common::tree_node::{DynTreeNode, Transformed, TreeNode};
use datafusion::error::Result;
use std::sync::Arc;

/// Extension trait for TreeNode.
pub trait TreeNodeExt: TreeNode {
    /// Like `transform_up`, but allows providing a closure with mutable access
    /// to the environment.
    fn transform_up_mut<F>(self, op: &mut F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up_mut(op))?;

        let new_node = op(after_op_children)?.into();
        Ok(new_node)
    }
}

impl<T: DynTreeNode + ?Sized> TreeNodeExt for Arc<T> {}
