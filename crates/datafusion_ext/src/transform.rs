use std::sync::Arc;

use datafusion::common::tree_node::{DynTreeNode, Transformed, TreeNode};
use datafusion::error::Result;

/// Extension trait for TreeNode.
pub trait TreeNodeExt: TreeNode {
    /// Like `transform_up`, but allows providing a closure with mutable access
    /// to the environment.
    fn transform_up_mut<F>(self, op: &mut F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| TreeNode::transform_up_mut(node, op))?;

        let new_node = op(after_op_children)?.into();
        Ok(new_node)
    }

    /// Like `transform_down`, but allows providing a closure with mutable access
    /// to the environment.
    fn transform_down_mut<F>(self, op: &mut F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let after_op = op(self)?.into();
        after_op.map_children(|node| TreeNode::transform_down_mut(node, op))
    }
}

impl<T: DynTreeNode + ?Sized> TreeNodeExt for Arc<T> {}
