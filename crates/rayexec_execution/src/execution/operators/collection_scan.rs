use std::sync::Arc;

use crate::arrays::collection::concurrent::ConcurrentColumnCollection;

#[derive(Debug)]
pub struct CollectionScanOperatorState {}

#[derive(Debug)]
pub struct PhysicalCollectionScan {
    pub(crate) collection: Arc<ConcurrentColumnCollection>,
}
