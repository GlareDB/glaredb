use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::datasource::TableProvider;

use crate::common::url::DatasourceUrl;
use crate::json::errors::JsonError;
use crate::object_store::generic::GenericStoreAccess;
use crate::object_store::ObjStoreAccess;

pub async fn json_streaming_table(
    store_access: GenericStoreAccess,
    source_url: DatasourceUrl,
) -> Result<Arc<dyn TableProvider>, JsonError> {
    let path = source_url.path();

    let store = store_access.create_store()?;

    // assume that the file type is a glob and see if there are
    // more files...
    let mut list = store_access.list_globbed(&store, path.as_ref()).await?;

    if list.is_empty() {
        return Err(JsonError::NotFound(path.into_owned()));
    }

    // for consistent results, particularly for the sample, always
    // sort by location
    list.sort_by(|a, b| a.location.cmp(&b.location));

    let mut data = VecDeque::with_capacity(list.len());
    for obj in list {
        let blob = store.get(&obj.location).await?.bytes()?;
        data.append(serde_json::from_slice(blob)?)
    }

    Ok(())
}
