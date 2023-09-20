use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use glob::{glob_with, MatchOptions};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};

use super::errors::Result;
use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct LocalStoreAccess;

impl Display for LocalStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalStoreAccess")
    }
}

#[async_trait]
impl ObjStoreAccess for LocalStoreAccess {
    fn base_url(&self) -> Result<ObjectStoreUrl> {
        Ok(ObjectStoreUrl::local_filesystem())
    }

    fn create_store(&self) -> Result<Arc<dyn ObjectStore>> {
        Ok(Arc::new(LocalFileSystem::new()))
    }

    fn path(&self, location: &str) -> Result<ObjectStorePath> {
        Ok(ObjectStorePath::from_filesystem_path(location)?)
    }

    /// Given relative paths and all other stuff, it's much simpler to use
    /// `glob_with` from the crate to get metas for all objects.
    async fn list_globbed(
        &self,
        _store: &Arc<dyn ObjectStore>,
        pattern: &str,
    ) -> Result<Vec<ObjectMeta>> {
        let paths = glob_with(
            pattern,
            MatchOptions {
                case_sensitive: true,
                require_literal_separator: true,
                require_literal_leading_dot: true,
            },
        )?;

        let mut objects = Vec::new();
        for path in paths {
            let path = path?;
            let meta = path.metadata()?;
            let meta = ObjectMeta {
                location: self.path(path.to_string_lossy().as_ref())?,
                last_modified: meta.modified()?.into(),
                size: meta.len() as usize,
                e_tag: None,
            };
            objects.push(meta);
        }

        Ok(objects)
    }
}
