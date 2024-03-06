use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use glob::{glob_with, MatchOptions};
use ioutil::resolve_path;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};

use super::errors::Result;
use super::glob_util::ResolvedPattern;
use super::ObjStoreAccess;

#[derive(Debug, Clone)]
pub struct LocalStoreAccess;

impl Display for LocalStoreAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalStoreAccess")
    }
}
impl LocalStoreAccess {
    fn meta_from_path(&self, path: PathBuf) -> Result<ObjectMeta> {
        let path = resolve_path(&path)?;
        let meta = path.metadata()?;
        Ok(ObjectMeta {
            location: self.path(path.to_string_lossy().as_ref())?,
            last_modified: meta.modified()?.into(),
            size: meta.len() as usize,
            e_tag: None,
            version: None,
        })
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
        ObjectStorePath::from_filesystem_path(location)
            .map_err(super::errors::ObjectStoreSourceError::ObjectStorePath)
    }

    /// Given relative paths and all other stuff, it's much simpler to use
    /// `glob_with` from the crate to get metas for all objects.
    async fn list_globbed(
        &self,
        _store: &Arc<dyn ObjectStore>,
        pattern: ResolvedPattern,
    ) -> Result<Vec<ObjectMeta>> {
        let paths = glob_with(
            pattern.as_ref(),
            MatchOptions {
                case_sensitive: true,
                require_literal_separator: true,
                require_literal_leading_dot: true,
            },
        )?;

        let mut objects = Vec::new();

        for path in paths {
            let path = path?;

            let meta = self.meta_from_path(path)?;
            objects.push(meta);
        }

        if objects.is_empty() {
            let path = String::from(pattern);
            let meta = self.meta_from_path(path.into())?;
            objects.push(meta);
        }
        Ok(objects)
    }
}
