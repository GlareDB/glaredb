use crate::errors::{internal, Result};
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use std::path::{Path as StdPath, PathBuf};
use tokio::sync::{mpsc, oneshot};
use tracing::{trace, warn};

const DUMMY_FILE: &str = "dummy";

const WORKER_BUF_SIZE: usize = 256;

#[derive(Clone)]
pub struct CloudSync {
    local: PathBuf,
    requests: mpsc::Sender<(Operation, oneshot::Sender<Result<()>>)>,
}

impl CloudSync {
    /// Create a new cloud syncer.
    pub fn new<O: ObjectStore>(object_store: O, local: PathBuf) -> (CloudSync, Worker<O>) {
        let (tx, rx) = mpsc::channel(WORKER_BUF_SIZE);
        let worker = Worker::new(object_store, local.clone(), rx);
        let cs = CloudSync {
            local,
            requests: tx,
        };
        (cs, worker)
    }

    pub fn local_path(&self) -> &StdPath {
        self.local.as_path()
    }

    /// Allocate a table file locally as well as in object storage.
    pub async fn allocate_table_file<P: AsRef<StdPath>>(&self, path: P) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let op = Operation::AllocateTableFile(path.as_ref().to_path_buf());
        if let Err(_) = self.requests.send((op, tx)).await {
            return Err(internal!("failed to send"));
        }

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(internal!("failed to receive")),
        }
    }
}

#[derive(Debug)]
enum Operation {
    /// Allocate a table file with the given path.
    ///
    /// The path should be relative to the local root.
    AllocateTableFile(PathBuf),

    /// Sync the local file to the remote file.
    ///
    /// The path should be relative to the local root.
    SyncToRemote(PathBuf),
}

pub struct Worker<O> {
    object_store: O,
    local: PathBuf,
    requests: mpsc::Receiver<(Operation, oneshot::Sender<Result<()>>)>,
}

impl<O: ObjectStore> Worker<O> {
    fn new<P: AsRef<StdPath>>(
        object_store: O,
        local: P,
        requests: mpsc::Receiver<(Operation, oneshot::Sender<Result<()>>)>,
    ) -> Self {
        Worker {
            object_store,
            local: local.as_ref().to_path_buf(),
            requests,
        }
    }

    pub async fn run(mut self) {
        // TODO: Run these all with some sort of parallelism.
        while let Some((op, result_sender)) = self.requests.recv().await {
            trace!(?op, "execution sync op");
            let result = match op {
                Operation::AllocateTableFile(path) => self.allocate_table_file(path).await,
                Operation::SyncToRemote(path) => self.sync_to_remote(path).await,
                _other => unimplemented!(),
            };
            if let Err(result) = result_sender.send(result) {
                warn!(?result, "failed to send result of operation");
            }
        }
    }

    async fn allocate_table_file(&self, path: PathBuf) -> Result<()> {
        if path.is_absolute() {
            return Err(internal!("path not relative: {:?}", path));
        }
        if path.is_dir() {
            return Err(internal!("path must point to file: {:?}", path));
        }

        let parent = match path.parent() {
            Some(p) => p,
            None => return Err(internal!("missing parent directory for path: {:?}", path)),
        };

        let object_path = Path::from_filesystem_path(&path)?;
        self.object_store.put(&object_path, Bytes::new()).await?;

        std::fs::create_dir_all(parent)?;

        Ok(())
    }

    async fn sync_to_remote(&self, _path: PathBuf) -> Result<()> {
        // let source = std::fs::OpenOptions.
        unimplemented!()
    }
}
