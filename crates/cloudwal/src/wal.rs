use crate::errors::{Result, WalError};
use bytes::Buf;
use lemur::execute::stream::source::{DataSource, WriteTx};
use lemur::repr::df::{DataFrame, Schema};
use lemur::repr::relation::RelationKey;
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info, trace, warn};

/// Messages that correspond to mutable lemur data source operations.
// TODO: Not zero copy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LemurOp {
    Begin {
        tx_id: u64,
    },
    Commit {
        tx_id: u64,
    },
    Rollback {
        tx_id: u64,
    },
    AllocateTable {
        tx_id: u64,
        table: RelationKey,
        schema: Schema,
    },
    DeallocateTable {
        tx_id: u64,
        table: RelationKey,
    },
    Insert {
        tx_id: u64,
        table: RelationKey,
        pk_idxs: Vec<usize>,
        data: DataFrame,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalMessage {
    lsn: u64,
    op: LemurOp,
}

const TARGET_WAL_MESSAGES_PER_FILE: usize = 100_000;
const WAL_MESSAGE_BUF: usize = 256;
const MANIFEST_PATH: &'static str = "MANIFEST";

#[derive(Debug, Serialize, Deserialize, Default)]
struct Manifest {
    num_complete_wal_files: usize,
    has_incomplete_wal_file: bool,
    last_lsn: u64,
}

#[derive(Debug)]
struct WalInner<S> {
    /// The underlying lemur data source.
    source: S,
    control: mpsc::Sender<ControlMessage<S>>,
    incoming: mpsc::Sender<LemurOp>,
    handle: JoinHandle<Result<()>>,
}

#[derive(Debug)]
pub struct Wal<S> {
    inner: Arc<WalInner<S>>,
}

impl<S> Wal<S>
where
    S: DataSource + 'static,
{
    /// A open a WAL using the provided data source and object store.
    ///
    /// If it's detected that the object store contains a log from a previous
    /// run, the log will be recovered and executed against the data source.
    pub async fn open<O>(source: S, store: O, root: impl Into<Path>) -> Result<Self>
    where
        O: ObjectStore,
    {
        let (control_tx, control_rx) = mpsc::channel(WAL_MESSAGE_BUF);
        let (incoming_tx, incoming_rx) = mpsc::channel(WAL_MESSAGE_BUF);

        let worker = WalWorker::open(store, root.into(), control_rx, incoming_rx).await?;
        let handle = tokio::spawn(worker.run());

        let (source_tx, source_rx) = oneshot::channel();
        control_tx
            .send(ControlMessage::ReplayAll(source, source_tx))
            .await
            .map_err(|_| WalError::BrokenChannel)?;

        let source = source_rx.await.map_err(|_| WalError::BrokenChannel)?;

        Ok(Wal {
            inner: Arc::new(WalInner {
                source,
                control: control_tx,
                incoming: incoming_tx,
                handle,
            }),
        })
    }

    pub async fn shutdown_wait(&self) -> Result<()> {
        self.inner
            .control
            .send(ControlMessage::DrainShutdown)
            .await
            .map_err(|_| WalError::BrokenChannel)?;
        loop {
            trace!("awaiting handle finished");
            if self.inner.handle.is_finished() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    }

    pub async fn send_op(&self, op: LemurOp) -> Result<()> {
        self.inner
            .incoming
            .send(op)
            .await
            .map_err(|_| WalError::BrokenChannel)
    }

    pub async fn latest_lsn(&self) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .control
            .send(ControlMessage::Lsn(tx))
            .await
            .map_err(|_| WalError::BrokenChannel)?;
        rx.await.map_err(|_| WalError::BrokenChannel)
    }

    pub async fn flush_wal(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .control
            .send(ControlMessage::FlushToStore(tx))
            .await
            .map_err(|_| WalError::BrokenChannel)?;
        rx.await.map_err(|_| WalError::BrokenChannel)
    }
}

impl<S: DataSource> Wal<S> {
    pub(crate) fn get_source(&self) -> &S {
        &self.inner.source
    }
}

impl<S> Clone for Wal<S> {
    fn clone(&self) -> Self {
        Wal {
            inner: self.inner.clone(),
        }
    }
}

unsafe impl<S> Sync for Wal<S> {}
unsafe impl<S> Send for Wal<S> {}

enum ControlMessage<S> {
    DrainShutdown,
    /// Get the most recent lsn.
    Lsn(oneshot::Sender<u64>),
    /// Flush all messages to the store.
    FlushToStore(oneshot::Sender<()>),
    /// Replay the entirety of the log on a given data source.
    ReplayAll(S, oneshot::Sender<S>),
}

impl<S> fmt::Debug for ControlMessage<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlMessage::DrainShutdown => write!(f, "DrainShutdown"),
            ControlMessage::Lsn(_) => write!(f, "Lsn"),
            ControlMessage::FlushToStore(_) => write!(f, "FlushToStore"),
            ControlMessage::ReplayAll(_, _) => write!(f, "ReplayAll"),
        }
    }
}

#[derive(Debug)]
struct WalWorker<O, S> {
    store: O,
    root: Path,
    manifest: Manifest,
    buf: Vec<WalMessage>,
    control: mpsc::Receiver<ControlMessage<S>>,
    incoming: mpsc::Receiver<LemurOp>,
}

impl<O, S> WalWorker<O, S>
where
    O: ObjectStore,
    S: DataSource,
{
    async fn open(
        store: O,
        root: Path,
        control: mpsc::Receiver<ControlMessage<S>>,
        incoming: mpsc::Receiver<LemurOp>,
    ) -> Result<Self> {
        let manifest = Self::read_manifest_or_default(&store, &root).await?;
        let buf = if manifest.has_incomplete_wal_file {
            Self::load_incomplete_wal_file(&store, &root).await?
        } else {
            Vec::new()
        };

        Ok(WalWorker {
            store,
            root,
            manifest,
            buf,
            control,
            incoming,
        })
    }

    async fn run(mut self) -> Result<()> {
        info!("wal worker running");
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    trace!("interval flush");
                    self.flush().await?;
                }

                Some(msg) = self.incoming.recv() => {
                    trace!(?msg, "received lemur op message");
                    self.handle_wal(msg).await?;
                }

                Some(msg) = self.control.recv() => {
                    trace!(?msg, "received control message");
                    self.handle_control(msg).await?;
                }

                else => {
                    info!("channel closed, flushing and exiting");
                    match self.flush().await {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            error!(?e, "failed to flush");
                            return Err(e);
                        }
                    }
                }
            };
        }
    }

    async fn handle_control(&mut self, msg: ControlMessage<S>) -> Result<()> {
        match msg {
            ControlMessage::DrainShutdown => {
                // Prevent any additional wal messages. This will cause the run
                // loop to exit, allowing use to await the join handle.
                self.incoming.close();
                self.control.close();
            }
            ControlMessage::Lsn(ch) => ch
                .send(self.manifest.last_lsn)
                .map_err(|_| WalError::BrokenChannel)?,
            ControlMessage::FlushToStore(ch) => {
                self.flush().await?;
                ch.send(()).map_err(|_| WalError::BrokenChannel)?;
            }
            ControlMessage::ReplayAll(source, ch) => {
                self.replay_all(&source).await?;
                ch.send(source).map_err(|_| WalError::BrokenChannel)?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.write_wal(true).await?;
        self.manifest.has_incomplete_wal_file = true;
        self.write_manifest().await?;
        Ok(())
    }

    async fn handle_wal(&mut self, op: LemurOp) -> Result<()> {
        let lsn = self.manifest.last_lsn;
        self.manifest.last_lsn += 1;
        let wal = WalMessage { lsn, op };

        trace!(?wal, "wal message");

        self.buf.push(wal);
        if self.buf.len() >= TARGET_WAL_MESSAGES_PER_FILE {
            self.write_wal(false).await?;
            self.buf.truncate(0);
            self.manifest.num_complete_wal_files += 1;
            self.manifest.has_incomplete_wal_file = false;
            self.write_manifest().await?;
        }
        Ok(())
    }

    async fn write_wal(&mut self, incomplete: bool) -> Result<()> {
        trace!(?incomplete, "writing wal");
        let path = if incomplete {
            self.root.child(Self::incomplete_wal_file_name())
        } else {
            self.root.child(Self::format_wal_file_name(
                self.manifest.num_complete_wal_files,
            ))
        };
        let buf = bincode::serialize(&self.buf)?;
        self.store.put(&path, buf.into()).await?;
        Ok(())
    }

    async fn write_manifest(&self) -> Result<()> {
        let path = self.root.child(MANIFEST_PATH);
        let buf = bincode::serialize(&self.manifest)?;
        self.store.put(&path, buf.into()).await?;
        Ok(())
    }

    /// Replay all logs and apply them to source.
    ///
    /// Note that this does _not_ exit on errors returned from source.
    async fn replay_all(&self, source: &S) -> Result<()> {
        let mut replayer = WalReplayer::new(source);

        for i in 0..self.manifest.num_complete_wal_files {
            let path = self.root.child(Self::format_wal_file_name(i));
            let buf = self.store.get(&path).await?.bytes().await?;
            let msgs: Vec<WalMessage> = bincode::deserialize(&buf)?;

            for msg in msgs.into_iter() {
                match replayer.replay_msg(msg).await {
                    Ok(_) => trace!("message replayed"),
                    Err(e) => warn!(?e, "source returned error during replay"),
                }
            }
        }

        for msg in self.buf.iter() {
            match replayer.replay_msg(msg.clone()).await {
                Ok(_) => trace!("message replayed"),
                Err(e) => warn!(?e, "source returned error during replay"),
            }
        }

        Ok(())
    }

    async fn read_manifest_or_default(store: &O, root: &Path) -> Result<Manifest> {
        let path = root.child(MANIFEST_PATH);
        let res = match store.get(&path).await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. }) => {
                info!("manifest not found, creating default");
                return Ok(Manifest::default());
            }
            Err(e) => return Err(e.into()),
        };
        let buf = res.bytes().await?;
        let manifest: Manifest = bincode::deserialize_from(buf.reader())?;
        info!(?manifest, "manifest found");
        Ok(manifest)
    }

    async fn load_incomplete_wal_file(store: &O, root: &Path) -> Result<Vec<WalMessage>> {
        let path = root.child(Self::incomplete_wal_file_name());
        let buf = store.get(&path).await?.bytes().await?;
        let msgs: Vec<WalMessage> = bincode::deserialize(&buf)?;
        Ok(msgs)
    }

    const fn incomplete_wal_file_name() -> &'static str {
        "wal-incomplete"
    }

    fn format_wal_file_name(num: usize) -> String {
        format!("wal-{}", num)
    }
}

struct WalReplayer<'a, S: DataSource> {
    active_txs: BTreeMap<u64, Vec<S::Tx>>,
    source: &'a S,
}

impl<'a, S: DataSource> WalReplayer<'a, S> {
    fn new(source: &'a S) -> Self {
        WalReplayer {
            active_txs: BTreeMap::new(),
            source,
        }
    }

    async fn replay_msg(&mut self, msg: WalMessage) -> Result<()> {
        use std::collections::btree_map::Entry;

        trace!(?msg, "replaying message");

        match msg.op {
            LemurOp::Begin { tx_id } => {
                let tx = self.source.begin().await?;
                match self.active_txs.entry(tx_id) {
                    Entry::Vacant(entry) => {
                        entry.insert(vec![tx]);
                    }
                    Entry::Occupied(mut entry) => {
                        let txs = entry.get_mut();
                        txs.push(tx);
                    }
                }
            }
            LemurOp::Commit { tx_id } => {
                let tx = self.pop_active_tx_for_id(tx_id)?;
                tx.commit().await?;
            }
            LemurOp::Rollback { tx_id } => {
                let tx = self.pop_active_tx_for_id(tx_id)?;
                tx.rollback().await?;
            }
            LemurOp::AllocateTable {
                tx_id,
                table,
                schema,
            } => {
                let tx = self.get_active_tx_for_id(tx_id)?;
                tx.allocate_table(table, schema).await?;
            }
            LemurOp::DeallocateTable { tx_id, table } => {
                let tx = self.get_active_tx_for_id(tx_id)?;
                tx.deallocate_table(&table).await?;
            }
            LemurOp::Insert {
                tx_id,
                table,
                pk_idxs,
                data,
            } => {
                let tx = self.get_active_tx_for_id(tx_id)?;
                tx.insert(&table, &pk_idxs, data).await?;
            }
        }

        Ok(())
    }

    fn get_active_tx_for_id(&self, tx_id: u64) -> Result<&S::Tx> {
        self.active_txs
            .get(&tx_id)
            .ok_or(WalError::MissingTxForReplay(tx_id))?
            .last()
            .ok_or(WalError::MissingTxForReplay(tx_id))
    }

    fn pop_active_tx_for_id(&mut self, tx_id: u64) -> Result<S::Tx> {
        let txs = self
            .active_txs
            .get_mut(&tx_id)
            .ok_or(WalError::MissingTxForReplay(tx_id))?;
        let tx = txs.pop().ok_or(WalError::MissingTxForReplay(tx_id))?;
        Ok(tx)
    }
}
