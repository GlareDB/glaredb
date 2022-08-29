use crate::errors::{Result, WalError};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use lemur::execute::stream::source::{DataFrameStream, DataSource, ReadTx, WriteTx};
use lemur::repr::df::{DataFrame, Schema};
use lemur::repr::relation::{PrimaryKeyIndices, RelationKey};
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, trace, warn};

/// Messages that correspond to mutable lemur data source operations.
// TODO: Not zero copy.
#[derive(Debug, Serialize, Deserialize)]
pub enum LemurOp {
    Begin,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct WalMessage {
    lsn: u64,
    op: LemurOp,
}

const MANIFEST_PATH: &'static str = "MANIFEST";

#[derive(Debug, Serialize, Deserialize, Default)]
struct Manifest {
    num_wal_files: usize,
    latest_complete: bool,
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
                handle,
                control: control_tx,
                incoming: incoming_tx,
            }),
        })
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
            ControlMessage::Lsn(_) => write!(f, "Lsn"),
            ControlMessage::FlushToStore(_) => write!(f, "FlushToStore"),
            ControlMessage::ReplayAll(_, _) => write!(f, "ReplayAll"),
        }
    }
}

const TARGET_WAL_MESSAGES_PER_FILE: usize = 100_000;
const WAL_MESSAGE_BUF: usize = 256;

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
        Ok(WalWorker {
            store,
            root,
            manifest,
            buf: Vec::new(),
            control,
            incoming,
        })
    }

    async fn run(mut self) -> Result<()> {
        info!("wal worker running");
        loop {
            tokio::select! {
                Some(msg) = self.incoming.recv() => {
                    trace!(?msg, "received lemur op message");
                    self.handle_wal(msg).await?;
                }

                Some(msg) = self.control.recv() => {
                    trace!(?msg, "received control message");
                    self.handle_control(msg).await?;
                }

                else => {
                    info!("channel closed, exiting wal worker");
                    return Ok(())
                }
            };
        }
    }

    async fn handle_control(&mut self, msg: ControlMessage<S>) -> Result<()> {
        match msg {
            ControlMessage::Lsn(ch) => ch
                .send(self.manifest.last_lsn)
                .map_err(|_| WalError::BrokenChannel)?,
            ControlMessage::FlushToStore(ch) => {
                self.write_wal().await?;
                ch.send(()).map_err(|_| WalError::BrokenChannel)?;
            }
            ControlMessage::ReplayAll(source, ch) => {
                self.replay_all(&source).await?;
                ch.send(source).map_err(|_| WalError::BrokenChannel)?;
            }
        }
        Ok(())
    }

    async fn handle_wal(&mut self, op: LemurOp) -> Result<()> {
        let lsn = self.manifest.last_lsn;
        self.manifest.last_lsn += 1;
        self.buf.push(WalMessage { lsn, op });
        if self.buf.len() >= TARGET_WAL_MESSAGES_PER_FILE {
            self.write_wal().await?;
            self.buf.truncate(0);
            self.manifest.num_wal_files += 1;
            self.write_manifest().await?;
        }
        Ok(())
    }

    async fn write_wal(&self) -> Result<()> {
        let path = self
            .root
            .child(Self::format_wal_file_name(self.manifest.num_wal_files));
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

        for i in 0..self.manifest.num_wal_files {
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
        static TX_ID: AtomicU64 = AtomicU64::new(0);

        match msg.op {
            LemurOp::Begin => {
                let tx = self.source.begin().await?;
                match self.active_txs.entry(TX_ID.fetch_add(1, Ordering::Relaxed)) {
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
