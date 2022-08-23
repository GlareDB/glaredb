use std::{sync::Arc, io::Cursor, ops::RangeBounds, fmt::Debug};

use async_trait::async_trait;
use openraft::{RaftStorage, Vote, StorageError, Entry, LogId, EffectiveMembership, StateMachineChanges, storage::{Snapshot, LogState}, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta};

use crate::rocks::RocksStore;

use super::{GlareTypeConfig, GlareNodeId, GlareNode, messaging::GlareResponse};

type StorageResult<T> = Result<T, StorageError<GlareNodeId>>;

impl RocksStore {
    fn set_vote_(&self, vote: &Vote<GlareNodeId>) -> StorageResult<()> {
        todo!();
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<GlareNodeId>>> {
        todo!();
    }
}

#[async_trait]
impl RaftStorage<GlareTypeConfig> for Arc<RocksStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<GlareNodeId>,
    ) -> Result<(), StorageError<GlareNodeId>> {
        self.set_vote_(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<GlareNodeId>>, StorageError<GlareNodeId>> {
        self.get_vote_()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<GlareTypeConfig>]
    ) -> StorageResult<()> {
        todo!();
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<GlareNodeId>,
    ) -> StorageResult<()> {
        todo!();
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<GlareNodeId>,
    ) -> StorageResult<()> {
        todo!();
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GlareNodeId>>,
            EffectiveMembership<GlareNodeId, GlareNode>,
        ),
        StorageError<GlareNodeId>,
    > {
        todo!();
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<GlareTypeConfig>],
    ) -> Result<Vec<GlareResponse>, StorageError<GlareNodeId>> {
        todo!();
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Self::SnapshotData>, StorageError<GlareNodeId>> {
        todo!();
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GlareNodeId, GlareNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<GlareTypeConfig>, StorageError<GlareNodeId>> {
        todo!();
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GlareNodeId, GlareNode, Self::SnapshotData>>, StorageError<GlareNodeId>> {
        todo!();
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        todo!();
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        todo!();
    }
}

#[async_trait]
impl RaftLogReader<GlareTypeConfig> for Arc<RocksStore> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<GlareTypeConfig>> {
        todo!();
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<GlareTypeConfig>>> {
        todo!();
    }
}

#[async_trait]
impl RaftSnapshotBuilder<GlareTypeConfig, Cursor<Vec<u8>>> for Arc<RocksStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GlareNodeId, GlareNode, Cursor<Vec<u8>>>, StorageError<GlareNodeId>> {
        todo!();
    }
}
