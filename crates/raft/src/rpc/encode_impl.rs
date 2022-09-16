use crate::openraft_types::prelude::*;

use super::pb::{
    AddLearnerResponse, AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, MetricsResponse, VoteRequest, VoteResponse, AddLearnerRequest,
};

macro_rules! bincode_try_from_rpc {
    ($from:ident, $to:ident) => {
        impl TryFrom<$from> for $to {
            type Error = bincode::Error;

            fn try_from(resp: $from) -> Result<Self, Self::Error> {
                let payload: $to = bincode::deserialize(&resp.payload)?;
                Ok(payload)
            }
        }
    };
}

macro_rules! bincode_try_to_rpc {
    ($from:ident, $to:ident) => {
        impl TryFrom<$from> for $to {
            type Error = bincode::Error;

            fn try_from(resp: $from) -> Result<Self, Self::Error> {
                let payload = bincode::serialize(&resp)?;
                Ok($to { payload })
            }
        }
    };
}

bincode_try_from_rpc!(InstallSnapshotRequest, OInstallSnapshotRequest);
bincode_try_to_rpc!(OInstallSnapshotRequest, InstallSnapshotRequest);
bincode_try_from_rpc!(InstallSnapshotResponse, OInstallSnapshotResponse);
bincode_try_to_rpc!(OInstallSnapshotResponse, InstallSnapshotResponse);

bincode_try_from_rpc!(AppendEntriesRequest, OAppendEntriesRequest);
bincode_try_to_rpc!(OAppendEntriesRequest, AppendEntriesRequest);
bincode_try_from_rpc!(AppendEntriesResponse, OAppendEntriesResponse);
bincode_try_to_rpc!(OAppendEntriesResponse, AppendEntriesResponse);

bincode_try_from_rpc!(VoteRequest, OVoteRequest);
bincode_try_to_rpc!(OVoteRequest, VoteRequest);
bincode_try_from_rpc!(VoteResponse, OVoteResponse);
bincode_try_to_rpc!(OVoteResponse, VoteResponse);

bincode_try_from_rpc!(ClientWriteResponse, OClientWriteResponse);

bincode_try_from_rpc!(MetricsResponse, ORaftMetrics);
bincode_try_to_rpc!(ORaftMetrics, MetricsResponse);

bincode_try_from_rpc!(AddLearnerResponse, OAddLearnerResponse);
bincode_try_to_rpc!(OAddLearnerResponse, AddLearnerResponse);
