pub mod consensus;
mod encode_impl;
pub mod management;

pub mod pb {
    #![allow(clippy::derive_partial_eq_without_eq)]

    tonic::include_proto!("glaredb.raft.network");
    tonic::include_proto!("glaredb.raft.management");
}

pub use consensus::RaftRpcHandler;
pub use management::ManagementRpcHandler;

type TonicResult<T> = Result<tonic::Response<T>, tonic::Status>;
