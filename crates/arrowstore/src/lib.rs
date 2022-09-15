pub mod proto {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("glaredb.arrowstore");
}
pub mod memory;
pub mod serialize;
