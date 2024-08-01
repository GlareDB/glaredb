//! Generated code for the Arrow IPC format.

#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(non_camel_case_types)]
#![allow(clippy::all)]

mod File;
mod Message;
mod Schema;
mod SparseTensor;
mod Tensor;

pub mod file {
    pub use super::File::*;
}

pub mod message {
    pub use super::Message::*;
}

pub mod schema {
    pub use super::Schema::*;
}

pub mod sparse_tensor {
    pub use super::SparseTensor::*;
}

pub mod tensor {
    pub use super::Tensor::*;
}
