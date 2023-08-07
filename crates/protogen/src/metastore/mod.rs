/// Generate types.
pub mod gen {
    pub mod arrow {
        tonic::include_proto!("metastore.arrow");
    }

    pub mod catalog {
        tonic::include_proto!("metastore.catalog");
    }

    pub mod service {
        tonic::include_proto!("metastore.service");
    }

    pub mod storage {
        tonic::include_proto!("metastore.storage");
    }

    pub mod options {
        tonic::include_proto!("metastore.options");
    }
}

pub mod strategy;
pub mod types;
