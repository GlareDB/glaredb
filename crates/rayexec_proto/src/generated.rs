//! Included generated code.

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/rayexec.schema.rs"));
}

pub mod array {
    include!(concat!(env!("OUT_DIR"), "/rayexec.array.rs"));
}

pub mod execution {
    include!(concat!(env!("OUT_DIR"), "/rayexec.execution.rs"));
}

pub mod expr {
    include!(concat!(env!("OUT_DIR"), "/rayexec.expr.rs"));
}

pub mod access {
    include!(concat!(env!("OUT_DIR"), "/rayexec.access.rs"));
}

pub mod binder {
    include!(concat!(env!("OUT_DIR"), "/rayexec.binder.rs"));
}

pub mod ast {
    pub mod raw {
        include!(concat!(env!("OUT_DIR"), "/rayexec.ast.raw.rs"));
    }
}

pub mod logical {
    include!(concat!(env!("OUT_DIR"), "/rayexec.logical.rs"));
}

pub mod foreign {
    include!(concat!(env!("OUT_DIR"), "/rayexec.foreign.rs"));
}

pub mod hybrid {
    include!(concat!(env!("OUT_DIR"), "/rayexec.hybrid.rs"));
}

pub mod catalog {
    include!(concat!(env!("OUT_DIR"), "/rayexec.catalog.rs"));
}
