//! Included generated code.

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/rayexec.schema.rs"));
}

pub mod physical_type {
    include!(concat!(env!("OUT_DIR"), "/rayexec.physical_type.rs"));
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

pub mod physical_expr {
    include!(concat!(env!("OUT_DIR"), "/rayexec.physical_expr.rs"));
}

pub mod functions {
    include!(concat!(env!("OUT_DIR"), "/rayexec.functions.rs"));
}

pub mod access {
    include!(concat!(env!("OUT_DIR"), "/rayexec.access.rs"));
}

pub mod resolver {
    include!(concat!(env!("OUT_DIR"), "/rayexec.resolver.rs"));
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
