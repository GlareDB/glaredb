fn main() {
    if let Err(e) = prost_build::compile_protos(
        &[
            "proto/schema.proto",
            "proto/array.proto",
            "proto/execution.proto",
            "proto/expr.proto",
            "proto/physical_expr.proto",
            "proto/functions.proto",
            "proto/access.proto",
            "proto/resolver.proto",
            "proto/ast/raw.proto",
            "proto/logical.proto",
            "proto/foreign.proto",
            "proto/hybrid.proto",
            "proto/catalog.proto",
        ],
        &["proto"],
    ) {
        // Printing out the error here instead of returning it so that we print
        // out the Display impl of the error which is easier to read (properly
        // formatted newlines).
        println!("{}", e);
        std::process::exit(1);
    }
}
