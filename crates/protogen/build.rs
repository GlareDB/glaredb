fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                // Common
                "proto/common/arrow.proto",
                // Metastore
                "proto/metastore/catalog.proto",
                "proto/metastore/service.proto",
                "proto/metastore/storage.proto",
                "proto/metastore/options.proto",
                // rpcsrv
                "proto/rpcsrv/service.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
