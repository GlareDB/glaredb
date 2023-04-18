fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &[
                "proto/arrow.proto",
                "proto/catalog.proto",
                "proto/service.proto",
                "proto/storage.proto",
                "proto/options.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
