fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .emit_rerun_if_changed(true)
        .compile(&["proto/arrowstore.proto"], &["proto"])?;
    Ok(())
}
