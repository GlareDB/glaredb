fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .compile(
            &[
                "proto/raft/network.proto",
            ],
            &["proto"],
        )?;

    tonic_build::configure()
        .type_attribute("AddLearnerRequest", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &[
                "proto/raft/management.proto",
            ],
            &["proto"],
        )?;

    tonic_build::configure()
        .compile(
            &[
                "proto/glaredb.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
