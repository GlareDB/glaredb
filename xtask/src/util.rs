use anyhow::Result;
use std::env;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use zip::write::FileOptions;
use zip::ZipWriter;

/// Get the path to the root of the project.
pub fn project_root() -> PathBuf {
    Path::new(
        &env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned()),
    )
    .ancestors()
    .nth(1)
    .unwrap()
    .to_path_buf()
}

/// Zip a source file and write a zip to some destination dest.
pub fn zip(src_path: &Path, dest_path: &Path) -> Result<()> {
    let file = File::create(dest_path)?;
    let mut writer = ZipWriter::new(io::BufWriter::new(file));
    writer.start_file(
        src_path.file_name().unwrap().to_str().unwrap(),
        FileOptions::default()
            .unix_permissions(0o755)
            .compression_method(zip::CompressionMethod::Deflated)
            .compression_level(Some(9)),
    )?;
    let mut input = io::BufReader::new(File::open(src_path)?);
    io::copy(&mut input, &mut writer)?;

    writer.finish()?;
    Ok(())
}
