use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};
use xshell::Shell;
use zip::write::SimpleFileOptions;
use zip::ZipWriter;

#[allow(clippy::pedantic)]
#[derive(Parser)]
#[clap(name = "xtask")]
#[clap(about = "Additional cargo tasks", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[allow(clippy::pedantic)]
#[derive(Subcommand)]
enum Commands {
    /// Zip a folder to some destination.
    ///
    /// Useful for cross-platform zipping.
    Zip {
        /// Source folder.
        #[clap(long)]
        src: String,
        /// Destination path.
        #[clap(long)]
        dst: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sh = &Shell::new()?;
    sh.set_var("CARGO_TERM_COLOR", "always"); // Always print output with color.

    match cli.command {
        Commands::Zip { src, dst } => {
            let src = PathBuf::from(src);
            let dst = PathBuf::from(dst);
            zip(&src, &dst)?
        }
    }

    Ok(())
}

/// Zip a source file and write a zip to some destination dest.
fn zip(src_path: &Path, dest_path: &Path) -> Result<()> {
    let file = File::create(dest_path)?;
    let mut writer = ZipWriter::new(io::BufWriter::new(file));
    writer.start_file(
        src_path.file_name().unwrap().to_str().unwrap(),
        SimpleFileOptions::default()
            .unix_permissions(0o755)
            .compression_method(zip::CompressionMethod::Deflated)
            .compression_level(Some(9)),
    )?;
    let mut input = io::BufReader::new(File::open(src_path)?);
    io::copy(&mut input, &mut writer)?;

    writer.finish()?;
    Ok(())
}
