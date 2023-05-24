use anyhow::Result;
use clap::{Parser, Subcommand};
use std::env;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use target::Target;
use xshell::{cmd, Shell};
use zip::ZipArchive;

mod target;

#[derive(Parser)]
#[clap(name = "xtask")]
#[clap(about = "Additional cargo tasks", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build glaredb.
    Build {
        /// Build with the release profile.
        #[clap(short, long)]
        release: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sh = &Shell::new()?;
    sh.change_dir(project_root());

    let target = Target::from_cfg()?;

    match cli.command {
        Commands::Build { release } => build(sh, &target, release)?,
    }

    Ok(())
}

fn build(sh: &Shell, target: &Target, release: bool) -> Result<()> {
    ensure_protoc(sh, target)?;
    if release {
        cmd!(sh, "cargo build --release --bin glaredb").run()?;
    } else {
        cmd!(sh, "cargo build --bin glaredb").run()?;
    }
    Ok(())
}

/// Check if protoc is in path. If not, download and set the PROTOC env var.
fn ensure_protoc(sh: &Shell, target: &Target) -> Result<()> {
    const PROTOC_OUT_DIR: &str = "deps/protoc";
    const PROTOC_PATH: &str = "deps/protoc/bin/protoc";

    if cmd!(sh, "protoc --version").run().is_err() {
        if sh.path_exists(project_root().join(PROTOC_PATH)) {
            println!("Downloaded protoc already exists");
            return Ok(());
        }

        println!("Missing protoc, downloading...");
        sh.remove_path(project_root().join(PROTOC_OUT_DIR))?;
        let res = reqwest::blocking::get(target.protoc_url()?)?;
        ZipArchive::new(Cursor::new(res.bytes()?))?.extract(project_root().join(PROTOC_OUT_DIR))?;
        sh.set_var("PROTOC", project_root().join(PROTOC_PATH));
    }
    Ok(())
}

fn project_root() -> PathBuf {
    Path::new(
        &env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned()),
    )
    .ancestors()
    .nth(1)
    .unwrap()
    .to_path_buf()
}
