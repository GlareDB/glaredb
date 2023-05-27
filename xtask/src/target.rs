use anyhow::{anyhow, Result};
use std::env;

pub struct Target {
    arch: Arch,
    os: Os,
}

impl Target {
    pub fn from_cfg() -> Result<Target> {
        Ok(Target {
            arch: Arch::from_cfg()?,
            os: Os::from_cfg()?,
        })
    }

    /// Get the target triple to use for building the dist binary for releases.
    ///
    /// This can be overridden by the `DIST_TARGET_TRIPLE` environment
    /// variable.
    pub fn dist_target_triple(&self) -> Result<String> {
        if let Ok(triple) = env::var("DIST_TARGET_TRIPLE") {
            println!("Using target triple override: {triple}");
            return Ok(triple);
        }

        Ok(match (&self.arch, &self.os) {
            (Arch::X86_64, Os::Mac) => "x86_64-apple-darwin",
            (Arch::Aarch64, Os::Mac) => "aarch64-apple-darwin",
            (Arch::X86_64, Os::Linux) => "x86_64-unknown-linux-gnu",
            (Arch::Aarch64, Os::Linux) => "aarch64-unknown-linux-gnu",
            (Arch::X86_64, Os::Windows) => "x86_64-pc-windows-msvc",
            _ => return Err(anyhow!("unsupported target")),
        }
        .to_string())
    }

    /// Get the url to use for downloading a protoc binary.
    pub fn protoc_url(&self) -> Result<&'static str> {
        Ok(match (&self.arch, &self.os) {
            (Arch::X86_64, Os::Mac) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-osx-universal_binary.zip",
            (Arch::Aarch64, Os::Mac) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-osx-universal_binary.zip",
            (Arch::X86_64, Os::Linux) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-x86_64.zip",
            (Arch::Aarch64, Os::Linux) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-aarch_64.zip",
            (Arch::X86_64, Os::Windows) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-win64.zip",
            _ => return Err(anyhow!("unsupported target")),
        })
    }

    pub fn dist_zip_name(&self) -> Result<String> {
        let target = self.dist_target_triple()?;
        Ok(format!("glaredb-{target}.zip"))
    }
}

enum Arch {
    X86_64,
    Aarch64,
}

impl Arch {
    fn from_cfg() -> Result<Arch> {
        if cfg!(target_arch = "x86_64") {
            Ok(Arch::X86_64)
        } else if cfg!(target_arch = "aarch64") {
            Ok(Arch::Aarch64)
        } else {
            Err(anyhow!("unsupported arch"))
        }
    }
}

enum Os {
    Mac,
    Linux,
    Windows,
}

impl Os {
    fn from_cfg() -> Result<Os> {
        if cfg!(target_os = "macos") {
            Ok(Os::Mac)
        } else if cfg!(target_os = "linux") {
            Ok(Os::Linux)
        } else if cfg!(target_os = "windows") {
            Ok(Os::Windows)
        } else {
            Err(anyhow!("unsupported os"))
        }
    }
}
