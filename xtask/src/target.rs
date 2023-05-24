use anyhow::{anyhow, Result};

pub enum Arch {
    X86_64,
    Aarch64,
}

impl Arch {
    pub fn from_cfg() -> Result<Arch> {
        if cfg!(target_arch = "x86_64") {
            Ok(Arch::X86_64)
        } else if cfg!(target_arch = "aarch64") {
            Ok(Arch::Aarch64)
        } else {
            Err(anyhow!("unsupported arch"))
        }
    }
}

pub enum Os {
    Mac,
    Linux,
    Windows,
}

impl Os {
    pub fn from_cfg() -> Result<Os> {
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

pub struct Target {
    pub arch: Arch,
    pub os: Os,
}

impl Target {
    pub fn from_cfg() -> Result<Target> {
        Ok(Target {
            arch: Arch::from_cfg()?,
            os: Os::from_cfg()?,
        })
    }

    pub fn protoc_url(&self) -> Result<&'static str> {
        Ok(match (&self.arch, &self.os) {
            (Arch::X86_64, Os::Mac) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-osx-universal_binary.zip",
            (Arch::Aarch64, Os::Mac) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-osx-universal_binary.zip",
            (Arch::X86_64, Os::Linux) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-x86_64.zip",
            (Arch::X86_64, Os::Windows) => "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-win_64.zip",
            _ => return Err(anyhow!("unsupported target")),
        })
    }
}
