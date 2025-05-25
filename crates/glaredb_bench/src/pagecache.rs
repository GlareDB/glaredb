use std::process::Command;

use glaredb_error::{DbError, Result};

pub fn drop_page_cache() -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let status = Command::new("sync").status()?;
        if !status.success() {
            Err(DbError::new(format!(
                "'sync' failed with status {}",
                status
            )))
        }

        // Requires sudo
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/sys/vm/drop_caches")?;
        // "3" = drop pagecache + dentries + inodes
        f.write_all(b"3\n")?;
        f.flush()?;
        Ok(())
    }

    #[cfg(target_os = "macos")]
    {
        // Use purge, requires xcode tools and sudo.
        let status = Command::new("purge").status()?;
        if !status.success() {
            Err(DbError::new(format!(
                "'purge' failed with status {}",
                status
            )))
        } else {
            Ok(())
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(DbError::new(
            "dropping file cache is only implemented for Linux and macOS",
        ))
    }
}
