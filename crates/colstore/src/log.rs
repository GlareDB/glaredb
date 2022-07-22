use anyhow::{anyhow, Result};
use crc32fast::Hasher;
use log::debug;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn(u64);

/// Name of the lockfile in the log dir. Only a single log writer may be used
/// for a log directory.
const LOCK_PATH: &str = "lock";

/// Name of the log file.
const LOG_PATH: &str = "log";

#[derive(Debug)]
#[repr(u8)]
enum LogSentinel {
    Unknown = 0,
    Written = 1,
}

impl LogSentinel {
    fn from_byte(b: u8) -> LogSentinel {
        match b {
            1 => LogSentinel::Written,
            _ => LogSentinel::Unknown,
        }
    }
}

/// Configuration options for the log.
#[derive(Debug)]
pub struct LogConfig {
    /// Directory to store log files.
    pub dir: PathBuf,
    pub buf_size: usize,
}

#[derive(Debug, Clone)]
pub struct LogWriter {
    conf: Arc<LogConfig>,
    log_file: Arc<Mutex<BufWriter<File>>>,
    flushed_lsn: Arc<AtomicU64>,
    lsn: Arc<AtomicU64>,
}

impl LogWriter {
    /// Open a log writer using the given config.
    ///
    /// If the log directory doesn't exist, create it.
    pub fn open(conf: LogConfig) -> Result<Self> {
        debug!("opening log directory at: {}", conf.dir.to_string_lossy());
        match fs::read_dir(&conf.dir) {
            Ok(_) => debug!("reusing log directory"),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                debug!("creating log directory");
                fs::create_dir_all(&conf.dir)?;
            }
            Err(e) => return Err(e.into()),
        }

        // Try to create lock file, erroring if it exists.
        let _ = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(conf.dir.join(LOCK_PATH))?;

        // TODO: Multiple logs in the same dir.
        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(conf.dir.join(LOG_PATH))?;

        let buf_size = conf.buf_size;
        Ok(LogWriter {
            conf: Arc::new(conf),
            log_file: Arc::new(Mutex::new(BufWriter::with_capacity(buf_size, log_file))),
            flushed_lsn: Arc::new(AtomicU64::new(0)),
            lsn: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Append an entry to the log.
    pub fn append(&self, entry: &[u8]) -> Result<Lsn> {
        let crc = crc32fast::hash(entry).to_le_bytes();
        let len = (entry.len() as u64).to_le_bytes();

        // Always acquire lock before incrementing lsn.
        let mut log_file = self.log_file.lock();
        let lsn = self.lsn.fetch_add(1, Ordering::Relaxed);

        log_file.write_all(&crc)?;
        log_file.write_all(&lsn.to_le_bytes())?;
        log_file.write_all(&len)?;
        log_file.write_all(entry)?;

        Ok(Lsn(lsn))
    }

    pub fn flush(&self) -> Result<()> {
        let mut log_file = self.log_file.lock();
        log_file.flush()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LogReader {
    conf: LogConfig,
    log_file: BufReader<File>,
}

impl LogReader {
    pub fn open(conf: LogConfig) -> Result<LogReader> {
        debug!("opening log directoyr for read at: {:?}", conf.dir);
        unimplemented!()
    }
}
