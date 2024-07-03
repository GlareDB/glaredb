use std::io::{self, Write};
use tracing::Level;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::FmtSubscriber;
use wasm_bindgen::prelude::*;
use web_sys::{console, js_sys::Array};

#[wasm_bindgen]
pub fn init_console_tracing() {
    let default_level = tracing::Level::TRACE;
    let env_filter = EnvFilter::builder()
        .with_default_directive(default_level.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_writer(MakeConsoleWriter::new(default_level))
        .without_time()
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[derive(Clone, Copy, Debug)]
pub struct MakeConsoleWriter {
    default_level: Level,
}

impl MakeConsoleWriter {
    pub fn new(level: Level) -> Self {
        MakeConsoleWriter {
            default_level: level,
        }
    }
}

impl<'a> MakeWriter<'a> for MakeConsoleWriter {
    type Writer = ConsoleWriter;

    fn make_writer(&'a self) -> Self::Writer {
        ConsoleWriter {
            level: self.default_level,
            buf: Vec::with_capacity(1024),
        }
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        ConsoleWriter {
            level: *meta.level(),
            buf: Vec::with_capacity(1024),
        }
    }
}

pub struct ConsoleWriter {
    level: Level,
    buf: Vec<u8>,
}

impl io::Write for ConsoleWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let data = std::str::from_utf8(&self.buf)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 data"))?;

        let array = Array::of1(&JsValue::from_str(data));

        match self.level {
            Level::TRACE => console::debug(&array),
            Level::DEBUG => console::debug(&array),
            Level::INFO => console::log(&array),
            Level::WARN => console::warn(&array),
            Level::ERROR => console::error(&array),
        }

        Ok(())
    }
}

impl Drop for ConsoleWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
