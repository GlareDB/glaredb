use std::io::BufWriter;
use std::sync::Arc;

use crossterm::event::{self, Event, KeyModifiers};
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_execution::engine::Engine;
use rayexec_execution::runtime::ExecutionRuntime;
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::ThreadedExecutionRuntime;
use rayexec_shell::lineedit::KeyEvent;
use rayexec_shell::shell::{Shell, ShellSignal};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Simple binary for quickly running arbitrary queries.
fn main() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::ERROR.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_test_writer() // TODO: Actually capture
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let runtime = Arc::new(
        ThreadedExecutionRuntime::try_new()
            .unwrap()
            .with_default_tokio()
            .unwrap(),
    );
    let tokio_handle = runtime.tokio_handle().expect("tokio to be configured");

    // Note we do an explicit clone here to avoid dropping the tokio runtime
    // owned by the execution runtime inside the async context.
    let runtime_clone = runtime.clone();
    let result = tokio_handle.block_on(async move { inner(runtime_clone).await });

    if let Err(e) = result {
        println!("ERROR: {e}");
        std::process::exit(1);
    }
}

fn from_crossterm_keycode(code: crossterm::event::KeyCode) -> KeyEvent {
    match code {
        crossterm::event::KeyCode::Backspace => KeyEvent::Backspace,
        crossterm::event::KeyCode::Enter => KeyEvent::Enter,
        crossterm::event::KeyCode::Left => KeyEvent::Left,
        crossterm::event::KeyCode::Right => KeyEvent::Right,
        crossterm::event::KeyCode::Up => KeyEvent::Up,
        crossterm::event::KeyCode::Down => KeyEvent::Down,
        crossterm::event::KeyCode::Home => KeyEvent::Home,
        crossterm::event::KeyCode::End => KeyEvent::End,
        crossterm::event::KeyCode::Tab => KeyEvent::Tab,
        crossterm::event::KeyCode::BackTab => KeyEvent::BackTab,
        crossterm::event::KeyCode::Delete => KeyEvent::Delete,
        crossterm::event::KeyCode::Insert => KeyEvent::Insert,
        crossterm::event::KeyCode::Char(c) => KeyEvent::Char(c),
        _ => KeyEvent::Unknown,
    }
}

async fn inner(runtime: Arc<dyn ExecutionRuntime>) -> Result<()> {
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?;
    let engine = Engine::new_with_registry(runtime, registry)?;
    let session = engine.new_session()?;

    let (cols, _rows) = crossterm::terminal::size()?;
    let stdout = BufWriter::new(std::io::stdout());

    crossterm::terminal::enable_raw_mode()?;

    let shell = Shell::new(stdout);
    shell.set_cols(cols as usize);
    shell.attach(session, "Rayexec Shell")?;

    let inner_loop = || async move {
        loop {
            match event::read()? {
                Event::Key(event::KeyEvent {
                    code, modifiers, ..
                }) => {
                    let key = if modifiers.contains(KeyModifiers::CONTROL) {
                        match code {
                            event::KeyCode::Char('c') => KeyEvent::CtrlC,
                            _ => KeyEvent::Unknown,
                        }
                    } else {
                        from_crossterm_keycode(code)
                    };

                    match shell.consume_key(key)? {
                        ShellSignal::Continue => (),
                        ShellSignal::ExecutePending => shell.execute_pending().await?,
                        ShellSignal::Exit => break,
                    }
                }
                Event::Resize(cols, _) => shell.set_cols(cols as usize),
                _event => (),
            }
        }
        Ok(())
    };

    let result = inner_loop().await;
    crossterm::terminal::disable_raw_mode()?;

    result
}
