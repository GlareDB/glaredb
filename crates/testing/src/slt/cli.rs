use pgsrv::auth::{LocalAuthenticator, PasswordlessAuthenticator, SingleUserAuthenticator};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use clap::Parser;
use glaredb::server::{Server, ServerConfig};
use tokio::{
    net::TcpListener,
    runtime::Builder,
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_postgres::{config::Config as ClientConfig, NoTls};
use uuid::Uuid;

use crate::slt::test::{Test, TestHooks};

#[derive(Parser)]
#[clap(name = "slt-runner")]
#[clap(about = "Run sqllogictests against a GlareDB server", long_about = None)]
pub struct Cli {
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// TCP address to bind to for the GlareDB server.
    ///
    /// Omitting this will attempt to bind to any available port.
    #[clap(long, value_parser)]
    bind_embedded: Option<String>,

    /// Address of metastore to use.
    ///
    /// If not provided, a Metastore will be spun up automatically.
    #[clap(long, value_parser)]
    metastore_addr: Option<String>,

    /// Whether or not to keep the embedded GlareDB server running after a
    /// failure.
    ///
    /// This allow for an external client to connect to allow for additional
    /// debugging.
    #[clap(long, value_parser)]
    keep_running: bool,

    /// Connection string to use for connecting to the database.
    ///
    /// If provided, an embedded server won't be started.
    #[clap(short, long, value_parser)]
    connection_string: Option<String>,

    /// List all the tests for the pattern (Dry Run).
    #[clap(short, long, value_parser)]
    list: bool,

    /// Number of jobs to run in parallel
    ///
    /// To run the max possible jobs, set it to 0. By default, this argument is
    /// set to 0 to run max possible jobs. Set it to `1` to run sequentially.
    #[clap(short, long, value_parser, default_value_t = 0)]
    jobs: u8,

    /// Timeout (exit) after this number of seconds.
    #[clap(long, value_parser, default_value_t = 5 * 60)]
    timeout: u64,

    /// Exclude these tests from the run.
    #[clap(short, long, value_parser)]
    exclude: Vec<String>,

    /// Tests to run.
    ///
    /// Provide a glob like regex for test name. If ommitted, runs all the
    /// tests. This is similar to providing parameter as `*`.
    tests_pattern: Option<String>,
}

impl Cli {
    pub fn run(tests: BTreeMap<String, Test>, hooks: TestHooks) -> Result<()> {
        let cli = Self::parse();

        let tests = cli.collect_tests(tests)?;

        if cli.list {
            for (test_name, _) in tests {
                println!("{test_name}");
            }
            return Ok(());
        }

        if tests.is_empty() {
            return Err(anyhow!("No tests to run. Exiting..."));
        }

        logutil::init(cli.verbose, false);

        // Abort the program on panic. This will ensure that slt tests will
        // never pass if there's a panic somewhere.
        std::panic::set_hook(Box::new(|info| {
            let backtrace = std::backtrace::Backtrace::force_capture();
            println!("Info: {}\n\nBacktrace:{}", info, backtrace);
            std::process::abort();
        }));

        Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async move { cli.run_tests(tests, hooks).await })
    }

    fn collect_tests(&self, tests: BTreeMap<String, Test>) -> Result<Vec<(String, Test)>> {
        let mut tests: Vec<_> = if let Some(pattern) = &self.tests_pattern {
            let pattern = glob::Pattern::new(pattern)
                .map_err(|e| anyhow!("Invalid glob pattern `{pattern}`: {e}"))?;
            tests
                .into_iter()
                .filter(|(k, _v)| pattern.matches(k))
                .collect()
        } else {
            tests.into_iter().collect()
        };
        // See if we want to exclude anything
        for pattern in &self.exclude {
            let pattern = glob::Pattern::new(pattern)
                .map_err(|e| anyhow!("Invalid glob pattern `{pattern}`: {e}"))?;
            tests.retain(|(k, _v)| !pattern.matches(k));
        }
        Ok(tests)
    }

    async fn run_tests(self, tests: Vec<(String, Test)>, hooks: TestHooks) -> Result<()> {
        // Temp directory for metastore
        let temp_dir = tempfile::tempdir()?;

        let configs: HashMap<String, ClientConfig> =
            if let Some(connection_string) = &self.connection_string {
                let config: ClientConfig = connection_string.parse()?;
                let mut configs = HashMap::with_capacity(tests.len());
                tests.iter().for_each(|(name, _)| {
                    configs.insert(name.clone(), config.clone());
                });
                configs
            } else {
                let pg_listener = TcpListener::bind(
                    self.bind_embedded
                        .unwrap_or_else(|| "localhost:0".to_string()),
                )
                .await?;
                let pg_addr = pg_listener.local_addr()?;
                let server_conf = ServerConfig { pg_listener };

                let server = Server::connect(
                    self.metastore_addr,
                    None,
                    Box::new(SingleUserAuthenticator {
                        user: "glaredb".to_string(),
                        password: "glaredb".to_string(),
                    }),
                    Some(temp_dir.path().to_path_buf()),
                    None,
                    /* integration_testing = */ true,
                )
                .await?;
                tokio::spawn(server.serve(server_conf));

                let host = pg_addr.ip().to_string();
                let port = pg_addr.port();

                let mut config = ClientConfig::new();
                config
                    .user("glaredb")
                    .password("glaredb")
                    .dbname("glaredb")
                    .host(&host)
                    .port(port);

                let mut configs = HashMap::new();
                tests.iter().for_each(|(name, _)| {
                    let mut cfg = config.clone();
                    let db_id = Uuid::new_v4().to_string();
                    cfg.dbname(&db_id);
                    configs.insert(name.clone(), cfg);
                });
                configs
            };

        let (jobs_tx, mut jobs_rx) = mpsc::unbounded_channel();
        let mut total_jobs = if self.jobs > 0 { self.jobs } else { u8::MAX };

        let num_tests = tests.len();
        let mut results = Vec::with_capacity(num_tests);

        let start = Instant::now();
        let timeout_at = start + Duration::from_secs(self.timeout);

        type Res = (String, Result<()>);
        async fn recv(
            rx: &mut mpsc::UnboundedReceiver<Res>,
            deadline: Instant,
        ) -> Result<Option<Res>> {
            let res = tokio::time::timeout_at(deadline, rx.recv()).await?;
            Ok(res)
        }

        let hooks = Arc::new(hooks);

        for (test_name, test) in tests {
            if total_jobs == 0 {
                // Wait to receive a result
                let res = recv(&mut jobs_rx, timeout_at).await?.unwrap();
                total_jobs += 1;
                results.push(res);
            }

            // Spawn a new job.
            total_jobs -= 1;
            let cfg = configs.get(&test_name).unwrap().clone();
            let tx = jobs_tx.clone();
            let hooks = Arc::clone(&hooks);
            tokio::spawn(async move {
                let res = Self::run_test(&test_name, test, cfg, hooks).await;
                tx.send((test_name.clone(), res)).unwrap();
            });
        }

        // Drain all the results.
        while let Some(res) = recv(&mut jobs_rx, timeout_at).await? {
            results.push(res);

            // Received everything? Close the channel and exit!
            if results.len() == num_tests {
                jobs_rx.close();
                break;
            }
        }

        let mut errored = false;
        let errors = results.iter().filter_map(|(name, res)| match res {
            Ok(_) => None,
            Err(e) => Some((name, e)),
        });

        for (name, error) in errors {
            errored = true;
            tracing::error!(%error, "Error while running test `{name}`");

            // If keep running, then connect to the client and do it!
            if self.connection_string.is_none() && self.keep_running {
                let conf = configs.get(name).unwrap();
                let port = conf.get_ports().first().unwrap();
                let password = String::from_utf8_lossy(conf.get_password().unwrap()).into_owned();
                let conn_string = format!(
                    "host=localhost port={} dbname={} user={} password={}",
                    port,
                    conf.get_dbname().unwrap(),
                    conf.get_user().unwrap(),
                    password
                );
                println!("connect to the database using connection string:\n  \"{conn_string}\"\n");
            }
        }

        if errored {
            if self.connection_string.is_none() && self.keep_running {
                println!("keeping the server running.");
                println!("connect to the corresponding database using the given connection strings with each error");
                println!("CTRL-C to exit");
                tokio::signal::ctrl_c().await?;
            }
            Err(anyhow!("Test failures"))
        } else {
            let time_taken = Instant::now().duration_since(start);
            eprintln!("Tests took {time_taken:?} to run");
            Ok(())
        }
    }

    async fn run_test(
        test_name: &str,
        test: Test,
        client_config: ClientConfig,
        hooks: Arc<TestHooks>,
    ) -> Result<()> {
        let start = Instant::now();

        let mut local_vars = HashMap::new();

        // Run the actual test
        let (mut client, conn) = client_config.connect(NoTls).await?;
        let (conn_err_tx, mut conn_err_rx) = oneshot::channel();
        tokio::spawn(async move { conn_err_tx.send(conn.await) });

        let hooks = hooks
            .iter()
            .filter(|(pattern, _)| pattern.matches(test_name));

        tracing::info!(%test_name, "Running test");

        // Run the pre-test hooks
        for (pattern, hook) in hooks.clone() {
            tracing::debug!(%pattern, %test_name, "Running pre hook for test");
            hook.pre(&client_config, &mut client, &mut local_vars)
                .await?;
        }

        // Run the actual test
        test.execute(&client_config, &mut client, &mut local_vars)
            .await?;

        // Run the post-test hooks
        for (pattern, hook) in hooks {
            tracing::debug!(%pattern, %test_name, "Running post hook for test");
            hook.post(&client_config, &mut client, &local_vars).await?;
        }

        if let Ok(result) = conn_err_rx.try_recv() {
            // Handle connection error
            match result {
                Ok(()) => return Err(anyhow!("Client connection unexpectedly closed")),
                Err(e) => return Err(e.into()),
            }
        }

        let time_taken = Instant::now().duration_since(start);
        tracing::info!(?time_taken, %test_name, "Done executing");

        Ok(())
    }
}
