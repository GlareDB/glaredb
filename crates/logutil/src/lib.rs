use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

pub fn configure_global_logger(default_level: tracing::Level) {
    let env_filter = EnvFilter::builder()
        .with_default_directive(default_level.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}
