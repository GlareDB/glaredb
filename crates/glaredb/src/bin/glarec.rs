use anyhow::{anyhow, Result};
use clap::Parser;
use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use glaredb::message::{new_client_stream, Request, Response, ResponseInner, SerializableBatch};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::net::TcpStream;

#[derive(Parser)]
#[clap(name = "GlareDB Client")]
#[clap(about = "Client foGlareDB SQL engine")]
struct Cli {
    #[clap(short, long, value_parser, default_value_t = String::from("localhost:6570"))]
    connect: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    run_repl(&cli.connect).await
}

async fn run_repl(connect_addr: &str) -> Result<()> {
    let socket = TcpStream::connect(connect_addr).await?;
    let mut stream = new_client_stream(socket);

    let mut rl = Editor::<()>::new();

    loop {
        let readline = rl.readline("gl> ");
        match readline {
            Ok(line) => {
                let req = Request::Execute(line);
                stream.send(req).await?;
                // TODO: Get all responses.
                if let Some(resp) = stream.try_next().await? {
                    println!("{}", resp);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
