use anyhow::Result;
use clap::{Parser, Subcommand};
// use std::str::FromStr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "gossimini", version, about = "Mini gossip node")]
struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    Run {
        #[arg(long, default_value = "/ip4/0.0.0.0/tcp/0")]
        listen: String,
        dial: Option<String>,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let Args { cmd } = Args::parse();

    match cmd {
        Sub::Run { listen, dial } => {
            let node = node::Node::new().await?;
            let mut node = node;
            node.listen_on(listen.parse().expect("valid multiaddr"))?;
            if let Some(addr) = dial {
                let dial_addr = addr.parse().expect("valid multiaddr");
                node.dial(dial_addr)?;
            }
            node.run().await?;
        }
    }

    Ok(())
}
