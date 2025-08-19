use anyhow::Result;
use clap::{Args as ClapArgs, Parser, Subcommand};
// use std::str::FromStr;
use core::TopicId;
use node::Node;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
#[derive(Parser)]
#[command(name = "gossimini", version, about = "Mini gossip node")]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Subscribe to a topic
    Sub(SubArgs),
    /// Publish data to a topic
    Pub(PubArgs),
}

#[derive(ClapArgs)]
struct SubArgs {
    /// Topic to subscribe to
    #[arg(long)]
    topic: String,

    /// Listen address (multiaddr)
    #[arg(long, default_value = "/ip4/0.0.0.0/tcp/0")]
    listen: String,

    /// Optional dial address (multiaddr)
    #[arg(long)]
    dial: Option<String>,
}

#[derive(ClapArgs)]
struct PubArgs {
    /// Topic to publish to
    #[arg(long)]
    topic: String,

    /// Data to publish
    #[arg(long)]
    data: String,

    /// Optional dial address (multiaddr)
    #[arg(long)]
    dial: Option<String>,
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let Args { cmd } = Args::parse();

    match cmd {
        Command::Sub(args) => {
            let mut node = Node::new().await?;
            node.listen_on(args.listen.parse().expect("valid multiaddr"))?;
            if let Some(dial) = args.dial {
                node.dial(dial.parse().expect("valid multiaddr"))?;
            }
            node.set_autosub(TopicId::new(args.topic.clone()));
            node.run().await?;
        }
        Command::Pub(args) => {
            use node::Node;
            let mut node = Node::new().await?;
            if let Some(dial) = args.dial {
                node.dial(dial.parse().expect("valid multiaddr"))?;
            }
            node.set_autopub(TopicId::new(args.topic.clone()), args.data.into_bytes());
            node.run().await?;
        }
    }

    Ok(())
}
