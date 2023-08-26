use clap::Parser;
use cw_unsuck::{describe::Sync, Cli};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Cli::parse();

    match args.command {
        cw_unsuck::Commands::Sync {} => Sync {}.sync().await?,
    }
    // ... make some calls with the client

    Ok(())
}
