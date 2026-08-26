mod cln;
mod descriptor;
mod events;
mod model;
mod tracker;

use crate::events::{on_bwatch_block_processed, on_bwatch_block_reverted, on_bwatch_match};
use crate::tracker::{
    AppState, ack_incident, health, inspect, list, load, metrics, reconcile, register,
    restore_watches, unregister, update,
};
use anyhow::{Context, Result};
use cln_plugin::{Builder, RpcMethodBuilder};
use std::path::PathBuf;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let Some(configured) = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-register", register)
                .description("Register a checksummed output descriptor with bwatch")
                .usage("name descriptor birthheight [lookahead] [confirmations]"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-unregister", unregister)
                .description("Stop tracking a descriptor and its discovered UTXOs")
                .usage("name"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-inspect", inspect)
                .description("Inspect a registered descriptor")
                .usage("name"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-update", update)
                .description("Change a registered descriptor's lookahead or confirmation depth")
                .usage("name [lookahead] [confirmations]"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-reconcile", reconcile)
                .description("Reconcile watches and retry pending Bookkeeper movements")
                .usage("[name]"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-list", list)
                .description("List all registered descriptors")
                .usage(""),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-health", health)
                .description("Report tracker, descriptor, and bwatch health")
                .usage(""),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-metrics", metrics)
                .description("Return bounded Tracker metrics for a Prometheus collector")
                .usage(""),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("tracker-ack-incident", ack_incident)
                .description("Acknowledge and clear a descriptor incident")
                .usage("name"),
        )
        .subscribe("bwatch_match", on_bwatch_match)
        .subscribe("bwatch_block_processed", on_bwatch_block_processed)
        .subscribe("bwatch_block_reverted", on_bwatch_block_reverted)
        .dynamic()
        .configure()
        .await?
    else {
        return Ok(());
    };

    let configuration = configured.configuration();
    let rpc_path = PathBuf::from(&configuration.lightning_dir).join(&configuration.rpc_file);
    let state = AppState::new(rpc_path, configuration.network);
    load(&state)
        .await
        .context("loading and validating tracker state")?;
    let plugin = configured.start(state).await?;
    restore_watches(plugin.state())
        .await
        .context("restoring tracker bwatch registrations")?;
    plugin.join().await
}
