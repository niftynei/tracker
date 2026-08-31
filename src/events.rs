use crate::cln;
use crate::model::{
    BwatchBlock, BwatchMatch, DescriptorStatus, MAX_LOOKAHEAD, MovementKind, PendingMovement,
    TrackedUtxo, WatchOwner, outpoint_owner, parse_owner, script_owner,
};
use crate::tracker::{
    AppState, current_bwatch_height, deliver_and_store, desired_range_end, drain_pending_movements,
    ensure_no_script_overlap, record_reorg, report_descriptor_failure, report_descriptor_success,
    validate_config,
};
use anyhow::{Context, Result, anyhow, ensure};
use cln_plugin::Plugin;
use serde::de::DeserializeOwned;
use serde_json::Value;

fn parse_notification<T: DeserializeOwned>(topic: &str, value: Value) -> Result<T> {
    let payload = value.get(topic).cloned().unwrap_or(value);
    serde_json::from_value(payload).with_context(|| format!("invalid {topic} notification"))
}

fn ensure_derivation_index_is_watched(derivation_index: u32, range_end: u32) -> Result<()> {
    ensure!(
        derivation_index < range_end,
        "bwatch match index is outside watched descriptor range"
    );
    Ok(())
}

fn is_at_or_after_birthheight(blockheight: u32, birthheight: u32) -> bool {
    blockheight >= birthheight
}

fn deposit_movement_id(outpoint: &str) -> String {
    format!("deposit:{outpoint}")
}

fn spend_movement_id(outpoint: &str, spending_txid: &str) -> String {
    format!("spend:{outpoint}:{spending_txid}")
}

fn descriptor_name(owner: &WatchOwner) -> &str {
    match owner {
        WatchOwner::Script { name, .. } | WatchOwner::Outpoint { name, .. } => name,
    }
}

pub async fn on_bwatch_match(plugin: Plugin<AppState>, value: Value) -> Result<()> {
    let event: BwatchMatch = parse_notification("bwatch_match", value)?;
    let Some(owner) = parse_owner(&event.owner) else {
        return Ok(());
    };
    let name = descriptor_name(&owner).to_owned();
    let result = match owner {
        WatchOwner::Script { name, .. }
            if event.historical_scan && event.watch_type == "outpoint" =>
        {
            let raw = event
                .tx
                .as_deref()
                .context("historical outpoint match omitted transaction")?;
            let input_index = event
                .index
                .context("historical outpoint match omitted input index")?;
            let tx = cln::parse_transaction(raw)?;
            let input = tx
                .input
                .get(usize::try_from(input_index).context("invalid input index")?)
                .context("historical spend input is outside transaction")?;
            handle_spend(
                plugin.clone(),
                event,
                name,
                input.previous_output.to_string(),
            )
            .await
        }
        WatchOwner::Script {
            name,
            branch,
            index,
        } => handle_deposit(plugin.clone(), event, name, branch, index).await,
        WatchOwner::Outpoint { name, outpoint } => {
            handle_spend(plugin.clone(), event, name, outpoint).await
        }
    };
    if let Err(error) = &result {
        let message = format!("{error:#}");
        let operation = if message.contains("Bookkeeper") {
            "bookkeeper_event"
        } else {
            "bwatch_event"
        };
        report_descriptor_failure(plugin.state(), &name, operation, error, false).await;
    } else {
        report_descriptor_success(plugin.state(), &name, &["bookkeeper_event", "bwatch_event"])
            .await;
    }
    result
}

async fn handle_deposit(
    plugin: Plugin<AppState>,
    event: BwatchMatch,
    name: String,
    branch: u32,
    derivation_index: u32,
) -> Result<()> {
    ensure!(
        event.watch_type == "scriptpubkey",
        "unexpected watch type for script owner"
    );
    let raw = event
        .tx
        .as_deref()
        .context("script match omitted transaction")?;
    let output_index = event.index.context("script match omitted output index")?;
    let tx = cln::parse_transaction(raw)?;
    let output = tx
        .output
        .get(usize::try_from(output_index).context("invalid output index")?)
        .context("bwatch output index is outside transaction")?;
    let outpoint = format!("{}:{output_index}", tx.compute_txid());
    let amount_msat = output
        .value
        .to_sat()
        .checked_mul(1_000)
        .context("output amount does not fit in millisatoshis")?;
    let movement_id = deposit_movement_id(&outpoint);
    let _operation = plugin.state().lock_descriptor(&name).await;
    let records = plugin.state().records_snapshot().await;
    let original = records
        .get(&name)
        .cloned()
        .with_context(|| format!("unknown descriptor '{name}' in bwatch owner"))?;
    if original.status == DescriptorStatus::Deleting
        || !is_at_or_after_birthheight(event.blockheight, original.config.birthheight)
    {
        return Ok(());
    }
    ensure!(
        matches!(
            original.status,
            DescriptorStatus::Active | DescriptorStatus::Syncing
        ),
        "descriptor '{name}' cannot accept matches"
    );
    ensure_derivation_index_is_watched(
        derivation_index,
        if event.historical_scan {
            MAX_LOOKAHEAD
        } else {
            original.range_end
        },
    )?;
    let descriptor = validate_config(&original.config, &plugin.state().network)?;
    let expected = descriptor.derive_one(branch, derivation_index)?;
    ensure!(
        expected.scriptpubkey == hex::encode(output.script_pubkey.as_bytes()),
        "bwatch match script does not match descriptor"
    );
    if let Some(existing) = original.utxos.get(&outpoint) {
        ensure!(
            existing.amount_msat == amount_msat,
            "persisted descriptor output amount differs from bwatch match"
        );
        if original.pending_movements.contains_key(&movement_id) {
            let tip = current_bwatch_height(plugin.state(), event.blockheight).await;
            deliver_and_store(plugin.state(), original, tip).await?;
        }
        return Ok(());
    }

    let timestamp = match event.timestamp {
        Some(timestamp) => timestamp,
        None => cln::block_timestamp(&plugin.state().rpc_path, event.blockheight).await?,
    };
    let mut updated = original;
    let mut added_scripts = Vec::new();
    if updated
        .last_used_index
        .is_none_or(|last| derivation_index > last)
    {
        let new_end = desired_range_end(Some(derivation_index), updated.config.lookahead)?;
        let old_end = updated.range_end;
        added_scripts = descriptor.derive_range(old_end, new_end)?;
        ensure_no_script_overlap(&name, &added_scripts, &records, &plugin.state().network)?;
        updated.last_used_index = Some(derivation_index);
        updated.range_end = new_end;
        updated.status = DescriptorStatus::Syncing;
    }
    updated.utxos.insert(
        outpoint.clone(),
        TrackedUtxo {
            outpoint: outpoint.clone(),
            amount_msat,
            derivation_index,
            branch,
            deposit_height: event.blockheight,
            spent_by: None,
            spent_height: None,
        },
    );
    updated.pending_movements.insert(
        movement_id.clone(),
        PendingMovement {
            id: movement_id.clone(),
            kind: MovementKind::Deposit,
            outpoint: outpoint.clone(),
            amount_msat,
            blockheight: event.blockheight,
            timestamp,
            spending_txid: None,
        },
    );
    cln::save_record(&plugin.state().rpc_path, &mut updated)
        .await
        .context("persisting observed descriptor deposit")?;
    plugin.state().put_record(updated.clone()).await;

    let added_watches = added_scripts
        .into_iter()
        .map(|script| {
            (
                script_owner(&name, script.branch, script.index),
                script.scriptpubkey,
            )
        })
        .collect::<Vec<_>>();
    if !event.historical_scan {
        cln::add_script_watches(
            &plugin.state().rpc_path,
            &added_watches,
            updated.config.birthheight,
        )
        .await
        .context("extending descriptor lookahead frontier")?;
        let added_owners = added_watches
            .iter()
            .map(|(owner, _)| owner.clone())
            .collect::<Vec<_>>();
        cln::rescan_watch_owners(
            &plugin.state().rpc_path,
            &added_owners,
            updated.config.birthheight,
        )
        .await
        .context("rescanning extended descriptor lookahead frontier")?;
        cln::add_outpoint_watch(
            &plugin.state().rpc_path,
            &outpoint_owner(&name, &outpoint),
            &outpoint,
            event.blockheight,
            true,
        )
        .await
        .context("registering spend watch for descriptor output")?;
    }
    if updated.status == DescriptorStatus::Syncing && !event.historical_scan {
        updated.status = DescriptorStatus::Active;
        cln::save_record(&plugin.state().rpc_path, &mut updated)
            .await
            .context("activating extended descriptor frontier")?;
    }
    let tip = current_bwatch_height(plugin.state(), event.blockheight).await;
    deliver_and_store(plugin.state(), updated, tip).await?;
    Ok(())
}

async fn handle_spend(
    plugin: Plugin<AppState>,
    event: BwatchMatch,
    name: String,
    outpoint: String,
) -> Result<()> {
    ensure!(
        event.watch_type == "outpoint",
        "unexpected watch type for outpoint owner"
    );
    let raw = event
        .tx
        .as_deref()
        .context("outpoint match omitted transaction")?;
    let input_index = event.index.context("outpoint match omitted input index")?;
    let tx = cln::parse_transaction(raw)?;
    let input = tx
        .input
        .get(usize::try_from(input_index).context("invalid input index")?)
        .context("bwatch input index is outside transaction")?;
    ensure!(
        input.previous_output.to_string() == outpoint,
        "bwatch spend input does not match watched outpoint"
    );
    let spending_txid = tx.compute_txid().to_string();
    let movement_id = spend_movement_id(&outpoint, &spending_txid);
    let _operation = plugin.state().lock_descriptor(&name).await;
    let original = plugin
        .state()
        .record(&name)
        .await
        .with_context(|| format!("unknown descriptor '{name}' in bwatch owner"))?;
    if original.status == DescriptorStatus::Deleting {
        return Ok(());
    }
    ensure!(
        matches!(
            original.status,
            DescriptorStatus::Active | DescriptorStatus::Syncing
        ),
        "descriptor '{name}' cannot accept matches"
    );
    let persisted = original
        .utxos
        .get(&outpoint)
        .with_context(|| format!("unknown tracked outpoint {outpoint}"))?;
    if persisted.spent_by.as_deref() == Some(&spending_txid) {
        if original.pending_movements.contains_key(&movement_id) {
            let tip = current_bwatch_height(plugin.state(), event.blockheight).await;
            deliver_and_store(plugin.state(), original, tip).await?;
        }
        return Ok(());
    }
    ensure!(
        persisted.spent_by.is_none(),
        "tracked outpoint already has a different spend"
    );
    let amount_msat = persisted.amount_msat;
    let timestamp = match event.timestamp {
        Some(timestamp) => timestamp,
        None => cln::block_timestamp(&plugin.state().rpc_path, event.blockheight).await?,
    };
    let mut updated = original;
    let utxo = updated.utxos.get_mut(&outpoint).expect("cloned above");
    utxo.spent_by = Some(spending_txid.clone());
    utxo.spent_height = Some(event.blockheight);
    updated.pending_movements.insert(
        movement_id.clone(),
        PendingMovement {
            id: movement_id.clone(),
            kind: MovementKind::Spend,
            outpoint: outpoint.clone(),
            amount_msat,
            blockheight: event.blockheight,
            timestamp,
            spending_txid: Some(spending_txid),
        },
    );
    cln::save_record(&plugin.state().rpc_path, &mut updated)
        .await
        .context("persisting observed descriptor spend")?;
    plugin.state().put_record(updated.clone()).await;
    let tip = current_bwatch_height(plugin.state(), event.blockheight).await;
    deliver_and_store(plugin.state(), updated, tip).await?;
    Ok(())
}

pub async fn on_bwatch_block_processed(plugin: Plugin<AppState>, value: Value) -> Result<()> {
    let event: BwatchBlock = parse_notification("bwatch_block_processed", value)?;
    let _blockhash = event.blockhash;
    let records = plugin.state().records_snapshot().await;
    let mut failures = Vec::new();
    for (name, record) in records {
        if record.pending_movements.is_empty() || record.status == DescriptorStatus::Deleting {
            continue;
        }
        if let Err(error) = drain_pending_movements(plugin.state(), &name, event.blockheight).await
        {
            report_descriptor_failure(plugin.state(), &name, "bookkeeper_delivery", &error, false)
                .await;
            failures.push(format!("{name}: {error:#}"));
        }
    }
    ensure!(
        failures.is_empty(),
        "failed to process bwatch block {}: {}",
        event.blockheight,
        failures.join("; ")
    );
    Ok(())
}

pub async fn on_bwatch_block_reverted(plugin: Plugin<AppState>, value: Value) -> Result<()> {
    let event: BwatchBlock = parse_notification("bwatch_block_reverted", value)?;
    let _blockhash = event.blockhash;
    let names = plugin.state().record_names().await;
    let mut failures = Vec::new();
    for name in names {
        if let Err(error) = revert_at_height(plugin.clone(), name.clone(), event.blockheight).await
        {
            report_descriptor_failure(plugin.state(), &name, "bwatch_reorg", &error, false).await;
            failures.push(format!("{name}: {error:#}"));
        }
    }
    ensure!(
        failures.is_empty(),
        "failed to revert bwatch block {}: {}",
        event.blockheight,
        failures.join("; ")
    );
    Ok(())
}

async fn revert_at_height(plugin: Plugin<AppState>, name: String, blockheight: u32) -> Result<()> {
    let _operation = plugin.state().lock_descriptor(&name).await;
    let Some(mut updated) = plugin.state().record(&name).await else {
        return Ok(());
    };
    let pending = updated
        .pending_movements
        .values()
        .filter(|movement| movement.blockheight == blockheight)
        .cloned()
        .collect::<Vec<_>>();
    let mut changed = !pending.is_empty();
    let mut removed_deposit = false;
    let mut booked = false;
    for movement in pending {
        updated.pending_movements.remove(&movement.id);
        match movement.kind {
            MovementKind::Deposit => {
                removed_deposit = true;
                updated.utxos.remove(&movement.outpoint);
                let _ = cln::del_outpoint_watch(
                    &plugin.state().rpc_path,
                    &outpoint_owner(&name, &movement.outpoint),
                    &movement.outpoint,
                )
                .await;
            }
            MovementKind::Spend => {
                if let Some(utxo) = updated.utxos.get_mut(&movement.outpoint) {
                    utxo.spent_by = None;
                    utxo.spent_height = None;
                }
            }
        }
    }

    let booked_deposits = updated
        .utxos
        .values()
        .filter(|utxo| utxo.deposit_height == blockheight)
        .map(|utxo| utxo.outpoint.clone())
        .collect::<Vec<_>>();
    for outpoint in booked_deposits {
        changed = true;
        booked = true;
        removed_deposit = true;
        updated.utxos.remove(&outpoint);
        let _ = cln::del_outpoint_watch(
            &plugin.state().rpc_path,
            &outpoint_owner(&name, &outpoint),
            &outpoint,
        )
        .await;
    }
    for utxo in updated.utxos.values_mut() {
        if utxo.spent_height == Some(blockheight) {
            changed = true;
            booked = true;
            utxo.spent_by = None;
            utxo.spent_height = None;
        }
    }
    if !changed {
        return Ok(());
    }
    if removed_deposit {
        updated.last_used_index = updated
            .utxos
            .values()
            .map(|utxo| utxo.derivation_index)
            .max();
    }
    cln::save_record(&plugin.state().rpc_path, &mut updated)
        .await
        .context("persisting descriptor reorg transition")?;
    plugin.state().put_record(updated).await;
    if booked {
        let error = anyhow!(
            "Bookkeeper movements require manual reconciliation after reorg at height {blockheight}"
        );
        drop(_operation);
        report_descriptor_failure(plugin.state(), &name, "bookkeeper_reorg", &error, true).await;
    } else {
        log::info!(
            "descriptor '{name}' discarded unbooked movements after reorg at height {blockheight}"
        );
        record_reorg(plugin.state()).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expanded_frontier_accepts_indexes_beyond_the_gap_size() {
        let lookahead = 20;
        let range_end = desired_range_end(Some(19), lookahead).unwrap();
        assert_eq!(range_end, 40);
        assert!(ensure_derivation_index_is_watched(30, range_end).is_ok());
        assert!(ensure_derivation_index_is_watched(range_end, range_end).is_err());
    }

    #[test]
    fn matches_before_birthheight_are_ignored() {
        assert!(!is_at_or_after_birthheight(999, 1_000));
        assert!(is_at_or_after_birthheight(1_000, 1_000));
    }

    #[test]
    fn movement_ids_are_owner_path_safe() {
        assert_eq!(deposit_movement_id("00aa:1"), "deposit:00aa:1");
        assert_eq!(spend_movement_id("00aa:1", "bbcc"), "spend:00aa:1:bbcc");
    }
}
