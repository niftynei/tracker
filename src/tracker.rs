use crate::cln;
use crate::descriptor::DescriptorSet;
use crate::model::{
    DescriptorConfig, DescriptorRecord, DescriptorStatus, IssuedAddress, ListAddressesRequest,
    MAX_ANNOTATION_BYTES, MAX_LOOKAHEAD, MovementKind, NameRequest, NewAddressRequest,
    OWNER_PREFIX, PendingRescan, ReconcileRequest, RegisterRequest, RescanRequest,
    SyncDescriptionsRequest, TrackerIncident, UpdateRequest, outpoint_owner, script_owner,
    validate_confirmations, validate_lookahead, validate_name,
};
use anyhow::{Context, Error, Result, ensure};
use cln_plugin::{Plugin, RequestContext};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::MissedTickBehavior;

static SCAN_OPERATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<Mutex<TrackerState>>,
    operations: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    active_scans: Arc<StdMutex<BTreeMap<String, String>>>,
    pub rpc_path: PathBuf,
    pub network: String,
}

pub struct ActiveScanGuard {
    active_scans: Arc<StdMutex<BTreeMap<String, String>>>,
    name: String,
    operation_id: String,
}

impl ActiveScanGuard {
    fn operation_id(&self) -> &str {
        &self.operation_id
    }
}

impl Drop for ActiveScanGuard {
    fn drop(&mut self) {
        let Ok(mut active_scans) = self.active_scans.lock() else {
            return;
        };
        if active_scans.get(&self.name) == Some(&self.operation_id) {
            active_scans.remove(&self.name);
        }
    }
}

#[derive(Default)]
pub struct TrackerState {
    pub records: BTreeMap<String, DescriptorRecord>,
    pub issued_addresses: BTreeMap<String, BTreeMap<(u32, u32), IssuedAddress>>,
    pub reconciliation_failures: u64,
    pub bookkeeper_failures: u64,
    pub reorgs: u64,
}

impl AppState {
    pub fn new(rpc_path: PathBuf, network: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TrackerState::default())),
            operations: Arc::new(Mutex::new(BTreeMap::new())),
            active_scans: Arc::new(StdMutex::new(BTreeMap::new())),
            rpc_path,
            network,
        }
    }

    pub async fn lock_descriptor(&self, name: &str) -> OwnedMutexGuard<()> {
        let lock = self
            .operations
            .lock()
            .await
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        lock.lock_owned().await
    }

    pub async fn record(&self, name: &str) -> Option<DescriptorRecord> {
        self.inner.lock().await.records.get(name).cloned()
    }

    pub async fn put_record(&self, record: DescriptorRecord) {
        self.inner
            .lock()
            .await
            .records
            .insert(record.config.name.clone(), record);
    }

    pub async fn remove_record(&self, name: &str) {
        let mut tracker = self.inner.lock().await;
        tracker.records.remove(name);
        tracker.issued_addresses.remove(name);
    }

    pub async fn records_snapshot(&self) -> BTreeMap<String, DescriptorRecord> {
        self.inner.lock().await.records.clone()
    }

    pub async fn record_names(&self) -> Vec<String> {
        self.inner.lock().await.records.keys().cloned().collect()
    }

    pub async fn issued_address(
        &self,
        name: &str,
        branch: u32,
        index: u32,
    ) -> Option<IssuedAddress> {
        self.inner
            .lock()
            .await
            .issued_addresses
            .get(name)
            .and_then(|addresses| addresses.get(&(branch, index)))
            .cloned()
    }

    pub async fn issued_addresses(&self, name: &str) -> Vec<IssuedAddress> {
        self.inner
            .lock()
            .await
            .issued_addresses
            .get(name)
            .map(|addresses| addresses.values().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn put_issued_address(&self, address: IssuedAddress) {
        self.inner
            .lock()
            .await
            .issued_addresses
            .entry(address.name.clone())
            .or_default()
            .insert((address.branch, address.index), address);
    }

    pub fn begin_scan(&self, name: &str) -> Result<ActiveScanGuard> {
        let mut active_scans = self
            .active_scans
            .lock()
            .map_err(|_| anyhow::anyhow!("tracker active scan lock is poisoned"))?;
        ensure!(
            !active_scans.contains_key(name),
            "descriptor '{name}' already has an active scan"
        );
        let operation_id = format!(
            "{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            SCAN_OPERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        active_scans.insert(name.to_owned(), operation_id.clone());
        Ok(ActiveScanGuard {
            active_scans: Arc::clone(&self.active_scans),
            name: name.to_owned(),
            operation_id,
        })
    }

    pub fn ensure_no_active_scan(&self, name: &str) -> Result<()> {
        let active_scans = self
            .active_scans
            .lock()
            .map_err(|_| anyhow::anyhow!("tracker active scan lock is poisoned"))?;
        ensure!(
            !active_scans.contains_key(name),
            "descriptor '{name}' has an active scan"
        );
        Ok(())
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn incident_code(operation: &str, message: &str) -> &'static str {
    if message.contains("Historical rescan failed at block") {
        "historical_block_unavailable"
    } else if operation == "bookkeeper_reorg" {
        "bookkeeper_reorg"
    } else if operation.contains("bookkeeper") || message.contains("Bookkeeper") {
        "bookkeeper_injection_failed"
    } else if message.contains("generation") {
        "datastore_generation_conflict"
    } else if message.contains("lookahead") || message.contains("derivation range") {
        "lookahead_extension_failed"
    } else if message.contains("bwatch") || message.contains("watch") {
        "watch_reconciliation_failed"
    } else {
        "tracker_operation_failed"
    }
}

async fn persist_failure(
    state: &AppState,
    record: &mut DescriptorRecord,
    operation: &str,
    error: &Error,
    operator_action_required: bool,
) {
    let now = unix_time();
    let message = format!("{error:#}");
    let code = incident_code(operation, &message).to_owned();
    if let Some(existing) = &record.incident {
        let preserve_existing = existing.operation != operation
            && (existing.operator_action_required
                || (record.status != DescriptorStatus::Active && operation == "bwatch_event"));
        if preserve_existing {
            log::error!(
                "additional tracker failure descriptor='{}' operation={operation}: {message}",
                record.config.name
            );
            return;
        }
    }
    let (first_seen, retry_count) = record
        .incident
        .as_ref()
        .filter(|incident| incident.code == code && incident.operation == operation)
        .map_or((now, 1), |incident| {
            (incident.first_seen, incident.retry_count.saturating_add(1))
        });
    record.incident = Some(TrackerIncident {
        code: code.clone(),
        operation: operation.to_owned(),
        message: message.clone(),
        first_seen,
        last_seen: now,
        retry_count,
        operator_action_required,
    });
    log::error!(
        "tracker incident code={code} descriptor='{}' operation={operation}: {message}",
        record.config.name
    );
    if let Err(save_error) = cln::save_record(&state.rpc_path, record).await {
        log::error!(
            "could not persist tracker incident for descriptor '{}': {save_error:#}",
            record.config.name
        );
    }
}

fn mark_success(record: &mut DescriptorRecord, operation: &str) {
    let lifecycle_operation = |value: &str| {
        matches!(
            value,
            "register" | "rescan" | "update" | "startup_reconciliation"
        )
    };
    let bookkeeper_operation = |value: &str| value.starts_with("bookkeeper");
    if record.incident.as_ref().is_some_and(|incident| {
        !incident.operator_action_required
            && (incident.operation == operation
                || (lifecycle_operation(&incident.operation) && lifecycle_operation(operation))
                || (bookkeeper_operation(&incident.operation) && bookkeeper_operation(operation)))
    }) {
        log::info!(
            "tracker incident recovered descriptor='{}' operation={operation}",
            record.config.name
        );
        record.incident = None;
    }
    record.last_success_at = Some(unix_time());
}

pub async fn report_descriptor_failure(
    state: &AppState,
    name: &str,
    operation: &str,
    error: &Error,
    operator_action_required: bool,
) {
    let _operation_guard = state.lock_descriptor(name).await;
    let record = {
        let mut tracker = state.inner.lock().await;
        if operation.contains("reorg") {
            tracker.reorgs = tracker.reorgs.saturating_add(1);
        } else if operation.contains("bookkeeper") {
            tracker.bookkeeper_failures = tracker.bookkeeper_failures.saturating_add(1);
        } else {
            tracker.reconciliation_failures = tracker.reconciliation_failures.saturating_add(1);
        }
        tracker.records.get(name).cloned()
    };
    let Some(mut record) = record else {
        log::error!("tracker operation failed for unknown descriptor '{name}': {error:#}");
        return;
    };
    persist_failure(
        state,
        &mut record,
        operation,
        error,
        operator_action_required,
    )
    .await;
    state.put_record(record).await;
}

pub async fn report_descriptor_success(state: &AppState, name: &str, operations: &[&str]) {
    let _operation_guard = state.lock_descriptor(name).await;
    let Some(mut record) = state.record(name).await else {
        return;
    };
    let Some(operation) = record.incident.as_ref().and_then(|incident| {
        operations
            .contains(&incident.operation.as_str())
            .then(|| incident.operation.clone())
    }) else {
        return;
    };
    mark_success(&mut record, &operation);
    if let Err(error) = cln::save_record(&state.rpc_path, &mut record).await {
        log::error!("could not persist tracker recovery for descriptor '{name}': {error:#}");
        return;
    }
    state.put_record(record).await;
}

pub async fn record_reorg(state: &AppState) {
    let mut tracker = state.inner.lock().await;
    tracker.reorgs = tracker.reorgs.saturating_add(1);
}

pub fn parse_request<T: DeserializeOwned>(args: Value, names: &[&str]) -> Result<T> {
    let args = match args {
        Value::Array(values) => {
            ensure!(
                values.len() <= names.len(),
                "too many positional RPC parameters"
            );
            let mut object = Map::new();
            for (name, value) in names.iter().zip(values) {
                object.insert((*name).to_owned(), value);
            }
            Value::Object(object)
        }
        other => other,
    };
    serde_json::from_value(args).context("invalid RPC parameters")
}

pub fn validate_config(config: &DescriptorConfig, network: &str) -> Result<DescriptorSet> {
    validate_name(&config.name)?;
    validate_lookahead(config.lookahead)?;
    validate_confirmations(config.confirmations)?;
    let descriptor = DescriptorSet::parse_checked(&config.descriptor, network)?;
    if !descriptor.is_ranged() {
        ensure!(
            config.lookahead == 1,
            "non-ranged descriptors require lookahead=1"
        );
    }
    Ok(descriptor)
}

pub fn desired_range_end(last_used: Option<u32>, lookahead: u32) -> Result<u32> {
    let end = match last_used {
        Some(index) => index
            .checked_add(1)
            .and_then(|value| value.checked_add(lookahead))
            .context("descriptor derivation range overflow")?,
        None => lookahead,
    };
    ensure!(
        end < (1 << 31),
        "descriptor derivation range exceeds unhardened indexes"
    );
    Ok(end)
}

pub fn branch_frontier(record: &DescriptorRecord, branch: u32) -> Option<u32> {
    let last_used = record.last_used_indexes.get(&branch).copied();
    let last_issued = record
        .next_indexes
        .get(&branch)
        .copied()
        .and_then(|next| next.checked_sub(1));
    last_used.max(last_issued)
}

pub fn desired_branch_range_end(
    record: &DescriptorRecord,
    branch: u32,
    lookahead: u32,
) -> Result<u32> {
    desired_range_end(branch_frontier(record, branch), lookahead)
}

fn refresh_aggregate_indexes(record: &mut DescriptorRecord) {
    record.range_end = record.range_ends.values().copied().max().unwrap_or(0);
    record.last_used_index = record.last_used_indexes.values().copied().max();
}

pub fn movement_is_mature(blockheight: u32, confirmations: u32, tip_height: u32) -> bool {
    blockheight
        .checked_add(confirmations.saturating_sub(1))
        .is_some_and(|maturity_height| tip_height >= maturity_height)
}

pub async fn deliver_mature_movements(
    state: &AppState,
    record: &mut DescriptorRecord,
    tip_height: u32,
) -> Result<u64> {
    let mature = record
        .pending_movements
        .values()
        .filter(|movement| {
            movement_is_mature(
                movement.blockheight,
                record.config.confirmations,
                tip_height,
            )
        })
        .map(|movement| movement.id.clone())
        .collect::<Vec<_>>();
    let mut delivered = 0_u64;
    for movement_id in mature {
        let movement = record
            .pending_movements
            .get(&movement_id)
            .cloned()
            .with_context(|| format!("pending movement '{movement_id}' disappeared"))?;
        match movement.kind {
            MovementKind::Deposit => {
                cln::inject_utxo_deposit(
                    &state.rpc_path,
                    &record.config.name,
                    &movement.outpoint,
                    movement.amount_msat,
                    movement.timestamp,
                    movement.blockheight,
                )
                .await
                .context("injecting durable Bookkeeper deposit")?;
                if let Some(description) = movement.description.as_deref() {
                    cln::describe_utxo(&state.rpc_path, &movement.outpoint, description)
                        .await
                        .context("describing durable Bookkeeper deposit")?;
                }
            }
            MovementKind::Spend => {
                let spending_txid = movement
                    .spending_txid
                    .as_deref()
                    .context("pending spend omitted spending transaction id")?;
                cln::inject_utxo_spend(
                    &state.rpc_path,
                    &record.config.name,
                    &movement.outpoint,
                    spending_txid,
                    movement.amount_msat,
                    movement.timestamp,
                    movement.blockheight,
                )
                .await
                .context("injecting durable Bookkeeper spend")?;
            }
            MovementKind::ExternalDeposit => {
                cln::inject_external_deposit(
                    &state.rpc_path,
                    &record.config.name,
                    &movement.outpoint,
                    movement.amount_msat,
                    movement.timestamp,
                    movement.blockheight,
                )
                .await
                .context("injecting durable external Bookkeeper deposit")?;
                if let Some(description) = movement.description.as_deref() {
                    cln::describe_utxo(&state.rpc_path, &movement.outpoint, description)
                        .await
                        .context("describing durable external Bookkeeper deposit")?;
                }
            }
            MovementKind::ExternalDescription => {
                let description = movement
                    .description
                    .as_deref()
                    .context("external description repair omitted its description")?;
                cln::describe_utxo(&state.rpc_path, &movement.outpoint, description)
                    .await
                    .context("repairing external Bookkeeper description")?;
            }
        }
        record.pending_movements.remove(&movement_id);
        mark_success(record, "bookkeeper_delivery");
        cln::save_record(&state.rpc_path, record)
            .await
            .context("persisting delivered Bookkeeper movement")?;
        delivered = delivered.saturating_add(1);
    }
    Ok(delivered)
}

pub async fn current_bwatch_height(state: &AppState, fallback: u32) -> u32 {
    cln::bwatch_status(&state.rpc_path)
        .await
        .map_or(fallback, |status| status.current_height.max(fallback))
}

pub async fn deliver_and_store(
    state: &AppState,
    mut record: DescriptorRecord,
    tip_height: u32,
) -> Result<u64> {
    let delivered = deliver_mature_movements(state, &mut record, tip_height).await?;
    state.put_record(record).await;
    Ok(delivered)
}

pub async fn drain_pending_movements(state: &AppState, name: &str, tip_height: u32) -> Result<u64> {
    let _operation = state.lock_descriptor(name).await;
    let record = state
        .record(name)
        .await
        .with_context(|| format!("descriptor '{name}' is not registered"))?;
    deliver_and_store(state, record, tip_height).await
}

pub async fn load(state: &AppState) -> Result<()> {
    let records = cln::load_records(&state.rpc_path).await?;
    let addresses = cln::load_issued_addresses(&state.rpc_path).await?;
    let mut issued_by_name = BTreeMap::<String, BTreeMap<(u32, u32), IssuedAddress>>::new();
    for address in addresses {
        issued_by_name
            .entry(address.name.clone())
            .or_default()
            .insert((address.branch, address.index), address);
    }
    let mut restored = BTreeMap::new();
    for mut record in records {
        let original = record.clone();
        let descriptor = validate_config(&record.config, &state.network)
            .with_context(|| format!("validating stored descriptor '{}'", record.config.name))?;
        let branch_count = u32::try_from(descriptor.branch_count()).context("too many branches")?;
        if record.range_ends.is_empty() {
            let migrated_end = if record.range_end == 0 {
                record.config.lookahead
            } else {
                record.range_end
            };
            for branch in 0..branch_count {
                record.range_ends.insert(branch, migrated_end);
            }
        }
        if record.last_used_indexes.is_empty() {
            for utxo in record.utxos.values() {
                record
                    .last_used_indexes
                    .entry(utxo.branch)
                    .and_modify(|index| *index = (*index).max(utxo.derivation_index))
                    .or_insert(utxo.derivation_index);
            }
            if record.last_used_indexes.is_empty()
                && let Some(legacy) = record.last_used_index
            {
                record.last_used_indexes.entry(0).or_insert(legacy);
            }
        }
        let descriptor_addresses = issued_by_name
            .get(&record.config.name)
            .cloned()
            .unwrap_or_default();
        for address in descriptor_addresses.values() {
            ensure!(
                address.branch < branch_count,
                "issued address for '{}' has invalid branch {}",
                record.config.name,
                address.branch
            );
            let derived = descriptor.derive_one(address.branch, address.index)?;
            ensure!(
                derived.scriptpubkey == address.scriptpubkey
                    && descriptor.derive_address(address.branch, address.index, &state.network)?
                        == address.address,
                "issued address metadata does not match descriptor '{}'",
                record.config.name
            );
            let next = address
                .index
                .checked_add(1)
                .context("issued address cursor overflow")?;
            record
                .next_indexes
                .entry(address.branch)
                .and_modify(|current| *current = (*current).max(next))
                .or_insert(next);
        }
        for branch in 0..branch_count {
            let desired = desired_branch_range_end(&record, branch, record.config.lookahead)?;
            record
                .range_ends
                .entry(branch)
                .and_modify(|end| *end = (*end).max(desired))
                .or_insert(desired);
        }
        refresh_aggregate_indexes(&mut record);
        let scripts = descriptor_scripts(&record, &descriptor)?;
        ensure_no_script_overlap(&record.config.name, &scripts, &restored, &state.network)?;
        ensure!(
            !restored.contains_key(&record.config.name),
            "duplicate stored descriptor '{}'",
            record.config.name
        );
        if record != original {
            cln::save_record(&state.rpc_path, &mut record)
                .await
                .with_context(|| format!("migrating descriptor '{}'", record.config.name))?;
        }
        restored.insert(record.config.name.clone(), record);
    }
    for name in issued_by_name.keys() {
        if !restored.contains_key(name) {
            cln::delete_issued_addresses(&state.rpc_path, name)
                .await
                .with_context(|| format!("removing orphaned issued addresses for '{name}'"))?;
        }
    }
    let mut tracker = state.inner.lock().await;
    tracker.records = restored;
    tracker.issued_addresses = issued_by_name
        .into_iter()
        .filter(|(name, _)| tracker.records.contains_key(name))
        .collect();
    Ok(())
}

fn owner_prefix(name: &str) -> String {
    format!("{OWNER_PREFIX}/{name}/")
}

fn owned_watch_owner(watch: &cln::OwnedWatch) -> &str {
    match watch {
        cln::OwnedWatch::Script { owner, .. }
        | cln::OwnedWatch::Outpoint { owner, .. }
        | cln::OwnedWatch::Blockdepth { owner, .. } => owner,
    }
}

fn expected_watches(
    record: &DescriptorRecord,
    descriptor: &DescriptorSet,
) -> Result<BTreeSet<cln::OwnedWatch>> {
    let mut expected = BTreeSet::new();
    for script in descriptor_scripts(record, descriptor)? {
        expected.insert(cln::OwnedWatch::Script {
            owner: script_owner(&record.config.name, script.branch, script.index),
            scriptpubkey: script.scriptpubkey,
        });
    }
    for utxo in record.utxos.values() {
        expected.insert(cln::OwnedWatch::Outpoint {
            owner: outpoint_owner(&record.config.name, &utxo.outpoint),
            outpoint: utxo.outpoint.clone(),
        });
    }
    Ok(expected)
}

fn descriptor_scripts(
    record: &DescriptorRecord,
    descriptor: &DescriptorSet,
) -> Result<Vec<crate::descriptor::DerivedScript>> {
    let mut scripts = Vec::new();
    for branch in 0..u32::try_from(descriptor.branch_count()).context("too many branches")? {
        let range_end = record
            .range_ends
            .get(&branch)
            .copied()
            .unwrap_or(record.range_end);
        scripts.extend(descriptor.derive_branch_range(branch, 0, range_end)?);
    }
    Ok(scripts)
}

async fn reconcile_watches(
    state: &AppState,
    record: &DescriptorRecord,
    rescan_missing: bool,
) -> Result<()> {
    let descriptor = validate_config(&record.config, &state.network)?;
    let expected = expected_watches(record, &descriptor)?;
    let current = cln::list_owned_watches(&state.rpc_path, &owner_prefix(&record.config.name))
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let missing = expected
        .difference(&current)
        .cloned()
        .collect::<BTreeSet<_>>();

    for watch in current.difference(&expected) {
        cln::del_owned_watch(&state.rpc_path, watch)
            .await
            .with_context(|| format!("removing unexpected watch for '{}'", record.config.name))?;
    }

    let script_watches = missing
        .iter()
        .filter_map(|watch| match watch {
            cln::OwnedWatch::Script {
                owner,
                scriptpubkey,
            } => Some((owner.clone(), scriptpubkey.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    cln::add_script_watches(&state.rpc_path, &script_watches, record.config.birthheight)
        .await
        .with_context(|| format!("restoring script watches for '{}'", record.config.name))?;

    if !rescan_missing {
        // A synchronous scanwatchset call already examined the complete
        // historical range.  Install the durable frontier without fetching
        // any of those blocks again.
    } else if record.status == DescriptorStatus::Syncing {
        // A crash may have happened after only part of a registration batch
        // was persisted, or after its scan completed but before activation.
        // Replay the complete descriptor namespace to guarantee coverage; the
        // persisted movement IDs make this recovery path idempotent.
        let owner_prefix = format!("plugin/tracker/{}/", record.config.name);
        cln::rescan_watch_prefix(&state.rpc_path, &owner_prefix, record.config.birthheight)
            .await
            .with_context(|| format!("rescanning wallet watch set for '{}'", record.config.name))?;
    } else {
        // Normal startup/manual reconciliation must not replay owners whose
        // watches were already intact.  Only newly restored scripts need a
        // historical scan.
        let missing_script_owners = script_watches
            .iter()
            .map(|(owner, _)| owner.clone())
            .collect::<Vec<_>>();
        cln::rescan_watch_owners(
            &state.rpc_path,
            &missing_script_owners,
            record.config.birthheight,
        )
        .await
        .with_context(|| {
            format!(
                "rescanning restored script watches for '{}'",
                record.config.name
            )
        })?;
    }

    for watch in &missing {
        match watch {
            cln::OwnedWatch::Script { .. } => {}
            cln::OwnedWatch::Outpoint { owner, outpoint } => {
                let start_block = record
                    .utxos
                    .get(outpoint)
                    .with_context(|| format!("missing tracked outpoint {outpoint}"))?
                    .deposit_height;
                cln::add_outpoint_watch(
                    &state.rpc_path,
                    owner,
                    outpoint,
                    start_block,
                    rescan_missing,
                )
                .await
                .with_context(|| format!("restoring outpoint watch {outpoint}"))?;
            }
            // Blockdepth watches from older Tracker versions are omitted from
            // `expected` and removed by the difference pass above.
            cln::OwnedWatch::Blockdepth { .. } => unreachable!("not an expected watch"),
        }
    }
    Ok(())
}

async fn delete_owned_watches(state: &AppState, name: &str) -> Result<()> {
    for watch in cln::list_owned_watches(&state.rpc_path, &owner_prefix(name)).await? {
        cln::del_owned_watch(&state.rpc_path, &watch)
            .await
            .with_context(|| format!("removing watch for '{name}'"))?;
    }
    Ok(())
}

pub(crate) fn ensure_no_script_overlap(
    candidate_name: &str,
    scripts: &[crate::descriptor::DerivedScript],
    records: &BTreeMap<String, DescriptorRecord>,
    network: &str,
) -> Result<()> {
    let candidate = scripts
        .iter()
        .map(|script| script.scriptpubkey.as_str())
        .collect::<BTreeSet<_>>();
    for record in records.values() {
        if record.config.name == candidate_name {
            continue;
        }
        let descriptor = validate_config(&record.config, network)?;
        for script in descriptor_scripts(record, &descriptor)? {
            ensure!(
                !candidate.contains(script.scriptpubkey.as_str()),
                "descriptor script overlaps registered descriptor '{}'",
                record.config.name
            );
        }
    }
    Ok(())
}

async fn reconcile_descriptor(state: &AppState, name: &str, tip_height: u32) -> Result<Value> {
    let _operation = state.lock_descriptor(name).await;
    state.ensure_no_active_scan(name)?;
    let mut record = state
        .record(name)
        .await
        .with_context(|| format!("descriptor '{name}' is not registered"))?;
    if record.status == DescriptorStatus::Deleting {
        delete_owned_watches(state, name).await?;
        cln::delete_issued_addresses(&state.rpc_path, name).await?;
        cln::delete_record(&state.rpc_path, &record).await?;
        state.remove_record(name).await;
        return Ok(json!({ "name": name, "removed": true }));
    }
    ensure!(
        record.initial_scan_complete,
        "descriptor '{name}' has an interrupted initial scan; retry tracker-register with the same configuration"
    );
    ensure!(
        record.pending_rescan.is_none(),
        "descriptor '{name}' has an interrupted rescan; retry tracker-rescan to resume it"
    );
    reconcile_watches(state, &record, true).await?;
    record.status = DescriptorStatus::Active;
    record.scan_operation_id = None;
    mark_success(&mut record, "startup_reconciliation");
    cln::save_record(&state.rpc_path, &mut record)
        .await
        .context("persisting reconciled descriptor")?;
    let delivered = deliver_mature_movements(state, &mut record, tip_height).await?;
    let descriptor = validate_config(&record.config, &state.network)?;
    let mut view = record_view(&record, &descriptor);
    view["delivered_movements"] = json!(delivered);
    state.put_record(record).await;
    Ok(view)
}

pub async fn restore_watches(state: &AppState) -> Result<()> {
    let known_prefixes = state
        .record_names()
        .await
        .iter()
        .map(|name| owner_prefix(name))
        .collect::<Vec<_>>();
    let all_watches = match cln::list_owned_watches(&state.rpc_path, &format!("{OWNER_PREFIX}/"))
        .await
    {
        Ok(watches) => watches,
        Err(error) => {
            let names = state.record_names().await;
            for name in names {
                report_descriptor_failure(state, &name, "startup_reconciliation", &error, false)
                    .await;
            }
            return Ok(());
        }
    };
    for watch in all_watches {
        if known_prefixes
            .iter()
            .any(|prefix| owned_watch_owner(&watch).starts_with(prefix))
        {
            continue;
        }
        if let Err(error) = cln::del_owned_watch(&state.rpc_path, &watch)
            .await
            .context("removing orphaned tracker watch")
        {
            log::error!("tracker orphan watch reconciliation failed: {error:#}");
        }
    }
    let names = state.record_names().await;
    let tip_height = cln::bwatch_status(&state.rpc_path)
        .await
        .map_or(0, |status| status.current_height);
    for name in names {
        if let Err(error) = reconcile_descriptor(state, &name, tip_height).await {
            let operation = if error.to_string().contains("Bookkeeper") {
                "bookkeeper_delivery"
            } else {
                "startup_reconciliation"
            };
            report_descriptor_failure(state, &name, operation, &error, false).await;
        }
    }
    Ok(())
}

pub async fn reconcile(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: ReconcileRequest = parse_request(args, &["name"])?;
    let names = if let Some(name) = request.name {
        vec![name]
    } else {
        plugin.state().record_names().await
    };
    let tip_height = cln::bwatch_status(&plugin.state().rpc_path)
        .await
        .context("reading bwatch height before reconciliation")?
        .current_height;
    let mut reconciled = Vec::new();
    for name in names {
        match reconcile_descriptor(plugin.state(), &name, tip_height).await {
            Ok(value) => reconciled.push(json!({ "name": name, "ok": true, "result": value })),
            Err(error) => {
                let operation = if error.to_string().contains("Bookkeeper") {
                    "bookkeeper_delivery"
                } else {
                    "manual_reconciliation"
                };
                report_descriptor_failure(plugin.state(), &name, operation, &error, false).await;
                reconciled.push(json!({
                    "name": name,
                    "ok": false,
                    "error": format!("{error:#}"),
                }));
            }
        }
    }
    Ok(json!({ "reconciled": reconciled }))
}

fn rpc_progress(blocks_processed: u64, blocks_total: u64) -> Option<(u32, u32)> {
    // lightning-cli's progress renderer expects a zero-indexed numerator and
    // currently cannot render a one-item progress bar.
    if blocks_processed == 0 || blocks_total < 2 {
        return None;
    }
    let total = u32::try_from(blocks_total).ok()?;
    let completed = u32::try_from(blocks_processed.min(blocks_total)).ok()?;
    Some((completed - 1, total))
}

async fn report_scan_progress(
    request_context: &RequestContext,
    last_progress: &mut Option<(u32, u32)>,
    blocks_processed: u64,
    blocks_total: u64,
) {
    let Some(progress) = rpc_progress(blocks_processed, blocks_total) else {
        return;
    };
    if Some(progress) == *last_progress {
        return;
    }
    if let Err(error) = request_context.progress(progress.0, progress.1, None).await {
        log::warn!("could not report descriptor scan progress: {error:#}");
        return;
    }
    *last_progress = Some(progress);
}

async fn scan_watch_set_with_progress(
    plugin: &Plugin<AppState>,
    request_context: &RequestContext,
    name: &str,
    watches: &[(String, String)],
    start_block: u32,
) -> Result<crate::model::BwatchScanResult> {
    let scan = cln::scan_watch_set(&plugin.state().rpc_path, watches, start_block);
    tokio::pin!(scan);
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let expected_owner_prefix = owner_prefix(name);
    let mut last_progress = None;

    loop {
        tokio::select! {
            result = &mut scan => {
                let result = result?;
                let total = if result.start_block > result.target_block {
                    0
                } else {
                    u64::from(result.target_block - result.start_block) + 1
                };
                report_scan_progress(
                    request_context,
                    &mut last_progress,
                    result.blocks_processed,
                    total,
                ).await;
                return Ok(result);
            }
            _ = poll.tick() => {
                match cln::bwatch_status(&plugin.state().rpc_path).await {
                    Ok(status) => {
                        if let Some(rescan) = status.active_rescans.iter().find(|rescan| {
                            rescan.watch_type == "set"
                                && rescan.owners.iter().any(|owner| {
                                    owner.starts_with(&expected_owner_prefix)
                                })
                        }) {
                            report_scan_progress(
                                request_context,
                                &mut last_progress,
                                rescan.blocks_processed,
                                rescan.blocks_total,
                            ).await;
                        }
                    }
                    Err(error) => {
                        log::debug!("could not poll bwatch scan progress: {error:#}");
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HistoricalScanSummary {
    start_block: u32,
    target_block: u32,
    blocks_processed: u64,
    matches_found: u64,
    deposits_matched: u64,
    spends_matched: u64,
}

async fn apply_historical_scan(
    plugin: &Plugin<AppState>,
    expected_start_block: u32,
    mut scan: crate::model::BwatchScanResult,
) -> Result<HistoricalScanSummary> {
    let expected_blocks = if scan.start_block > scan.target_block {
        0
    } else {
        u64::from(scan.target_block - scan.start_block) + 1
    };
    ensure!(
        scan.start_block == expected_start_block && scan.blocks_processed == expected_blocks,
        "bwatch returned an incomplete wallet scan"
    );
    let summary = HistoricalScanSummary {
        start_block: scan.start_block,
        target_block: scan.target_block,
        blocks_processed: scan.blocks_processed,
        matches_found: u64::try_from(scan.matches.len()).unwrap_or(u64::MAX),
        deposits_matched: u64::try_from(
            scan.matches
                .iter()
                .filter(|event| event.watch_type == "scriptpubkey")
                .count(),
        )
        .unwrap_or(u64::MAX),
        spends_matched: u64::try_from(
            scan.matches
                .iter()
                .filter(|event| event.watch_type == "outpoint")
                .count(),
        )
        .unwrap_or(u64::MAX),
    };
    // Within a block, record descriptor-owned outputs before processing
    // spends. That ensures same-transaction change is never mistaken for an
    // external recipient during historical repair.
    scan.matches.sort_by_key(|event| {
        (
            event.blockheight,
            if event.watch_type == "scriptpubkey" {
                0
            } else {
                1
            },
        )
    });
    for mut event in scan.matches {
        event.historical_scan = true;
        crate::events::on_bwatch_match(
            plugin.clone(),
            serde_json::to_value(event).context("serializing historical match")?,
        )
        .await
        .context("applying historical descriptor match")?;
    }
    Ok(summary)
}

pub async fn register(
    plugin: Plugin<AppState>,
    request_context: RequestContext,
    args: Value,
) -> Result<Value> {
    let request: RegisterRequest = parse_request(
        args,
        &[
            "name",
            "descriptor",
            "birthheight",
            "lookahead",
            "confirmations",
        ],
    )?;
    let config = DescriptorConfig {
        name: request.name,
        descriptor: request.descriptor,
        birthheight: request.birthheight,
        lookahead: request.lookahead,
        confirmations: request.confirmations,
    };
    let descriptor = validate_config(&config, &plugin.state().network)?;
    let scripts = descriptor.derive_range(0, config.lookahead)?;
    let initial_range_ends = (0..u32::try_from(descriptor.branch_count())
        .context("too many descriptor branches")?)
        .map(|branch| (branch, config.lookahead))
        .collect::<BTreeMap<_, _>>();
    let scan_end = if descriptor.is_ranged() {
        MAX_LOOKAHEAD
    } else {
        1
    };
    let scan_scripts = descriptor
        .derive_range(0, scan_end)
        .context("deriving transient historical scan range")?;
    let operation = plugin.state().lock_descriptor(&config.name).await;
    let records = plugin.state().records_snapshot().await;
    ensure_no_script_overlap(
        &config.name,
        &scan_scripts,
        &records,
        &plugin.state().network,
    )?;

    let mut record = if let Some(existing) = records.get(&config.name) {
        ensure!(
            existing.status == DescriptorStatus::Syncing
                && !existing.initial_scan_complete
                && existing.pending_rescan.is_none()
                && existing.config == config,
            "descriptor '{}' is already registered",
            config.name
        );
        existing.clone()
    } else {
        DescriptorRecord {
            config,
            utxos: BTreeMap::new(),
            pending_movements: BTreeMap::new(),
            external_outpoints: BTreeSet::new(),
            next_indexes: BTreeMap::new(),
            range_ends: initial_range_ends,
            last_used_indexes: BTreeMap::new(),
            range_end: scripts
                .iter()
                .map(|script| script.index + 1)
                .max()
                .unwrap_or(0),
            last_used_index: None,
            status: DescriptorStatus::Syncing,
            initial_scan_complete: false,
            pending_rescan: None,
            scan_operation_id: None,
            incident: None,
            last_success_at: None,
            generation: None,
        }
    };
    let scan_operation = plugin.state().begin_scan(&record.config.name)?;
    record.scan_operation_id = Some(scan_operation.operation_id().to_owned());
    cln::save_record(&plugin.state().rpc_path, &mut record)
        .await
        .context("persisting descriptor registration intent")?;
    plugin.state().put_record(record.clone()).await;

    drop(operation);

    if !record.initial_scan_complete {
        let transient_watches = scan_scripts
            .into_iter()
            .map(|script| {
                (
                    script_owner(&record.config.name, script.branch, script.index),
                    script.scriptpubkey,
                )
            })
            .collect::<Vec<_>>();
        let scan = match scan_watch_set_with_progress(
            &plugin,
            &request_context,
            &record.config.name,
            &transient_watches,
            record.config.birthheight,
        )
        .await
        .context("scanning descriptor history in one block pass")
        {
            Ok(scan) => scan,
            Err(error) => {
                report_descriptor_failure(
                    plugin.state(),
                    &record.config.name,
                    "register",
                    &error,
                    false,
                )
                .await;
                return Err(error);
            }
        };
        if let Err(error) = apply_historical_scan(&plugin, record.config.birthheight, scan).await {
            report_descriptor_failure(
                plugin.state(),
                &record.config.name,
                "register",
                &error,
                false,
            )
            .await;
            return Err(error);
        }
    }

    let _operation = plugin.state().lock_descriptor(&record.config.name).await;
    record = plugin
        .state()
        .record(&record.config.name)
        .await
        .context("descriptor disappeared while applying historical scan")?;
    ensure!(
        record.scan_operation_id.as_deref() == Some(scan_operation.operation_id()),
        "descriptor registration scan operation changed while its scan was running"
    );
    if !record.initial_scan_complete {
        record.initial_scan_complete = true;
        cln::save_record(&plugin.state().rpc_path, &mut record)
            .await
            .context("persisting completed initial scan")?;
        plugin.state().put_record(record.clone()).await;
    }
    if let Err(error) = reconcile_watches(plugin.state(), &record, false)
        .await
        .context("installing descriptor watches after historical scan")
    {
        plugin.state().put_record(record.clone()).await;
        drop(_operation);
        report_descriptor_failure(
            plugin.state(),
            &record.config.name,
            "register",
            &error,
            false,
        )
        .await;
        return Err(error);
    }
    record.status = DescriptorStatus::Active;
    record.scan_operation_id = None;
    mark_success(&mut record, "register");
    cln::save_record(&plugin.state().rpc_path, &mut record)
        .await
        .context("activating descriptor")?;
    let response = record_view(&record, &descriptor);
    plugin.state().put_record(record).await;
    Ok(response)
}

fn rescan_resume_parameters_match(
    pending: &PendingRescan,
    persisted_lookahead: u32,
    requested_start: u32,
    requested_lookahead: u32,
) -> bool {
    pending.start_block == requested_start && persisted_lookahead == requested_lookahead
}

pub async fn rescan(
    plugin: Plugin<AppState>,
    request_context: RequestContext,
    args: Value,
) -> Result<Value> {
    let request: RescanRequest = parse_request(args, &["name", "start_block", "lookahead"])?;
    if let Some(lookahead) = request.lookahead {
        validate_lookahead(lookahead)?;
    }

    let operation = plugin.state().lock_descriptor(&request.name).await;
    let records = plugin.state().records_snapshot().await;
    let old = records
        .get(&request.name)
        .cloned()
        .with_context(|| format!("descriptor '{}' is not registered", request.name))?;
    ensure!(
        old.status != DescriptorStatus::Deleting,
        "descriptor '{}' is being deleted",
        request.name
    );
    ensure!(
        old.initial_scan_complete,
        "descriptor '{}' has an interrupted initial scan; retry tracker-register with the same configuration",
        request.name
    );
    let descriptor = validate_config(&old.config, &plugin.state().network)?;
    let requested_lookahead = request.lookahead.unwrap_or(old.config.lookahead);
    if !descriptor.is_ranged() {
        ensure!(
            requested_lookahead == 1,
            "non-ranged descriptors require lookahead=1"
        );
    }
    let requested_start = request
        .start_block
        .or_else(|| {
            old.pending_rescan
                .as_ref()
                .map(|pending| pending.start_block)
        })
        .unwrap_or(old.config.birthheight);
    ensure!(
        requested_start >= old.config.birthheight,
        "start_block cannot be earlier than descriptor birthheight {}",
        old.config.birthheight
    );

    let mut record = if let Some(pending) = &old.pending_rescan {
        ensure!(
            rescan_resume_parameters_match(
                pending,
                old.config.lookahead,
                requested_start,
                requested_lookahead,
            ),
            "descriptor '{}' has an interrupted rescan; retry start_block={} lookahead={} first",
            request.name,
            pending.start_block,
            old.config.lookahead
        );
        // pending_rescan is the durable operation marker. Older recovery paths
        // could leave status=active alongside that marker; accepting the exact
        // persisted parameters and restoring syncing heals that state without
        // permitting a different scan to replace it.
        let mut resumed = old;
        resumed.status = DescriptorStatus::Syncing;
        resumed
    } else {
        ensure!(
            old.status == DescriptorStatus::Active,
            "descriptor '{}' has another interrupted operation; reconcile it before rescanning",
            request.name
        );
        let mut updated = old;
        updated.config.lookahead = requested_lookahead;
        for branch in
            0..u32::try_from(descriptor.branch_count()).context("too many descriptor branches")?
        {
            let new_end = desired_branch_range_end(&updated, branch, requested_lookahead)?;
            updated.range_ends.insert(branch, new_end);
        }
        refresh_aggregate_indexes(&mut updated);
        let desired_scripts = descriptor_scripts(&updated, &descriptor)?;
        ensure_no_script_overlap(
            &updated.config.name,
            &desired_scripts,
            &records,
            &plugin.state().network,
        )?;
        updated.status = DescriptorStatus::Syncing;
        updated.pending_rescan = Some(PendingRescan {
            start_block: requested_start,
        });
        updated
    };
    let scan_operation = plugin.state().begin_scan(&record.config.name)?;
    record.scan_operation_id = Some(scan_operation.operation_id().to_owned());
    cln::save_record(&plugin.state().rpc_path, &mut record)
        .await
        .context("persisting descriptor rescan intent")?;
    plugin.state().put_record(record.clone()).await;

    let scan_scripts =
        descriptor_scripts(&record, &descriptor).context("deriving descriptor rescan range")?;
    let transient_watches = scan_scripts
        .into_iter()
        .map(|script| {
            (
                script_owner(&record.config.name, script.branch, script.index),
                script.scriptpubkey,
            )
        })
        .collect::<Vec<_>>();
    drop(operation);

    let scan = match scan_watch_set_with_progress(
        &plugin,
        &request_context,
        &record.config.name,
        &transient_watches,
        requested_start,
    )
    .await
    .context("rescanning descriptor history in one block pass")
    {
        Ok(scan) => scan,
        Err(error) => {
            report_descriptor_failure(plugin.state(), &record.config.name, "rescan", &error, false)
                .await;
            return Err(error);
        }
    };
    let summary = match apply_historical_scan(&plugin, requested_start, scan).await {
        Ok(summary) => summary,
        Err(error) => {
            report_descriptor_failure(plugin.state(), &record.config.name, "rescan", &error, false)
                .await;
            return Err(error);
        }
    };

    let _operation = plugin.state().lock_descriptor(&record.config.name).await;
    record = plugin
        .state()
        .record(&record.config.name)
        .await
        .context("descriptor disappeared while applying historical rescan")?;
    ensure!(
        record
            .pending_rescan
            .as_ref()
            .is_some_and(|pending| { pending.start_block == requested_start }),
        "descriptor rescan operation changed while its scan was running"
    );
    ensure!(
        record.scan_operation_id.as_deref() == Some(scan_operation.operation_id()),
        "descriptor rescan identity changed while its scan was running"
    );
    if let Err(error) = reconcile_watches(plugin.state(), &record, false)
        .await
        .context("installing descriptor watches after historical rescan")
    {
        plugin.state().put_record(record.clone()).await;
        drop(_operation);
        report_descriptor_failure(plugin.state(), &record.config.name, "rescan", &error, false)
            .await;
        return Err(error);
    }
    record.pending_rescan = None;
    record.scan_operation_id = None;
    record.status = DescriptorStatus::Active;
    mark_success(&mut record, "rescan");
    cln::save_record(&plugin.state().rpc_path, &mut record)
        .await
        .context("activating rescanned descriptor")?;
    let descriptor = validate_config(&record.config, &plugin.state().network)?;
    let mut response = record_view(&record, &descriptor);
    response["rescan"] = json!({
        "start_block": summary.start_block,
        "target_block": summary.target_block,
        "blocks_processed": summary.blocks_processed,
        "matches_found": summary.matches_found,
        "deposits_matched": summary.deposits_matched,
        "spends_matched": summary.spends_matched,
    });
    plugin.state().put_record(record).await;
    Ok(response)
}

pub async fn unregister(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: NameRequest = parse_request(args, &["name"])?;
    let _operation = plugin.state().lock_descriptor(&request.name).await;
    plugin.state().ensure_no_active_scan(&request.name)?;
    let mut record = plugin
        .state()
        .record(&request.name)
        .await
        .with_context(|| format!("descriptor '{}' is not registered", request.name))?;
    if record.status != DescriptorStatus::Deleting {
        record.status = DescriptorStatus::Deleting;
        cln::save_record(&plugin.state().rpc_path, &mut record)
            .await
            .context("persisting descriptor deletion intent")?;
        plugin.state().put_record(record.clone()).await;
    }
    if let Err(error) = delete_owned_watches(plugin.state(), &request.name)
        .await
        .context("deleting descriptor watches")
    {
        drop(_operation);
        report_descriptor_failure(plugin.state(), &request.name, "unregister", &error, false).await;
        return Err(error);
    }
    if let Err(error) = cln::delete_issued_addresses(&plugin.state().rpc_path, &request.name)
        .await
        .context("deleting issued address metadata")
    {
        drop(_operation);
        report_descriptor_failure(plugin.state(), &request.name, "unregister", &error, false).await;
        return Err(error);
    }
    if let Err(error) = cln::delete_record(&plugin.state().rpc_path, &record)
        .await
        .context("deleting descriptor from datastore")
    {
        drop(_operation);
        report_descriptor_failure(plugin.state(), &request.name, "unregister", &error, false).await;
        return Err(error);
    }
    plugin.state().remove_record(&request.name).await;
    Ok(json!({ "name": request.name, "removed": true }))
}

pub async fn inspect(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: NameRequest = parse_request(args, &["name"])?;
    let record = plugin
        .state()
        .record(&request.name)
        .await
        .with_context(|| format!("descriptor '{}' is not registered", request.name))?;
    let descriptor = validate_config(&record.config, &plugin.state().network)?;
    Ok(record_view(&record, &descriptor))
}

pub async fn new_address(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: NewAddressRequest =
        parse_request(args, &["name", "branch", "annotation", "minimum_index"])?;
    if let Some(annotation) = request.annotation.as_deref() {
        ensure!(!annotation.is_empty(), "annotation cannot be empty");
        ensure!(
            annotation.len() <= MAX_ANNOTATION_BYTES,
            "annotation exceeds {MAX_ANNOTATION_BYTES} UTF-8 bytes"
        );
    }

    let _operation = plugin.state().lock_descriptor(&request.name).await;
    plugin.state().ensure_no_active_scan(&request.name)?;
    let records = plugin.state().records_snapshot().await;
    let mut record = records
        .get(&request.name)
        .cloned()
        .with_context(|| format!("descriptor '{}' is not registered", request.name))?;
    ensure!(
        record.status == DescriptorStatus::Active && record.initial_scan_complete,
        "descriptor '{}' is not ready to issue addresses",
        request.name
    );
    let descriptor = validate_config(&record.config, &plugin.state().network)?;
    ensure!(
        descriptor.is_ranged(),
        "non-ranged descriptors cannot issue new addresses"
    );
    ensure!(
        usize::try_from(request.branch)
            .ok()
            .is_some_and(|branch| branch < descriptor.branch_count()),
        "descriptor branch {} does not exist",
        request.branch
    );

    let observed_next = record
        .utxos
        .values()
        .filter(|utxo| utxo.branch == request.branch)
        .map(|utxo| utxo.derivation_index.saturating_add(1))
        .max()
        .unwrap_or(0);
    let issued_next = plugin
        .state()
        .issued_addresses(&request.name)
        .await
        .into_iter()
        .filter(|issued| issued.branch == request.branch)
        .map(|issued| issued.index.saturating_add(1))
        .max()
        .unwrap_or(0);
    let next_index = record
        .next_indexes
        .get(&request.branch)
        .copied()
        .unwrap_or(0)
        .max(observed_next)
        .max(issued_next);
    let index = next_index.max(request.minimum_index.unwrap_or(0));
    ensure!(
        index < (1 << 31) - 1,
        "address index exceeds unhardened indexes"
    );
    let derived = descriptor.derive_one(request.branch, index)?;
    let address = descriptor.derive_address(request.branch, index, &plugin.state().network)?;
    let mut issued = IssuedAddress {
        name: request.name.clone(),
        branch: request.branch,
        index,
        address,
        scriptpubkey: derived.scriptpubkey,
        annotation: request.annotation,
        issued_at: unix_time(),
        generation: None,
    };
    cln::save_issued_address(&plugin.state().rpc_path, &mut issued)
        .await
        .context("persisting allocated address metadata")?;
    plugin.state().put_issued_address(issued.clone()).await;
    record.next_indexes.insert(request.branch, index + 1);
    let branch_end = desired_branch_range_end(&record, request.branch, record.config.lookahead)?;
    record.range_ends.insert(request.branch, branch_end);
    refresh_aggregate_indexes(&mut record);
    let desired_scripts = descriptor_scripts(&record, &descriptor)?;
    ensure_no_script_overlap(
        &record.config.name,
        &desired_scripts,
        &records,
        &plugin.state().network,
    )?;

    record.status = DescriptorStatus::Syncing;
    cln::save_record(&plugin.state().rpc_path, &mut record)
        .await
        .context("persisting allocated descriptor address")?;
    plugin.state().put_record(record.clone()).await;
    if let Err(error) = reconcile_watches(plugin.state(), &record, false)
        .await
        .context("installing watches for allocated descriptor address")
    {
        plugin.state().put_record(record).await;
        drop(_operation);
        report_descriptor_failure(plugin.state(), &request.name, "new_address", &error, false)
            .await;
        return Err(error);
    }
    record.status = DescriptorStatus::Active;
    mark_success(&mut record, "new_address");
    cln::save_record(&plugin.state().rpc_path, &mut record)
        .await
        .context("activating allocated descriptor address")?;
    let response = json!({
        "name": record.config.name,
        "branch": issued.branch,
        "index": issued.index,
        "address": issued.address,
        "scriptpubkey": issued.scriptpubkey,
        "annotation": issued.annotation,
        "next_index": record.next_indexes.get(&issued.branch),
        "lookahead": record.config.lookahead,
        "range_end": record.range_end,
        "range_ends": record.range_ends,
    });
    plugin.state().put_record(record).await;
    Ok(response)
}

pub async fn list_addresses(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: ListAddressesRequest = parse_request(args, &["name", "branch", "start", "limit"])?;
    ensure!(
        (1..=1_000).contains(&request.limit),
        "limit must be between 1 and 1000"
    );
    ensure!(
        plugin.state().record(&request.name).await.is_some(),
        "descriptor '{}' is not registered",
        request.name
    );
    let mut addresses = plugin
        .state()
        .issued_addresses(&request.name)
        .await
        .into_iter()
        .filter(|address| address.branch == request.branch)
        .filter(|address| request.start.is_none_or(|start| address.index >= start))
        .collect::<Vec<_>>();
    addresses.sort_by_key(|address| (address.branch, address.index));
    let has_more = addresses.len() > usize::try_from(request.limit).unwrap_or(usize::MAX);
    addresses.truncate(usize::try_from(request.limit).unwrap_or(usize::MAX));
    let next_start = has_more
        .then(|| {
            addresses
                .last()
                .map(|address| address.index.saturating_add(1))
        })
        .flatten();
    Ok(json!({
        "name": request.name,
        "branch": request.branch,
        "addresses": addresses,
        "next_start": next_start,
    }))
}

fn json_msat_is_nonzero(value: Option<&Value>) -> bool {
    let Some(value) = value else {
        return false;
    };
    value.as_u64().is_some_and(|amount| amount > 0)
        || value
            .as_str()
            .and_then(|amount| amount.strip_suffix("msat"))
            .and_then(|amount| amount.parse::<u64>().ok())
            .is_some_and(|amount| amount > 0)
        || value
            .get("msat")
            .and_then(Value::as_u64)
            .is_some_and(|amount| amount > 0)
}

pub async fn sync_descriptions(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: SyncDescriptionsRequest = parse_request(args, &["name", "overwrite"])?;
    let _operation = plugin.state().lock_descriptor(&request.name).await;
    plugin.state().ensure_no_active_scan(&request.name)?;
    let record = plugin
        .state()
        .record(&request.name)
        .await
        .with_context(|| format!("descriptor '{}' is not registered", request.name))?;
    ensure!(
        record.status != DescriptorStatus::Deleting,
        "descriptor '{}' is being deleted",
        request.name
    );
    let events = cln::bookkeeper_account_events(&plugin.state().rpc_path, &request.name)
        .await
        .context("listing Bookkeeper events before description sync")?;
    let descriptions = events
        .iter()
        .filter(|event| json_msat_is_nonzero(event.get("credit_msat")))
        .filter_map(|event| {
            event
                .get("outpoint")
                .and_then(Value::as_str)
                .map(|outpoint| {
                    (
                        outpoint.to_owned(),
                        event
                            .get("description")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    )
                })
        })
        .collect::<BTreeMap<_, _>>();

    let mut synced = Vec::new();
    let mut unchanged = Vec::new();
    let mut conflicts = Vec::new();
    let mut missing_events = Vec::new();
    for utxo in record.utxos.values() {
        let Some(annotation) = utxo.annotation.as_deref() else {
            continue;
        };
        let Some(current) = descriptions.get(&utxo.outpoint) else {
            missing_events.push(utxo.outpoint.clone());
            continue;
        };
        if current.as_deref() == Some(annotation) {
            unchanged.push(utxo.outpoint.clone());
            continue;
        }
        if current.is_some() && !request.overwrite {
            conflicts.push(json!({
                "outpoint": utxo.outpoint,
                "tracker_annotation": annotation,
                "bookkeeper_description": current,
            }));
            continue;
        }
        cln::describe_utxo(&plugin.state().rpc_path, &utxo.outpoint, annotation)
            .await
            .with_context(|| format!("syncing Bookkeeper description for {}", utxo.outpoint))?;
        synced.push(utxo.outpoint.clone());
    }

    Ok(json!({
        "name": request.name,
        "overwrite": request.overwrite,
        "annotated_utxos": record.utxos.values().filter(|utxo| utxo.annotation.is_some()).count(),
        "synced": synced,
        "unchanged": unchanged,
        "conflicts": conflicts,
        "missing_events": missing_events,
    }))
}

pub async fn list(plugin: Plugin<AppState>, _args: Value) -> Result<Value> {
    let descriptors = plugin
        .state()
        .records_snapshot()
        .await
        .values()
        .map(|record| {
            let descriptor = validate_config(&record.config, &plugin.state().network)?;
            Ok(record_view(record, &descriptor))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({ "descriptors": descriptors }))
}

pub async fn update(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: UpdateRequest = parse_request(args, &["name", "lookahead", "confirmations"])?;
    ensure!(
        request.lookahead.is_some() || request.confirmations.is_some(),
        "lookahead or confirmations is required"
    );
    if let Some(lookahead) = request.lookahead {
        validate_lookahead(lookahead)?;
    }
    if let Some(confirmations) = request.confirmations {
        validate_confirmations(confirmations)?;
    }
    let _operation = plugin.state().lock_descriptor(&request.name).await;
    plugin.state().ensure_no_active_scan(&request.name)?;
    let records = plugin.state().records_snapshot().await;
    let old = records
        .get(&request.name)
        .cloned()
        .with_context(|| format!("descriptor '{}' is not registered", request.name))?;
    ensure!(
        old.status != DescriptorStatus::Deleting,
        "descriptor '{}' is being deleted",
        request.name
    );
    let descriptor = validate_config(&old.config, &plugin.state().network)?;
    let requested_lookahead = request.lookahead.unwrap_or(old.config.lookahead);
    let requested_confirmations = request.confirmations.unwrap_or(old.config.confirmations);
    if !descriptor.is_ranged() {
        ensure!(
            requested_lookahead == 1,
            "non-ranged descriptors require lookahead=1"
        );
    }
    if old.status == DescriptorStatus::Syncing {
        ensure!(
            old.pending_rescan.is_none(),
            "descriptor '{}' has an interrupted rescan; retry tracker-rescan first",
            request.name
        );
        ensure!(
            old.initial_scan_complete,
            "descriptor '{}' has an interrupted initial scan; retry tracker-register with the same configuration",
            request.name
        );
        ensure!(
            requested_lookahead == old.config.lookahead
                && requested_confirmations == old.config.confirmations,
            "descriptor '{}' has an interrupted update; retry lookahead={} confirmations={} first",
            request.name,
            old.config.lookahead,
            old.config.confirmations
        );
        let mut active = old;
        if let Err(error) = reconcile_watches(plugin.state(), &active, true)
            .await
            .context("resuming descriptor watch reconciliation")
        {
            plugin.state().put_record(active).await;
            drop(_operation);
            report_descriptor_failure(plugin.state(), &request.name, "update", &error, false).await;
            return Err(error);
        }
        active.status = DescriptorStatus::Active;
        mark_success(&mut active, "update");
        cln::save_record(&plugin.state().rpc_path, &mut active)
            .await
            .context("activating reconciled descriptor")?;
        let tip_height = cln::bwatch_status(&plugin.state().rpc_path)
            .await
            .context("reading bwatch height after update")?
            .current_height;
        deliver_mature_movements(plugin.state(), &mut active, tip_height).await?;
        let response = record_view(&active, &descriptor);
        plugin.state().put_record(active).await;
        return Ok(response);
    }
    if requested_lookahead == old.config.lookahead
        && requested_confirmations == old.config.confirmations
    {
        return Ok(record_view(&old, &descriptor));
    }

    let mut updated = old.clone();
    updated.config.lookahead = requested_lookahead;
    updated.config.confirmations = requested_confirmations;
    for branch in
        0..u32::try_from(descriptor.branch_count()).context("too many descriptor branches")?
    {
        let new_end = desired_branch_range_end(&updated, branch, requested_lookahead)?;
        updated.range_ends.insert(branch, new_end);
    }
    refresh_aggregate_indexes(&mut updated);
    let desired_scripts = descriptor_scripts(&updated, &descriptor)?;
    ensure_no_script_overlap(
        &updated.config.name,
        &desired_scripts,
        &records,
        &plugin.state().network,
    )?;
    updated.status = DescriptorStatus::Syncing;
    cln::save_record(&plugin.state().rpc_path, &mut updated)
        .await
        .context("persisting updated lookahead intent")?;
    plugin.state().put_record(updated.clone()).await;
    if let Err(error) = reconcile_watches(plugin.state(), &updated, true)
        .await
        .context("changing descriptor lookahead watches")
    {
        plugin.state().put_record(updated).await;
        drop(_operation);
        report_descriptor_failure(plugin.state(), &request.name, "update", &error, false).await;
        return Err(error);
    }
    updated.status = DescriptorStatus::Active;
    mark_success(&mut updated, "update");
    cln::save_record(&plugin.state().rpc_path, &mut updated)
        .await
        .context("activating updated lookahead")?;
    let tip_height = cln::bwatch_status(&plugin.state().rpc_path)
        .await
        .context("reading bwatch height after update")?
        .current_height;
    deliver_mature_movements(plugin.state(), &mut updated, tip_height).await?;
    let response = record_view(&updated, &descriptor);
    plugin.state().put_record(updated).await;
    Ok(response)
}

pub async fn health_snapshot(state: &AppState) -> Value {
    let bwatch = cln::bwatch_status(&state.rpc_path).await;
    let tracker = state.inner.lock().await;
    let mut active = 0_u64;
    let mut syncing = 0_u64;
    let mut deleting = 0_u64;
    let mut pending_movements = 0_u64;
    let mut incidents = Vec::new();
    for record in tracker.records.values() {
        pending_movements = pending_movements
            .saturating_add(u64::try_from(record.pending_movements.len()).unwrap_or(u64::MAX));
        match record.status {
            DescriptorStatus::Active => active += 1,
            DescriptorStatus::Syncing => syncing += 1,
            DescriptorStatus::Deleting => deleting += 1,
        }
        if let Some(incident) = &record.incident {
            incidents.push(json!({
                "descriptor": record.config.name,
                "status": record.status,
                "code": incident.code,
                "operation": incident.operation,
                "message": incident.message,
                "first_seen": incident.first_seen,
                "last_seen": incident.last_seen,
                "retry_count": incident.retry_count,
                "operator_action_required": incident.operator_action_required,
            }));
        }
    }
    let bwatch_healthy = bwatch.as_ref().is_ok_and(|status| {
        status.enabled
            && status.caught_up.unwrap_or(false)
            && status.last_poll_error.is_none()
            && status.last_rescan_error.is_none()
    });
    let healthy = incidents.is_empty() && syncing == 0 && deleting == 0 && bwatch_healthy;
    let bwatch_value = match bwatch {
        Ok(status) => serde_json::to_value(status).unwrap_or(Value::Null),
        Err(error) => json!({ "error": format!("{error:#}") }),
    };
    json!({
        "healthy": healthy,
        "descriptors": {
            "total": tracker.records.len(),
            "active": active,
            "syncing": syncing,
            "deleting": deleting,
            "pending_movements": pending_movements,
        },
        "incidents": incidents,
        "bwatch": bwatch_value,
        "counters": {
            "reconciliation_failures": tracker.reconciliation_failures,
            "bookkeeper_failures": tracker.bookkeeper_failures,
            "reorgs": tracker.reorgs,
        }
    })
}

pub async fn health(plugin: Plugin<AppState>, _args: Value) -> Result<Value> {
    Ok(health_snapshot(plugin.state()).await)
}

fn metrics_from_health(health: &Value) -> Value {
    let gauge = |name: &str, help: &str, value: Value| {
        json!({
            "name": name,
            "help": help,
            "type": "gauge",
            "samples": [{ "labels": {}, "value": value }],
        })
    };
    let counter = |name: &str, help: &str, value: Value| {
        json!({
            "name": name,
            "help": help,
            "type": "counter",
            "samples": [{ "labels": {}, "value": value }],
        })
    };
    let number = |pointer: &str| health.pointer(pointer).cloned().unwrap_or(json!(0));
    let mut incident_samples = Vec::new();
    for incident in health
        .get("incidents")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        incident_samples.push(json!({
            "labels": {
                "descriptor": incident.get("descriptor").and_then(Value::as_str).unwrap_or("unknown"),
                "code": incident.get("code").and_then(Value::as_str).unwrap_or("unknown"),
                "operation": incident.get("operation").and_then(Value::as_str).unwrap_or("unknown"),
            },
            "value": 1,
        }));
    }
    let descriptor_samples = ["active", "syncing", "deleting"]
        .into_iter()
        .map(|status| {
            json!({
                "labels": { "status": status },
                "value": number(&format!("/descriptors/{status}")),
            })
        })
        .collect::<Vec<_>>();
    let active_rescans = health
        .pointer("/bwatch/active_rescans")
        .and_then(Value::as_array);
    let rescan_count = active_rescans.map_or(0, Vec::len);
    let rescan_blocks_processed = active_rescans
        .into_iter()
        .flatten()
        .filter_map(|rescan| rescan.get("blocks_processed").and_then(Value::as_u64))
        .sum::<u64>();
    let rescan_blocks_total = active_rescans
        .into_iter()
        .flatten()
        .filter_map(|rescan| rescan.get("blocks_total").and_then(Value::as_u64))
        .sum::<u64>();
    let rescan_script_matches = active_rescans
        .into_iter()
        .flatten()
        .filter_map(|rescan| rescan.get("script_matches_found").and_then(Value::as_u64))
        .sum::<u64>();
    let rescan_outpoint_matches = active_rescans
        .into_iter()
        .flatten()
        .filter_map(|rescan| rescan.get("outpoint_matches_found").and_then(Value::as_u64))
        .sum::<u64>();
    let rescan_outpoints_followed = active_rescans
        .into_iter()
        .flatten()
        .filter_map(|rescan| rescan.get("outpoints_followed").and_then(Value::as_u64))
        .sum::<u64>();
    let rescan_progress_ratio = if rescan_blocks_total == 0 {
        0.0
    } else {
        rescan_blocks_processed as f64 / rescan_blocks_total as f64
    };
    let families = vec![
        gauge(
            "healthy",
            "Whether Tracker, its descriptors, and bwatch are healthy.",
            json!(u8::from(
                health
                    .get("healthy")
                    .and_then(Value::as_bool)
                    .is_some_and(|value| value)
            )),
        ),
        json!({
            "name": "descriptors",
            "help": "Registered Tracker descriptors by lifecycle state.",
            "type": "gauge",
            "samples": descriptor_samples,
        }),
        gauge(
            "pending_movements",
            "Bookkeeper movements waiting for their confirmation depth.",
            number("/descriptors/pending_movements"),
        ),
        json!({
            "name": "incident",
            "help": "Unresolved Tracker incidents by descriptor and operation.",
            "type": "gauge",
            "samples": incident_samples,
        }),
        gauge(
            "bwatch_height",
            "Last block height processed by bwatch.",
            number("/bwatch/current_height"),
        ),
        gauge(
            "bwatch_lag_blocks",
            "Number of blocks bwatch is behind the chain tip.",
            number("/bwatch/lag"),
        ),
        gauge(
            "bwatch_active_rescans",
            "Number of historical bwatch rescans currently in progress.",
            json!(rescan_count),
        ),
        gauge(
            "bwatch_rescan_blocks_processed",
            "Blocks processed across active historical bwatch rescans.",
            json!(rescan_blocks_processed),
        ),
        gauge(
            "bwatch_rescan_blocks_total",
            "Total blocks across active historical bwatch rescans.",
            json!(rescan_blocks_total),
        ),
        gauge(
            "bwatch_rescan_progress_ratio",
            "Aggregate completion ratio across active historical bwatch rescans.",
            json!(rescan_progress_ratio),
        ),
        gauge(
            "bwatch_rescan_script_matches_found",
            "Descriptor outputs found across active historical bwatch rescans.",
            json!(rescan_script_matches),
        ),
        gauge(
            "bwatch_rescan_outpoint_matches_found",
            "Spending inputs found across active historical bwatch rescans.",
            json!(rescan_outpoint_matches),
        ),
        gauge(
            "bwatch_rescan_outpoints_followed",
            "Unique matched outputs followed across active historical bwatch rescans.",
            json!(rescan_outpoints_followed),
        ),
        counter(
            "reconciliation_failures_total",
            "Tracker watch reconciliation failures in this process.",
            number("/counters/reconciliation_failures"),
        ),
        counter(
            "bookkeeper_failures_total",
            "Tracker Bookkeeper delivery failures in this process.",
            number("/counters/bookkeeper_failures"),
        ),
        counter(
            "reorgs_total",
            "Chain reorganizations handled by Tracker in this process.",
            number("/counters/reorgs"),
        ),
    ];

    json!({
        "version": 1,
        "namespace": "tracker",
        "families": families,
    })
}

pub async fn metrics(plugin: Plugin<AppState>, _args: Value) -> Result<Value> {
    Ok(metrics_from_health(&health_snapshot(plugin.state()).await))
}

pub async fn ack_incident(plugin: Plugin<AppState>, args: Value) -> Result<Value> {
    let request: NameRequest = parse_request(args, &["name"])?;
    let _operation = plugin.state().lock_descriptor(&request.name).await;
    let mut record = plugin
        .state()
        .record(&request.name)
        .await
        .with_context(|| format!("descriptor '{}' is not registered", request.name))?;
    if let Some(incident) = &record.incident {
        ensure!(
            incident.operator_action_required,
            "incident '{}' is operational and clears only after a successful retry",
            incident.code
        );
    }
    let removed = record.incident.take();
    if removed.is_some() {
        record.last_success_at = Some(unix_time());
        cln::save_record(&plugin.state().rpc_path, &mut record)
            .await
            .context("acknowledging tracker incident")?;
        log::info!(
            "tracker incident acknowledged descriptor='{}'",
            request.name
        );
        plugin.state().put_record(record).await;
    }
    Ok(json!({ "name": request.name, "acknowledged": removed.is_some() }))
}

pub fn record_view(record: &DescriptorRecord, descriptor: &DescriptorSet) -> Value {
    json!({
        "name": record.config.name,
        "descriptor": record.config.descriptor,
        "birthheight": record.config.birthheight,
        "lookahead": record.config.lookahead,
        "confirmations": record.config.confirmations,
        "range_end": record.range_end,
        "last_used_index": record.last_used_index,
        "status": record.status,
        "initial_scan_complete": record.initial_scan_complete,
        "pending_rescan": record.pending_rescan,
        "scan_operation_id": record.scan_operation_id,
        "incident": record.incident,
        "last_success_at": record.last_success_at,
        "branches": descriptor.branch_count(),
        "next_indexes": record.next_indexes,
        "range_ends": record.range_ends,
        "last_used_indexes": record.last_used_indexes,
        "tracked_utxos": record.utxos.values().collect::<Vec<_>>(),
        "external_outpoints": record.external_outpoints,
        "pending_movements": record.pending_movements.values().collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use miniscript::descriptor::{Descriptor, DescriptorPublicKey};
    use std::str::FromStr;

    fn checked_descriptor(body: &str) -> String {
        Descriptor::<DescriptorPublicKey>::from_str(body)
            .unwrap()
            .to_string()
    }

    #[test]
    fn positional_rpc_parameters_are_supported() {
        let request: UpdateRequest = parse_request(
            json!(["treasury", 50]),
            &["name", "lookahead", "confirmations"],
        )
        .unwrap();
        assert_eq!(request.name, "treasury");
        assert_eq!(request.lookahead, Some(50));
        assert_eq!(request.confirmations, None);
    }

    #[test]
    fn excess_positional_parameters_are_rejected() {
        let result = parse_request::<NameRequest>(json!(["treasury", "extra"]), &["name"]);
        assert!(result.is_err());
    }

    #[test]
    fn rescan_parameters_are_optional() {
        let request: RescanRequest = parse_request(
            json!({ "name": "treasury" }),
            &["name", "start_block", "lookahead"],
        )
        .unwrap();
        assert_eq!(request.name, "treasury");
        assert_eq!(request.start_block, None);
        assert_eq!(request.lookahead, None);

        let request: RescanRequest = parse_request(
            json!(["treasury", 850_000, 500]),
            &["name", "start_block", "lookahead"],
        )
        .unwrap();
        assert_eq!(request.start_block, Some(850_000));
        assert_eq!(request.lookahead, Some(500));
    }

    #[test]
    fn persisted_rescan_intent_is_resumable_with_exact_parameters() {
        let pending = PendingRescan {
            start_block: 825_000,
        };
        assert!(rescan_resume_parameters_match(&pending, 200, 825_000, 200));
        assert!(!rescan_resume_parameters_match(&pending, 200, 825_001, 200));
        assert!(!rescan_resume_parameters_match(&pending, 200, 825_000, 201));
    }

    #[test]
    fn lookahead_is_a_gap_beyond_the_last_used_index() {
        assert_eq!(desired_range_end(None, 20).unwrap(), 20);
        assert_eq!(desired_range_end(Some(7), 20).unwrap(), 28);
    }

    #[test]
    fn movement_maturity_uses_inclusive_confirmation_depth() {
        assert!(movement_is_mature(100, 1, 100));
        assert!(!movement_is_mature(100, 2, 100));
        assert!(movement_is_mature(100, 2, 101));
        assert!(!movement_is_mature(u32::MAX, 2, u32::MAX));
    }

    #[test]
    fn block_counts_map_to_zero_indexed_rpc_progress() {
        assert_eq!(rpc_progress(0, 100), None);
        assert_eq!(rpc_progress(1, 100), Some((0, 100)));
        assert_eq!(rpc_progress(50, 100), Some((49, 100)));
        assert_eq!(rpc_progress(100, 100), Some((99, 100)));
        assert_eq!(rpc_progress(101, 100), Some((99, 100)));
        assert_eq!(rpc_progress(1, 1), None);
        assert_eq!(rpc_progress(1, u64::from(u32::MAX) + 1), None);
    }

    #[test]
    fn active_scans_are_single_flight_and_release_on_drop() {
        let state = AppState::new(PathBuf::from("/tmp/lightning-rpc"), "regtest".to_owned());
        let first = state.begin_scan("treasury").unwrap();
        let first_id = first.operation_id().to_owned();

        assert!(state.ensure_no_active_scan("treasury").is_err());
        assert!(state.begin_scan("treasury").is_err());
        assert!(state.begin_scan("savings").is_ok());

        drop(first);
        state.ensure_no_active_scan("treasury").unwrap();
        let replacement = state.begin_scan("treasury").unwrap();
        assert_ne!(replacement.operation_id(), first_id);
    }

    #[test]
    fn booked_reorg_has_a_distinct_incident_code() {
        assert_eq!(
            incident_code(
                "bookkeeper_reorg",
                "Bookkeeper movements require manual reconciliation"
            ),
            "bookkeeper_reorg"
        );
        assert_eq!(
            incident_code("bookkeeper_delivery", "Bookkeeper RPC failed"),
            "bookkeeper_injection_failed"
        );
    }

    #[test]
    fn metrics_contract_is_versioned_and_bounded() {
        let health = json!({
            "healthy": true,
            "descriptors": {
                "active": 2,
                "syncing": 1,
                "deleting": 0,
                "pending_movements": 3
            },
            "incidents": [{
                "descriptor": "treasury",
                "code": "bookkeeper_delivery_failed",
                "operation": "deliver"
            }],
            "bwatch": {
                "current_height": 42,
                "lag": 1,
                "active_rescans": [{
                    "blocks_processed": 25,
                    "blocks_total": 100,
                    "script_matches_found": 7,
                    "outpoint_matches_found": 3,
                    "outpoints_followed": 6
                }]
            },
            "counters": {
                "reconciliation_failures": 4,
                "bookkeeper_failures": 5,
                "reorgs": 6
            }
        });
        let metrics = metrics_from_health(&health);

        assert_eq!(metrics["version"], 1);
        assert_eq!(metrics["namespace"], "tracker");
        assert_eq!(metrics["families"][0]["name"], "healthy");
        assert_eq!(metrics["families"][0]["samples"][0]["value"], 1);
        assert_eq!(metrics["families"][1]["samples"][0]["value"], 2);
        assert_eq!(
            metrics["families"][3]["samples"][0]["labels"]["descriptor"],
            "treasury"
        );
        let family = |name: &str| {
            metrics["families"]
                .as_array()
                .unwrap()
                .iter()
                .find(|family| family["name"] == name)
                .unwrap()
        };
        assert_eq!(family("bwatch_active_rescans")["samples"][0]["value"], 1);
        assert_eq!(
            family("bwatch_rescan_blocks_processed")["samples"][0]["value"],
            25
        );
        assert_eq!(
            family("bwatch_rescan_progress_ratio")["samples"][0]["value"],
            0.25
        );
        assert_eq!(
            family("bwatch_rescan_script_matches_found")["samples"][0]["value"],
            7
        );
        assert_eq!(
            family("bwatch_rescan_outpoint_matches_found")["samples"][0]["value"],
            3
        );
        assert_eq!(
            family("bwatch_rescan_outpoints_followed")["samples"][0]["value"],
            6
        );
        assert_eq!(family("reorgs_total")["name"], "reorgs_total");
    }

    #[test]
    fn overlapping_registered_scripts_are_rejected() {
        let descriptor = checked_descriptor(
            "wpkh(0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798)",
        );
        let config = DescriptorConfig {
            name: "first".to_owned(),
            descriptor: descriptor.clone(),
            birthheight: 1,
            lookahead: 1,
            confirmations: 1,
        };
        let mut records = BTreeMap::new();
        records.insert(
            config.name.clone(),
            DescriptorRecord {
                config,
                utxos: BTreeMap::new(),
                pending_movements: BTreeMap::new(),
                external_outpoints: BTreeSet::new(),
                next_indexes: BTreeMap::new(),
                range_ends: BTreeMap::from([(0, 1)]),
                last_used_indexes: BTreeMap::new(),
                range_end: 1,
                last_used_index: None,
                status: DescriptorStatus::Active,
                initial_scan_complete: true,
                pending_rescan: None,
                scan_operation_id: None,
                incident: None,
                last_success_at: None,
                generation: None,
            },
        );
        let candidate = DescriptorSet::parse_checked(&descriptor, "regtest")
            .unwrap()
            .derive_range(0, 1)
            .unwrap();

        assert!(ensure_no_script_overlap("second", &candidate, &records, "regtest").is_err());
    }
}
