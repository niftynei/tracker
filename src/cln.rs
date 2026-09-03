use crate::model::{
    BwatchScanResult, DescriptorRecord, IssuedAddress, STORE_ADDRESSES, STORE_DESCRIPTORS,
    STORE_PREFIX,
};
use anyhow::{Context, Result, anyhow};
use cln_rpc::ClnRpc;
use miniscript::bitcoin::{Block, Transaction, consensus};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::Path;

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct BwatchError {
    pub height: u32,
    #[serde(default)]
    pub target_height: Option<u32>,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct BwatchRescan {
    pub watch_type: String,
    #[serde(default)]
    pub owners: Vec<String>,
    pub start_height: u32,
    pub current_height: u32,
    pub target_height: u32,
    pub blocks_processed: u64,
    pub blocks_total: u64,
    pub progress_percent: u64,
    #[serde(default)]
    pub script_matches_found: u64,
    #[serde(default)]
    pub outpoint_matches_found: u64,
    #[serde(default)]
    pub outpoints_followed: u64,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct BwatchStatus {
    pub enabled: bool,
    pub current_height: u32,
    #[serde(default)]
    pub backend_height: Option<u32>,
    #[serde(default)]
    pub lag: Option<u32>,
    #[serde(default)]
    pub caught_up: Option<bool>,
    #[serde(default)]
    pub last_poll_error: Option<BwatchError>,
    #[serde(default)]
    pub last_rescan_error: Option<BwatchError>,
    #[serde(default)]
    pub active_rescans: Vec<BwatchRescan>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OwnedWatch {
    Script { owner: String, scriptpubkey: String },
    Outpoint { owner: String, outpoint: String },
    Blockdepth { owner: String, start_block: u32 },
}

async fn call(path: &Path, method: &str, params: Value) -> Result<Value> {
    let mut rpc = ClnRpc::new(path)
        .await
        .with_context(|| format!("connecting to CLN RPC at {}", path.display()))?;
    rpc.call_raw(method, &params)
        .await
        .map_err(|e| anyhow!("CLN RPC {method} failed: {e:?}"))
}

pub async fn bwatch_status(path: &Path) -> Result<BwatchStatus> {
    serde_json::from_value(call(path, "bwatch-status", json!({})).await?)
        .context("decoding bwatch-status response")
}

pub async fn add_script_watches(
    path: &Path,
    watches: &[(String, String)],
    birthheight: u32,
) -> Result<()> {
    if watches.is_empty() {
        return Ok(());
    }
    let params = json!({
        "watches": watches
            .iter()
            .map(|(owner, scriptpubkey)| json!({
                "owner": owner,
                "scriptpubkey": scriptpubkey,
            }))
            .collect::<Vec<_>>(),
        "start_block": birthheight,
        "rescan": false,
    });
    let mut rpc = ClnRpc::new(path)
        .await
        .with_context(|| format!("connecting to CLN RPC at {}", path.display()))?;
    rpc.call_raw::<Value, _>("addscriptpubkeywatches", &params)
        .await
        .map_err(|error| anyhow!("CLN RPC addscriptpubkeywatches failed: {error:?}"))?;
    Ok(())
}

pub async fn rescan_watch_owners(path: &Path, owners: &[String], start_block: u32) -> Result<()> {
    if owners.is_empty() {
        return Ok(());
    }
    let params = json!({
        "owners": owners,
        "start_block": start_block,
    });
    let mut rpc = ClnRpc::new(path)
        .await
        .with_context(|| format!("connecting to CLN RPC at {}", path.display()))?;
    rpc.call_raw::<Value, _>("rescanwatchset", &params)
        .await
        .map_err(|error| anyhow!("CLN RPC rescanwatchset failed: {error:?}"))?;
    Ok(())
}

pub async fn rescan_watch_prefix(path: &Path, owner_prefix: &str, start_block: u32) -> Result<()> {
    let params = json!({
        "owner_prefix": owner_prefix,
        "start_block": start_block,
    });
    let mut rpc = ClnRpc::new(path)
        .await
        .with_context(|| format!("connecting to CLN RPC at {}", path.display()))?;
    rpc.call_raw::<Value, _>("rescanwatchset", &params)
        .await
        .map_err(|error| anyhow!("CLN RPC rescanwatchset failed: {error:?}"))?;
    Ok(())
}

pub async fn scan_watch_set(
    path: &Path,
    watches: &[(String, String)],
    start_block: u32,
) -> Result<BwatchScanResult> {
    let mut rpc = ClnRpc::new(path)
        .await
        .with_context(|| format!("connecting to CLN RPC at {}", path.display()))?;
    let params = json!({
        "watches": watches
            .iter()
            .map(|(owner, scriptpubkey)| json!({
                "owner": owner,
                "scriptpubkey": scriptpubkey,
            }))
            .collect::<Vec<_>>(),
        "start_block": start_block,
    });
    rpc.call_raw::<BwatchScanResult, _>("scanwatchset", &params)
        .await
        .map_err(|error| anyhow!("CLN RPC scanwatchset failed: {error:?}"))
}

pub async fn del_script_watch(path: &Path, owner: &str, scriptpubkey: &str) -> Result<()> {
    call(
        path,
        "delscriptpubkeywatch",
        json!({ "owner": owner, "scriptpubkey": scriptpubkey }),
    )
    .await?;
    Ok(())
}

pub async fn add_outpoint_watch(
    path: &Path,
    owner: &str,
    outpoint: &str,
    start_block: u32,
    rescan: bool,
) -> Result<()> {
    call(
        path,
        "addoutpointwatch",
        json!({
            "owner": owner,
            "outpoint": outpoint,
            "start_block": start_block,
            "rescan": rescan,
        }),
    )
    .await?;
    Ok(())
}

pub async fn del_outpoint_watch(path: &Path, owner: &str, outpoint: &str) -> Result<()> {
    call(
        path,
        "deloutpointwatch",
        json!({ "owner": owner, "outpoint": outpoint }),
    )
    .await?;
    Ok(())
}

pub async fn del_blockdepth_watch(path: &Path, owner: &str, start_block: u32) -> Result<()> {
    call(
        path,
        "delblockdepthwatch",
        json!({ "owner": owner, "start_block": start_block }),
    )
    .await?;
    Ok(())
}

pub async fn list_owned_watches(path: &Path, owner_prefix: &str) -> Result<Vec<OwnedWatch>> {
    let response = call(path, "listwatch", json!({})).await?;
    let watches = response
        .get("watches")
        .and_then(Value::as_array)
        .context("listwatch response omitted watches")?;
    let mut owned = Vec::new();
    for watch in watches {
        let watch_type = watch.get("type").and_then(Value::as_str);
        let owners = watch
            .get("owners")
            .and_then(Value::as_array)
            .context("listwatch entry omitted owners")?;
        for owner in owners.iter().filter_map(Value::as_str) {
            if !owner.starts_with(owner_prefix) {
                continue;
            }
            match watch_type {
                Some("scriptpubkey") => owned.push(OwnedWatch::Script {
                    owner: owner.to_owned(),
                    scriptpubkey: watch
                        .get("scriptpubkey")
                        .and_then(Value::as_str)
                        .context("scriptpubkey watch omitted scriptpubkey")?
                        .to_owned(),
                }),
                Some("outpoint") => owned.push(OwnedWatch::Outpoint {
                    owner: owner.to_owned(),
                    outpoint: watch
                        .get("outpoint")
                        .and_then(Value::as_str)
                        .context("outpoint watch omitted outpoint")?
                        .to_owned(),
                }),
                Some("blockdepth") => owned.push(OwnedWatch::Blockdepth {
                    owner: owner.to_owned(),
                    start_block: watch
                        .get("start_block")
                        .and_then(Value::as_u64)
                        .and_then(|height| u32::try_from(height).ok())
                        .context("blockdepth watch omitted valid start_block")?,
                }),
                _ => {}
            }
        }
    }
    Ok(owned)
}

pub async fn del_owned_watch(path: &Path, watch: &OwnedWatch) -> Result<()> {
    match watch {
        OwnedWatch::Script {
            owner,
            scriptpubkey,
        } => del_script_watch(path, owner, scriptpubkey).await,
        OwnedWatch::Outpoint { owner, outpoint } => del_outpoint_watch(path, owner, outpoint).await,
        OwnedWatch::Blockdepth { owner, start_block } => {
            del_blockdepth_watch(path, owner, *start_block).await
        }
    }
}

pub async fn inject_utxo_deposit(
    path: &Path,
    account: &str,
    outpoint: &str,
    amount_msat: u64,
    timestamp: u64,
    blockheight: u32,
) -> Result<()> {
    call(
        path,
        "injectutxodeposit",
        json!({
            "account": account,
            "outpoint": outpoint,
            "amount_msat": format!("{amount_msat}msat"),
            "timestamp": timestamp,
            "blockheight": blockheight,
        }),
    )
    .await?;
    Ok(())
}

pub async fn inject_external_deposit(
    path: &Path,
    transfer_from: &str,
    outpoint: &str,
    amount_msat: u64,
    timestamp: u64,
    blockheight: u32,
) -> Result<()> {
    call(
        path,
        "injectutxodeposit",
        json!({
            "account": "external",
            "transfer_from": transfer_from,
            "outpoint": outpoint,
            "amount_msat": format!("{amount_msat}msat"),
            "timestamp": timestamp,
            "blockheight": blockheight,
        }),
    )
    .await?;
    Ok(())
}

pub async fn describe_utxo(path: &Path, outpoint: &str, description: &str) -> Result<()> {
    call(
        path,
        "bkpr-editdescriptionbyoutpoint",
        json!({
            "outpoint": outpoint,
            "description": description,
        }),
    )
    .await?;
    Ok(())
}

pub async fn bookkeeper_account_events(path: &Path, account: &str) -> Result<Vec<Value>> {
    let response = call(
        path,
        "bkpr-listaccountevents",
        json!({ "account": account }),
    )
    .await?;
    response
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .context("bkpr-listaccountevents response omitted events")
}

pub async fn inject_utxo_spend(
    path: &Path,
    account: &str,
    outpoint: &str,
    spending_txid: &str,
    amount_msat: u64,
    timestamp: u64,
    blockheight: u32,
) -> Result<()> {
    call(
        path,
        "injectutxospend",
        json!({
            "account": account,
            "outpoint": outpoint,
            "spending_txid": spending_txid,
            "amount_msat": format!("{amount_msat}msat"),
            "timestamp": timestamp,
            "blockheight": blockheight,
        }),
    )
    .await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct DatastoreResponse {
    generation: u64,
}

#[derive(Debug, Deserialize)]
struct ListDatastoreResponse {
    datastore: Vec<DatastoreEntry>,
}

#[derive(Debug, Deserialize)]
struct DatastoreEntry {
    key: Vec<String>,
    generation: Option<u64>,
    string: Option<String>,
}

pub async fn load_records(path: &Path) -> Result<Vec<DescriptorRecord>> {
    let response: ListDatastoreResponse = serde_json::from_value(
        call(
            path,
            "listdatastore",
            json!({ "key": [STORE_PREFIX, STORE_DESCRIPTORS] }),
        )
        .await?,
    )
    .context("decoding listdatastore response")?;

    let mut records = Vec::with_capacity(response.datastore.len());
    for entry in response.datastore {
        if entry.key.len() != 3 || entry.key[0] != STORE_PREFIX || entry.key[1] != STORE_DESCRIPTORS
        {
            continue;
        }
        let data = entry
            .string
            .with_context(|| format!("tracker datastore entry {:?} is not UTF-8", entry.key))?;
        let mut record: DescriptorRecord = serde_json::from_str(&data)
            .with_context(|| format!("decoding tracker descriptor '{}'", entry.key[2]))?;
        anyhow::ensure!(
            record.config.name == entry.key[2],
            "tracker datastore key '{}' contains descriptor named '{}'",
            entry.key[2],
            record.config.name
        );
        record.generation = entry.generation;
        records.push(record);
    }
    Ok(records)
}

pub async fn load_issued_addresses(path: &Path) -> Result<Vec<IssuedAddress>> {
    // listdatastore returns only the immediate children of a key. Address
    // records deliberately use a hierarchy so a descriptor's namespace is
    // inspectable, therefore walk name -> branch -> index to reach each leaf.
    let names = list_datastore_children(path, &[STORE_PREFIX, STORE_ADDRESSES]).await?;
    let mut entries = Vec::new();
    for name_entry in names.datastore {
        if name_entry.key.len() != 3 {
            continue;
        }
        let name = name_entry.key[2].as_str();
        let branches = list_datastore_children(path, &[STORE_PREFIX, STORE_ADDRESSES, name])
            .await
            .with_context(|| format!("listing address branches for '{name}'"))?;
        for branch_entry in branches.datastore {
            if branch_entry.key.len() != 4 {
                continue;
            }
            let branch = branch_entry.key[3].as_str();
            entries.extend(
                list_datastore_children(path, &[STORE_PREFIX, STORE_ADDRESSES, name, branch])
                    .await
                    .with_context(|| {
                        format!("listing address indexes for '{name}' branch {branch}")
                    })?
                    .datastore,
            );
        }
    }

    let mut addresses = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.key.len() != 5 || entry.key[0] != STORE_PREFIX || entry.key[1] != STORE_ADDRESSES {
            continue;
        }
        let data = entry
            .string
            .with_context(|| format!("tracker address entry {:?} is not UTF-8", entry.key))?;
        let mut address: IssuedAddress = serde_json::from_str(&data)
            .with_context(|| format!("decoding tracker address entry {:?}", entry.key))?;
        anyhow::ensure!(
            address.name == entry.key[2]
                && address.branch.to_string() == entry.key[3]
                && address.index.to_string() == entry.key[4],
            "tracker address datastore key does not match its value"
        );
        address.generation = entry.generation;
        addresses.push(address);
    }
    Ok(addresses)
}

async fn list_datastore_children(path: &Path, key: &[&str]) -> Result<ListDatastoreResponse> {
    serde_json::from_value(call(path, "listdatastore", json!({ "key": key })).await?)
        .context("decoding listdatastore response")
}

pub async fn save_issued_address(path: &Path, address: &mut IssuedAddress) -> Result<()> {
    let data = serde_json::to_string(address).context("serializing issued address")?;
    let mut params = json!({
        "key": [
            STORE_PREFIX,
            STORE_ADDRESSES,
            address.name,
            address.branch.to_string(),
            address.index.to_string(),
        ],
        "string": data,
    });
    if let Some(generation) = address.generation {
        params["mode"] = json!("must-replace");
        params["generation"] = json!(generation);
    } else {
        params["mode"] = json!("must-create");
    }
    let response: DatastoreResponse =
        serde_json::from_value(call(path, "datastore", params).await?)
            .context("decoding issued-address datastore response")?;
    address.generation = Some(response.generation);
    Ok(())
}

pub async fn delete_issued_addresses(path: &Path, name: &str) -> Result<()> {
    let addresses = load_issued_addresses(path).await?;
    for address in addresses.into_iter().filter(|address| address.name == name) {
        let mut params = json!({
            "key": [
                STORE_PREFIX,
                STORE_ADDRESSES,
                address.name,
                address.branch.to_string(),
                address.index.to_string(),
            ],
        });
        if let Some(generation) = address.generation {
            params["generation"] = json!(generation);
        }
        call(path, "deldatastore", params).await?;
    }
    Ok(())
}

pub async fn save_record(path: &Path, record: &mut DescriptorRecord) -> Result<()> {
    let data = serde_json::to_string(record).context("serializing descriptor record")?;
    let mut params = json!({
        "key": [STORE_PREFIX, STORE_DESCRIPTORS, record.config.name],
        "string": data,
    });
    if let Some(generation) = record.generation {
        params["mode"] = json!("must-replace");
        params["generation"] = json!(generation);
    } else {
        params["mode"] = json!("must-create");
    }
    let response: DatastoreResponse =
        serde_json::from_value(call(path, "datastore", params).await?)
            .context("decoding datastore response")?;
    record.generation = Some(response.generation);
    Ok(())
}

pub async fn delete_record(path: &Path, record: &DescriptorRecord) -> Result<()> {
    let mut params = json!({
        "key": [STORE_PREFIX, STORE_DESCRIPTORS, record.config.name],
    });
    if let Some(generation) = record.generation {
        params["generation"] = json!(generation);
    }
    call(path, "deldatastore", params).await?;
    Ok(())
}

pub fn parse_transaction(raw: &str) -> Result<Transaction> {
    consensus::encode::deserialize_hex(raw).context("decoding raw transaction from bwatch")
}

pub async fn block_timestamp(path: &Path, height: u32) -> Result<u64> {
    let response = call(path, "getrawblockbyheight", json!({ "height": height })).await?;
    let raw = response
        .get("block")
        .and_then(Value::as_str)
        .with_context(|| format!("getrawblockbyheight returned no block for height {height}"))?;
    let block: Block = consensus::encode::deserialize_hex(raw)
        .with_context(|| format!("decoding block at height {height}"))?;
    Ok(u64::from(block.header.time))
}

#[cfg(test)]
mod tests {
    use super::BwatchStatus;
    use serde_json::json;

    #[test]
    fn live_rescan_match_counters_are_relayed() {
        let status: BwatchStatus = serde_json::from_value(json!({
            "enabled": true,
            "current_height": 852023,
            "active_rescans": [{
                "watch_type": "set",
                "owners": ["plugin/tracker/treasury/spk/0/0"],
                "start_height": 825000,
                "current_height": 852023,
                "target_height": 964806,
                "blocks_processed": 27023,
                "blocks_total": 139807,
                "progress_percent": 19,
                "script_matches_found": 12,
                "outpoint_matches_found": 5,
                "outpoints_followed": 11
            }]
        }))
        .unwrap();
        let scan = &status.active_rescans[0];

        assert_eq!(scan.script_matches_found, 12);
        assert_eq!(scan.outpoint_matches_found, 5);
        assert_eq!(scan.outpoints_followed, 11);
    }
}
