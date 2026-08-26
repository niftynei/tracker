use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const STORE_PREFIX: &str = "tracker";
pub const STORE_DESCRIPTORS: &str = "descriptors";
pub const OWNER_PREFIX: &str = "plugin/tracker";
pub const DEFAULT_LOOKAHEAD: u32 = 20;
pub const DEFAULT_CONFIRMATIONS: u32 = 1;
pub const MAX_LOOKAHEAD: u32 = 100_000;
pub const MAX_CONFIRMATIONS: u32 = 2_016;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DescriptorConfig {
    pub name: String,
    pub descriptor: String,
    pub birthheight: u32,
    pub lookahead: u32,
    #[serde(default = "default_confirmations")]
    pub confirmations: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MovementKind {
    Deposit,
    Spend,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingMovement {
    pub id: String,
    pub kind: MovementKind,
    pub outpoint: String,
    pub amount_msat: u64,
    pub blockheight: u32,
    pub timestamp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spending_txid: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TrackedUtxo {
    pub outpoint: String,
    pub amount_msat: u64,
    pub derivation_index: u32,
    pub branch: u32,
    pub deposit_height: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spent_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spent_height: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DescriptorRecord {
    #[serde(flatten)]
    pub config: DescriptorConfig,
    #[serde(default)]
    pub utxos: BTreeMap<String, TrackedUtxo>,
    #[serde(default)]
    pub pending_movements: BTreeMap<String, PendingMovement>,
    #[serde(default)]
    pub range_end: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_index: Option<u32>,
    #[serde(default)]
    pub status: DescriptorStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incident: Option<TrackerIncident>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<u64>,
    #[serde(skip)]
    pub generation: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TrackerIncident {
    pub code: String,
    pub operation: String,
    pub message: String,
    pub first_seen: u64,
    pub last_seen: u64,
    pub retry_count: u64,
    #[serde(default)]
    pub operator_action_required: bool,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorStatus {
    Syncing,
    #[default]
    Active,
    Deleting,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub descriptor: String,
    pub birthheight: u32,
    #[serde(default = "default_lookahead")]
    pub lookahead: u32,
    #[serde(default = "default_confirmations")]
    pub confirmations: u32,
}

fn default_lookahead() -> u32 {
    DEFAULT_LOOKAHEAD
}

fn default_confirmations() -> u32 {
    DEFAULT_CONFIRMATIONS
}

#[derive(Clone, Debug, Deserialize)]
pub struct NameRequest {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpdateRequest {
    pub name: String,
    #[serde(default)]
    pub lookahead: Option<u32>,
    #[serde(default)]
    pub confirmations: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ReconcileRequest {
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BwatchMatch {
    pub owner: String,
    pub watch_type: String,
    pub blockheight: u32,
    #[serde(default)]
    pub tx: Option<String>,
    #[serde(default)]
    pub index: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BwatchBlock {
    pub blockheight: u32,
    pub blockhash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchOwner {
    Script {
        name: String,
        branch: u32,
        index: u32,
    },
    Outpoint {
        name: String,
        outpoint: String,
    },
}

pub fn script_owner(name: &str, branch: u32, index: u32) -> String {
    format!("{OWNER_PREFIX}/{name}/spk/{branch}/{index}")
}

pub fn outpoint_owner(name: &str, outpoint: &str) -> String {
    format!("{OWNER_PREFIX}/{name}/outpoint/{outpoint}")
}

pub fn parse_owner(owner: &str) -> Option<WatchOwner> {
    let rest = owner.strip_prefix(&format!("{OWNER_PREFIX}/"))?;
    let mut parts = rest.split('/');
    let name = parts.next()?.to_owned();
    match parts.next()? {
        "spk" => {
            let branch = parts.next()?.parse().ok()?;
            let index = parts.next()?.parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            Some(WatchOwner::Script {
                name,
                branch,
                index,
            })
        }
        "outpoint" => {
            let outpoint = parts.next()?.to_owned();
            if parts.next().is_some() {
                return None;
            }
            Some(WatchOwner::Outpoint { name, outpoint })
        }
        _ => None,
    }
}

pub fn validate_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "descriptor name must not be empty");
    anyhow::ensure!(name.len() <= 64, "descriptor name must be at most 64 bytes");
    anyhow::ensure!(
        name.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-')),
        "descriptor name may only contain ASCII letters, digits, '.', '_' and '-'"
    );
    Ok(())
}

pub fn validate_lookahead(lookahead: u32) -> anyhow::Result<()> {
    anyhow::ensure!(lookahead > 0, "lookahead must be greater than zero");
    anyhow::ensure!(
        lookahead <= MAX_LOOKAHEAD,
        "lookahead exceeds maximum of {MAX_LOOKAHEAD}"
    );
    Ok(())
}

pub fn validate_confirmations(confirmations: u32) -> anyhow::Result<()> {
    anyhow::ensure!(confirmations > 0, "confirmations must be greater than zero");
    anyhow::ensure!(
        confirmations <= MAX_CONFIRMATIONS,
        "confirmations exceeds maximum of {MAX_CONFIRMATIONS}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owners_round_trip() {
        assert_eq!(
            parse_owner(&script_owner("treasury", 1, 42)),
            Some(WatchOwner::Script {
                name: "treasury".to_owned(),
                branch: 1,
                index: 42,
            })
        );
        let outpoint = "00aabb:3";
        assert_eq!(
            parse_owner(&outpoint_owner("treasury", outpoint)),
            Some(WatchOwner::Outpoint {
                name: "treasury".to_owned(),
                outpoint: outpoint.to_owned(),
            })
        );
    }

    #[test]
    fn names_are_safe_for_owner_paths_and_datastore_keys() {
        assert!(validate_name("cold-wallet_2.receive").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("bad/name").is_err());
        assert!(validate_name("spaces are bad").is_err());
    }

    #[test]
    fn legacy_records_default_to_active() {
        let record: DescriptorRecord = serde_json::from_value(serde_json::json!({
            "name": "legacy",
            "descriptor": "raw(51)#8lvh9jxk",
            "birthheight": 1,
            "lookahead": 1,
            "range_end": 1
        }))
        .unwrap();
        assert_eq!(record.status, DescriptorStatus::Active);
        assert_eq!(record.config.confirmations, DEFAULT_CONFIRMATIONS);
        assert!(record.pending_movements.is_empty());
    }
}
