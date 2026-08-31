# tracker

tracker is a Rust plugin for Core Lightning that tracks checksummed Bitcoin
output descriptors through CLN's experimental bwatch plugin. It does not
connect to Bitcoin Core directly.

It derives descriptor scriptPubKeys through the configured lookahead, promotes
matched outputs into outpoint watches, persists configuration and discovered
UTXOs in CLN's datastore, restores watches after restart, and emits
Bookkeeper movements through CLN's injectutxodeposit and injectutxospend RPCs.

The plugin requires the accompanying CLN fork with `bwatch_match`,
`bwatch_block_processed`, and `bwatch_block_reverted` notifications. Start CLN
with `experimental-bwatch` enabled.

## Build and install

~~~console
nix build
~~~

The result is at result/bin/cln-tracker. A development shell is also included:

~~~console
nix develop
cargo test
cargo clippy --all-targets -- -D warnings
~~~

Add these lines to the CLN configuration:

~~~text
experimental-bwatch
plugin=/absolute/path/to/tracker/result/bin/cln-tracker
~~~

## RPC interface

Descriptor names are stable identifiers and become Bookkeeper account names.
They may contain ASCII letters, digits, periods, underscores, and hyphens.

birthheight is the first block height that may contain wallet activity. A
historical birthheight causes bwatch to rescan. Tracker registers a descriptor's
derived scripts without rescanning them individually, then asks bwatch to rescan
the descriptor's owner namespace as one wallet set. Bwatch therefore reads each
historical block once for the whole lookahead instead of once per script, while
unrelated CLN and plugin watches receive no replay notifications. lookahead defaults to 20 and is
capped at 100,000. It is a moving gap: when an index is used, tracker extends
every descriptor branch so that lookahead unused indexes remain watched.
confirmations defaults to 1 and is capped at 2,016. Tracker durably records a
matched deposit or spend immediately, then evaluates pending movements once per
`bwatch_block_processed` boundary. It does not inject a movement into Bookkeeper
until the configured confirmation depth is reached.

Every descriptor must include a valid eight-character checksum. Extended-key
network prefixes are checked against CLN's network. Public ranged, static, and
BIP 389 multipath descriptors are supported. Static descriptors require a
lookahead of 1. Registrations whose currently-derived scripts overlap another
descriptor are rejected to prevent double-accounting.

Register:

~~~console
lightning-cli tracker-register \
  name=treasury \
  descriptor='wpkh([fingerprint/84h/0h/0h]xpub.../0/*)#checksum' \
  birthheight=850000 \
  lookahead=100 \
  confirmations=6
~~~

For a historical registration, `lightning-cli` displays a request-scoped
progress bar using bwatch's completed and total block counts. Tracker polls the
in-process bwatch status once per second; it does not fetch blocks itself or
open another network service. Automated callers that require JSON-only output
can use `lightning-cli --notifications=none tracker-register ...` and inspect
the same live scan through `tracker-health` from another client.

Inspect:

~~~console
lightning-cli tracker-inspect name=treasury
~~~

The response contains name, descriptor, birthheight, lookahead, confirmations,
range_end, last_used_index, status, incident, last_success_at, branches,
tracked_utxos, and pending_movements.

Update the lookahead:

~~~console
lightning-cli tracker-update name=treasury lookahead=250 confirmations=6
~~~

The lookahead may increase or decrease. Decreasing it does not discard already
discovered UTXOs; their spend watches remain active. A confirmation change
applies to pending and future movements. Increasing it cannot retract movements
already accepted by Bookkeeper.

Rescan a registered descriptor in one historical block pass:

~~~console
lightning-cli tracker-rescan \
  name=treasury \
  start_block=875000 \
  lookahead=500
~~~

Both `start_block` and `lookahead` are optional. The start defaults to the
descriptor's immutable birthheight and the lookahead defaults to its current
gap limit. When supplied, lookahead becomes the descriptor's persistent gap
limit. `start_block` cannot precede the birthheight; unregister and register a
replacement record to move that boundary earlier.

The rescan uses a transient wallet set and dynamically follows outputs found
during the scan, so a deposit and its later spend are discovered in the same
block pass. The operation persists its intent before scanning, streams
request-scoped block progress, and can be resumed with the same
`tracker-rescan` arguments after an interruption. Its result includes the scan
range, processed block count, and deposit/spend match counts.

Reconcile expected watches and retry every mature, pending Bookkeeper movement:

~~~console
lightning-cli tracker-reconcile name=treasury
~~~

Omit name to reconcile every descriptor. The RPC returns an independent result
for each descriptor so one unavailable historical range does not hide the
others' status.

List all registrations:

~~~console
lightning-cli tracker-list
~~~

Operational health, including bwatch scan progress and persisted incidents:

~~~console
lightning-cli tracker-health
lightning-cli bwatch-status \
  | jq '.active_rescans[] | {watch_type, owners, current_height, target_height, blocks_processed, blocks_total, progress_percent}'
~~~

`bwatch-status.active_rescans` is empty when no historical scan is running.
Each entry is removed on success or failure; `last_rescan_error` retains the
most recent incomplete scan. `rescans_completed_total` and
`rescan_blocks_processed_total` are process-lifetime counters for completed
passes and successfully completed block work. Tracker exports aggregate active-scan count,
processed/total blocks, and completion ratio to Prometheus without using watch
owners or scripts as metric labels.

The same primitive can rescan CLN's onchain wallet without replaying plugin or
channel watch owners:

~~~console
lightning-cli rescanwatchset owner_prefix=wallet/ start_block=850000
~~~

`owner_prefix` must end in `/`. Callers that do not own a namespace can instead
pass an exact `owners` array. The broad `plugin/` prefix is rejected; select a
specific descendant such as `plugin/tracker/` or
`plugin/tracker/treasury/`. The rescan snapshots only matching owners across
script, outpoint, SCID, and block-depth watches.

Bounded, versioned metrics for a compatible Prometheus exporter:

~~~console
lightning-cli tracker-metrics
~~~

`tracker-metrics` follows the plugin metrics contract: version 1, namespace
`tracker`, and gauge/counter families containing numeric samples and bounded
labels. The exporter discovers it through CLN's `help` RPC and publishes the
families with a `cln_tracker_` prefix. `tracker-health` remains the richer
human/operator diagnostic interface. Active rescans report live block progress,
descriptor-output matches, spending-input matches, and unique outpoints being
followed.

After manually resolving an incident such as Bookkeeper reconciliation after a
reorg, acknowledge it with:

~~~console
lightning-cli tracker-ack-incident name=treasury
~~~

Unregister:

~~~console
lightning-cli tracker-unregister name=treasury
~~~

Unregister removes descriptor watches, discovered outpoint watches, and the
persisted tracker record. It does not remove movements already recorded by
Bookkeeper.

## Bookkeeper and reorganizations

CLN deduplicates injected movements, so replaying pending delivery after a
restart is safe. Tracker persists the observation before adding its confirmation
watch or calling Bookkeeper. A crash at any later point is resumed by startup
reconciliation or `tracker-reconcile`.

If a reorganization removes a movement before its configured confirmation
depth, Tracker discards it without notifying Bookkeeper. Bookkeeper currently
has no movement-reversal API. If a deeper reorganization removes an already
booked movement, Tracker repairs its own UTXO state and creates an
operator-action-required incident for manual accounting reconciliation.

Registration, lookahead changes, automatic frontier growth, and deletion first
persist their intended lifecycle state. On startup tracker reconciles its
expected watches with bwatch, resumes interrupted syncing/deletion, and removes
orphaned tracker-owned watches. Datastore generation numbers detect unexpected
concurrent changes. The descriptor and birthheight are immutable; replace
either by unregistering and registering a new named record.

Descriptor operations are serialized independently. Slow historical scans and
Bookkeeper calls do not hold Tracker's global state mutex, so unrelated
descriptors and inspection RPCs remain responsive. Failure, Bookkeeper, and
reorg counters are process-lifetime Prometheus counters; durable incidents and
unfinished movements remain persisted in CLN's datastore.

## End-to-end tests

The Rust unit suite covers descriptor validation, migration defaults, owner
encoding, range growth, and confirmation maturity. Tracker's flake pins commit
`c902a7d4a11204fd78b754339f091a2c67f7715f` from the
[`bwatch-plugin-block-events`](https://github.com/niftynei/lightning/tree/bwatch-plugin-block-events)
CLN branch. Its integration package deliberately builds only the CLN programs
and plugins needed for regtest, avoiding unrelated manual and Rust-plugin build
failures on macOS.

Run the self-contained lifecycle suite. It starts temporary Bitcoin Core and
CLN regtest daemons, loads bwatch and Tracker, then verifies checksum rejection,
registration and inspection, overlap rejection, lookahead changes, actual
descriptor deposits and spends, Bookkeeper delivery and deduplication, startup
watch reconciliation, unregistration cleanup, historical rescans, and shallow
and already-booked deep reorg handling. It shuts everything down afterward:

~~~console
nix run .#integration-test
~~~

For interactive or larger integration work, enter the shell containing the
same patched CLN build, Bitcoin Core, Tracker, and Rust toolchain:

~~~console
nix develop .#integration
echo "$CLN_BWATCH_COMMIT"
lightningd --version
tracker-integration-test
~~~

## Prometheus and alerting

Tracker does not open a network listener. Its deliberately bounded monitoring
state is available from `tracker-metrics`; the accompanying Prometheus exporter
discovers and collects that RPC automatically. Detailed operational state is
available separately from `tracker-health`.

CLNREST already exposes every RPC method, so Tracker health is also available
on CLNREST's existing listener:

~~~console
curl -k -X POST https://127.0.0.1:3010/v1/tracker-health \
  -H 'Rune: <read-only-rune>' \
  -H 'Content-Type: application/json' \
  -d '{}'
~~~

`prometheus-alerts.yml` contains a ready-to-load rule group for plugin
availability, Tracker health, descriptor incidents, bwatch lag, stalled
historical rescans, and Bookkeeper injection failures.

Useful alerts include `cln_tracker_healthy == 0`,
`cln_tracker_bwatch_lag_blocks > 2`, and any `cln_tracker_incident == 1`. Descriptor
names are bounded labels; descriptors, outpoints, transaction IDs, and error
messages should never be used as metric labels.

Tracker logs incident transitions at warning/error level. CLN forwards these
through its built-in `warning` notification, and CLNREST broadcasts all CLN
notifications over its authenticated Socket.IO connection. CLN also emits
`plugin_stopped` if Tracker exits. The CLN gRPC plugin exposes the warning
subscription for alert delivery. For fail-closed accounting, configure Tracker with
`important-plugin` instead of `plugin`; CLN will then exit if Tracker exits.
