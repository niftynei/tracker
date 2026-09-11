set -euo pipefail

test_root="$(mktemp -d "${TMPDIR:-/tmp}/tracker-integration.XXXXXXXX")"
bitcoin_dir="$test_root/bitcoin"
lightning_dir="$test_root/lightning"
rpc_port=$((21000 + $$ % 10000))
lightning_port=$((31000 + $$ % 10000))
bitcoin_pid=""
lightning_pid=""

fail() {
  printf 'integration failure: %s\n' "$*" >&2
  return 1
}

ln_cli() {
  lightning-cli --lightning-dir="$lightning_dir" --network=regtest "$@"
}

btc_cli() {
  bitcoin-cli \
    -datadir="$bitcoin_dir" \
    -rpcuser=tracker \
    -rpcpassword=tracker-integration \
    -rpcport="$rpc_port" \
    "$@"
}

wallet_cli() {
  btc_cli -rpcwallet=tracker-wallet "$@"
}

assert_jq() {
  value=$1
  filter=$2
  message=$3
  jq -e "$filter" >/dev/null <<<"$value" || fail "$message"
}

wait_for_jq() {
  filter=$1
  message=$2
  shift 2
  for _wait_attempt in $(seq 1 120); do
    if value=$("$@" 2>/dev/null) && jq -e "$filter" >/dev/null <<<"$value"; then
      printf '%s\n' "$value"
      return 0
    fi
    sleep 0.25
  done
  "$@" >&2 || true
  fail "$message"
}

replace_tracker_record() {
  record_name=$1
  jq_filter=$2
  key="$(jq -cn --arg name "$record_name" '["tracker", "descriptors", $name]')"
  response="$(ln_cli listdatastore key="$key")"
  entry="$(jq -c '.datastore[0] // null' <<<"$response")"
  if [[ "$entry" == null ]]; then
    fail "descriptor datastore record is missing (name=$record_name response=$response)"
  fi
  generation="$(jq -er '.generation' <<<"$entry")" \
    || fail "descriptor datastore record has no generation (entry=$entry)"
  record="$(jq -er '.string' <<<"$entry")" \
    || fail "descriptor datastore record has no JSON string (entry=$entry)"
  jq -e . >/dev/null <<<"$record" \
    || fail "descriptor datastore record is invalid JSON (record=$record)"
  updated="$(jq -cer "$jq_filter" <<<"$record")" \
    || fail "could not construct crash snapshot (filter=$jq_filter record=$record)"
  encoded_updated="$(jq -cn --arg value "$updated" '$value')"
  ln_cli datastore \
    key="$key" \
    string="$encoded_updated" \
    mode=must-replace \
    generation="$generation" >/dev/null
}

cleanup() {
  status=$?
  if [[ "$status" -ne 0 ]]; then
    tail -n 100 "$lightning_dir/lightning.log" 2>/dev/null || true
    tail -n 100 "$bitcoin_dir/regtest/debug.log" 2>/dev/null || true
  fi
  ln_cli stop >/dev/null 2>&1 || true
  bitcoin-cli \
    -datadir="$bitcoin_dir" \
    -rpcuser=tracker \
    -rpcpassword=tracker-integration \
    -rpcport="$rpc_port" \
    stop >/dev/null 2>&1 || true
  if [[ -n "$lightning_pid" ]]; then
    kill "$lightning_pid" 2>/dev/null || true
    wait "$lightning_pid" 2>/dev/null || true
  fi
  if [[ -n "$bitcoin_pid" ]]; then
    kill "$bitcoin_pid" 2>/dev/null || true
    wait "$bitcoin_pid" 2>/dev/null || true
  fi
  if [[ "${KEEP_TEST_ROOT:-0}" == 1 ]]; then
    printf 'preserving integration test data at %s\n' "$test_root" >&2
  else
    rm -rf "$test_root"
  fi
}
trap cleanup EXIT INT TERM

mkdir -p "$bitcoin_dir" "$lightning_dir"

bitcoind \
  -datadir="$bitcoin_dir" \
  -regtest \
  -server \
  -listen=0 \
  -fallbackfee=0.0002 \
  -rpcuser=tracker \
  -rpcpassword=tracker-integration \
  -rpcport="$rpc_port" >/dev/null 2>&1 &
bitcoin_pid=$!

bitcoin-cli \
  -datadir="$bitcoin_dir" \
  -rpcwait \
  -rpcuser=tracker \
  -rpcpassword=tracker-integration \
  -rpcport="$rpc_port" \
  getblockchaininfo >/dev/null

# Give regtest a mature chain before lightningd starts.  Otherwise newer
# Bitcoin Core/CLN combinations can leave lightningd waiting for initial block
# download while the test waits for lightning-cli to become available.
btc_cli -named createwallet wallet_name=tracker-wallet descriptors=true >/dev/null
mining_address="$(wallet_cli getnewaddress)"
btc_cli generatetoaddress 101 "$mining_address" >/dev/null

lightningd \
  --lightning-dir="$lightning_dir" \
  --network=regtest \
  --addr="127.0.0.1:$lightning_port" \
  --bitcoin-datadir="$bitcoin_dir" \
  --bitcoin-rpcuser=tracker \
  --bitcoin-rpcpassword=tracker-integration \
  --bitcoin-rpcport="$rpc_port" \
  --experimental-bwatch \
  --bwatch-poll-interval=100 \
  --plugin="$(command -v cln-tracker)" \
  --log-level=debug \
  --log-file="$lightning_dir/lightning.log" >/dev/null 2>&1 &
lightning_pid=$!

for _attempt in $(seq 1 120); do
  if lightning-cli --lightning-dir="$lightning_dir" --network=regtest getinfo >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$lightning_pid" 2>/dev/null; then
    fail "lightningd exited during startup (lightningd=$(command -v lightningd), tracker=$(command -v cln-tracker))"
  fi
  sleep 0.25
done

ln_cli getinfo >/dev/null \
  || fail "lightningd RPC did not become ready (lightningd=$(command -v lightningd), tracker=$(command -v cln-tracker))"
plugins="$(ln_cli plugin list)"
assert_jq "$plugins" \
  '.plugins[] | select(.name | endswith("/cln-bwatch")) | .active == true' \
  'bwatch plugin is not active'
assert_jq "$plugins" \
  '.plugins[] | select(.name | endswith("/cln-tracker")) | .active == true' \
  'tracker plugin is not active'
assert_jq "$(ln_cli tracker-list)" '.descriptors == []' \
  'tracker did not start with an empty descriptor set'

# Build a descriptor from the already-mature regtest wallet.
wait_for_jq '.bwatch.caught_up == true and .healthy == true' \
  'bwatch did not catch up after initial mining' ln_cli tracker-health >/dev/null

descriptor="$(wallet_cli listdescriptors \
  | jq -er '.descriptors | map(select(.active == true and .internal == false)) | first | .desc')"
start_height=$(( $(btc_cli getblockcount) + 1 ))

# A wide descriptor registration must scan its historical range once even when
# an address beyond the initial lookahead received funds and that output was
# later spent.  This exercises transient frontier discovery and dynamic
# outpoint following, rather than the old empty-wallet fast path.
btc_cli -named createwallet wallet_name=scan-wallet descriptors=true >/dev/null
for _scan_address_index in $(seq 0 51); do
  scan_address="$(btc_cli -rpcwallet=scan-wallet getnewaddress)"
done
scan_descriptor="$(btc_cli -rpcwallet=scan-wallet listdescriptors \
  | jq -er '.descriptors | map(select(.active == true and .internal == false and (.desc | startswith("wpkh(")))) | first | .desc')"
scan_deposit_txid="$(wallet_cli sendtoaddress "$scan_address" 0.01)"
scan_deposit_hash="$(btc_cli generatetoaddress 1 "$mining_address" | jq -er '.[0]')"
scan_birth="$(btc_cli getblockheader "$scan_deposit_hash" | jq -er '.height')"
scan_deposit_vout="$(wallet_cli gettransaction "$scan_deposit_txid" \
  | jq -er --arg address "$scan_address" '.details[] | select(.category == "send" and .address == $address) | .vout')"
scan_destination="$(wallet_cli getnewaddress)"
scan_raw_spend="$(btc_cli createrawtransaction \
  "[{\"txid\":\"$scan_deposit_txid\",\"vout\":$scan_deposit_vout}]" \
  "[{\"$scan_destination\":0.009}]")"
scan_signed_spend="$(btc_cli -rpcwallet=scan-wallet signrawtransactionwithwallet "$scan_raw_spend" | jq -er '.hex')"
scan_spend_txid="$(btc_cli sendrawtransaction "$scan_signed_spend")"
btc_cli generatetoaddress 1 "$mining_address" >/dev/null
btc_cli generatetoaddress 5 "$mining_address" >/dev/null
scan_chain_tip="$(btc_cli getblockcount)"
wait_for_jq ".bwatch.caught_up == true and .bwatch.current_height == $scan_chain_tip" \
  'bwatch did not catch up before wallet scan' ln_cli tracker-health >/dev/null
scan_before="$(ln_cli bwatch-status)"
scan_tip="$(jq -er '.current_height' <<<"$scan_before")"
if ln_cli rescanwatchset \
  owner_prefix=plugin/ \
  start_block="$scan_birth" >/dev/null 2>&1; then
  fail "bwatch accepted the dangerously broad plugin/ rescan prefix"
fi
scan_registration_output="$(ln_cli --raw tracker-register \
  name=wide-scan \
  descriptor="$scan_descriptor" \
  birthheight="$scan_birth" \
  lookahead=64 \
  confirmations=1)"
if ! grep -Eq $'# [[:space:]]*[0-9]+/[0-9]+ .*\|=+\|' <<<"$scan_registration_output"; then
  fail "tracker-register did not stream block scan progress (output=$scan_registration_output)"
fi
scan_registered="$(ln_cli -N none tracker-inspect name=wide-scan)"
assert_jq "$scan_registered" \
  ".name == \"wide-scan\" and .lookahead == 64 and .range_end == 116 and .last_used_index == 51 and .status == \"active\" and (.tracked_utxos | length) == 1 and .tracked_utxos[0].spent_by == \"$scan_spend_txid\"" \
  "single-pass descriptor registration missed the historical deposit or spend (inspect=$scan_registered)"
assert_jq "$(ln_cli bkpr-listaccountevents account=external)" \
  ".events | map(select(.origin == \"wide-scan\" and (.outpoint | startswith(\"$scan_spend_txid:\")) and .description == \"$scan_destination\")) | length == 1" \
  'historical registration did not reconstruct the external recipient output'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/wide-scan/spk/"))] | length == 116' \
  'descriptor registration did not persist the extended lookahead frontier'
scan_after="$(ln_cli bwatch-status)"
scan_completed_before="$(jq -er '.rescans_completed_total' <<<"$scan_before")"
scan_blocks_before="$(jq -er '.rescan_blocks_processed_total' <<<"$scan_before")"
if ! jq -e \
  ".rescans_completed_total == ($scan_completed_before + 1) and .rescan_blocks_processed_total == ($scan_blocks_before + $scan_tip - $scan_birth + 1)" \
  >/dev/null <<<"$scan_after"; then
  fail "64-address descriptor registration did not use exactly one historical block pass (birth=$scan_birth tip=$scan_tip before=$scan_before after=$scan_after)"
fi

# Put a deposit beyond the installed frontier, spend it, and recover both with
# one explicit Tracker rescan. The supplied lookahead becomes the durable gap.
for _rescan_address_index in $(seq 52 119); do
  rescan_address="$(btc_cli -rpcwallet=scan-wallet getnewaddress)"
done
rescan_deposit_txid="$(wallet_cli sendtoaddress "$rescan_address" 0.01)"
rescan_deposit_hash="$(btc_cli generatetoaddress 1 "$mining_address" | jq -er '.[0]')"
rescan_start="$(btc_cli getblockheader "$rescan_deposit_hash" | jq -er '.height')"
rescan_deposit_vout="$(wallet_cli gettransaction "$rescan_deposit_txid" \
  | jq -er --arg address "$rescan_address" '.details[] | select(.category == "send" and .address == $address) | .vout')"
rescan_destination="$(wallet_cli getnewaddress)"
rescan_raw_spend="$(btc_cli createrawtransaction \
  "[{\"txid\":\"$rescan_deposit_txid\",\"vout\":$rescan_deposit_vout}]" \
  "[{\"$rescan_destination\":0.009}]")"
rescan_signed_spend="$(btc_cli -rpcwallet=scan-wallet signrawtransactionwithwallet "$rescan_raw_spend" | jq -er '.hex')"
rescan_spend_txid="$(btc_cli sendrawtransaction "$rescan_signed_spend")"
btc_cli generatetoaddress 1 "$mining_address" >/dev/null
btc_cli generatetoaddress 5 "$mining_address" >/dev/null
rescan_tip="$(btc_cli getblockcount)"
wait_for_jq ".bwatch.caught_up == true and .bwatch.current_height == $rescan_tip" \
  'bwatch did not catch up before explicit descriptor rescan' ln_cli tracker-health >/dev/null
rescan_before="$(ln_cli bwatch-status)"
rescan_output="$(ln_cli --raw tracker-rescan \
  name=wide-scan \
  start_block="$rescan_start" \
  lookahead=100)"
if ! grep -Eq $'# [[:space:]]*[0-9]+/[0-9]+ .*\|=+\|' <<<"$rescan_output"; then
  fail "tracker-rescan did not stream block scan progress (output=$rescan_output)"
fi
rescanned="$(ln_cli -N none tracker-inspect name=wide-scan)"
assert_jq "$rescanned" \
  ".name == \"wide-scan\" and .lookahead == 100 and .range_end == 220 and .last_used_index == 119 and .status == \"active\" and .pending_rescan == null and (.tracked_utxos | length) == 2 and ([.tracked_utxos[] | select(.spent_by == \"$rescan_spend_txid\")] | length) == 1" \
  'tracker-rescan missed the beyond-frontier deposit or its later spend'
assert_jq "$(ln_cli bkpr-listaccountevents account=external)" \
  ".events | map(select(.origin == \"wide-scan\" and (.outpoint | startswith(\"$rescan_spend_txid:\")) and .description == \"$rescan_destination\")) | length == 1" \
  'explicit rescan did not reconstruct the external recipient output'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/wide-scan/spk/"))] | length == 220' \
  'tracker-rescan did not persist the widened descriptor frontier'
rescan_after="$(ln_cli bwatch-status)"
rescan_completed_before="$(jq -er '.rescans_completed_total' <<<"$rescan_before")"
rescan_blocks_before="$(jq -er '.rescan_blocks_processed_total' <<<"$rescan_before")"
if ! jq -e \
  ".rescans_completed_total == ($rescan_completed_before + 1) and .rescan_blocks_processed_total == ($rescan_blocks_before + $rescan_tip - $rescan_start + 1)" \
  >/dev/null <<<"$rescan_after"; then
  fail "tracker-rescan did not use exactly one historical block pass (start=$rescan_start tip=$rescan_tip before=$rescan_before after=$rescan_after)"
fi
future_rescan="$(ln_cli -N none tracker-rescan \
  name=wide-scan \
  start_block=$((rescan_tip + 1)))"
assert_jq "$future_rescan" \
  ".lookahead == 100 and .rescan.start_block == ($rescan_tip + 1) and .rescan.target_block == $rescan_tip and .rescan.blocks_processed == 0 and .rescan.matches_found == 0" \
  'tracker-rescan did not return the expected structured zero-block result with omitted lookahead'
ln_cli tracker-unregister name=wide-scan >/dev/null

# Checksums are enforced at the RPC boundary.
descriptor_without_checksum="${descriptor%#*}"
if ln_cli tracker-register \
  name=invalid-checksum \
  descriptor="$descriptor_without_checksum" \
  birthheight="$start_height" \
  lookahead=3 >/dev/null 2>&1; then
  fail 'tracker accepted a descriptor without a checksum'
fi

# Register, inspect, and verify the initial bwatch ownership.
registered="$(ln_cli -N none tracker-register \
  name=treasury \
  descriptor="$descriptor" \
  birthheight="$start_height" \
  lookahead=3 \
  confirmations=1)"
assert_jq "$registered" \
  '.name == "treasury" and .lookahead == 3 and .range_end == 3 and .status == "active"' \
  'tracker-register returned an unexpected record'
[[ "$(jq -r '.descriptor' <<<"$registered")" == "$descriptor" ]] \
  || fail 'tracker-register returned the wrong descriptor'
[[ "$(jq -r '.birthheight' <<<"$registered")" == "$start_height" ]] \
  || fail 'tracker-register returned the wrong birthheight'

inspected="$(ln_cli tracker-inspect name=treasury)"
assert_jq "$inspected" '.name == "treasury" and .tracked_utxos == []' \
  'tracker-inspect did not return the registered empty descriptor'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/treasury/spk/"))] | length == 3' \
  'registration did not create three descriptor watches'

# The same scripts cannot be registered under a second account.
if ln_cli tracker-register \
  name=overlap \
  descriptor="$descriptor" \
  birthheight="$start_height" \
  lookahead=3 >/dev/null 2>&1; then
  fail 'tracker accepted an overlapping descriptor'
fi

# Shrinking and growing lookahead changes the real bwatch set.
assert_jq "$(ln_cli tracker-update name=treasury lookahead=2)" \
  '.lookahead == 2 and .range_end == 2 and .status == "active"' \
  'lookahead shrink did not update tracker state'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/treasury/spk/"))] | length == 2' \
  'lookahead shrink did not remove descriptor watches'
assert_jq "$(ln_cli tracker-update name=treasury lookahead=5)" \
  '.lookahead == 5 and .range_end == 5 and .status == "active"' \
  'lookahead growth did not update tracker state'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/treasury/spk/"))] | length == 5' \
  'lookahead growth did not add descriptor watches'

# Allocate index zero with durable address metadata. The issued index advances
# the watched frontier before the address is returned, and its annotation must
# become the Bookkeeper description when that address receives a deposit.
issued="$(ln_cli tracker-newaddr \
  name=treasury \
  branch=0 \
  annotation="Quarterly reserve")"
assert_jq "$issued" \
  '.name == "treasury" and .branch == 0 and .index == 0 and .annotation == "Quarterly reserve" and .next_index == 1 and .range_end == 6' \
  'tracker-newaddr did not allocate and advance the descriptor frontier'
tracked_address="$(jq -er '.address' <<<"$issued")"
issued_inspect="$(ln_cli tracker-inspect name=treasury)"
assert_jq "$issued_inspect" \
  '.next_indexes["0"] == 1 and .range_ends["0"] == 6' \
  'tracker-inspect did not retain the issued address cursor and frontier'
issued_addresses="$(ln_cli tracker-listaddresses name=treasury branch=0 start=0 limit=10)"
assert_jq "$issued_addresses" \
  '.addresses[0].annotation == "Quarterly reserve" and .addresses[0].index == 0 and .next_start == null' \
  'tracker-listaddresses did not return issued address metadata'
[[ "$(jq -er '.addresses[0].address' <<<"$issued_addresses")" == "$tracked_address" ]] \
  || fail 'tracker-listaddresses returned the wrong issued address'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/treasury/spk/"))] | length == 6' \
  'tracker-newaddr returned before installing its extended watch frontier'

# Fund the issued address and wait for Tracker plus Bookkeeper to process it.
deposit_txid="$(wallet_cli sendtoaddress "$tracked_address" 0.1)"
btc_cli generatetoaddress 1 "$mining_address" >/dev/null
treasury="$(wait_for_jq \
  '(.tracked_utxos | length) == 1 and .tracked_utxos[0].annotation == "Quarterly reserve" and .pending_movements == [] and .last_used_index == 0' \
  'tracker did not discover and deliver the deposit' \
  ln_cli tracker-inspect name=treasury)"
outpoint="$(jq -er '.tracked_utxos[0].outpoint' <<<"$treasury")"
expected_script_watches="$(jq -er '.range_end' <<<"$treasury")"
[[ "$outpoint" == "$deposit_txid:"* ]] || fail 'tracked outpoint has the wrong deposit txid'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/treasury/outpoint/"))] | length == 1' \
  'deposit did not create an outpoint watch'
wait_for_jq \
  ".events | map(select(.account == \"treasury\" and .outpoint == \"$outpoint\" and .credit_msat != 0 and .credit_msat != \"0msat\" and .description == \"Quarterly reserve\")) | length == 1" \
  'Bookkeeper did not receive exactly one annotated deposit' \
  ln_cli bkpr-listaccountevents >/dev/null

# Description reconciliation preserves a manual Bookkeeper edit unless the
# caller explicitly elects to make Tracker's annotation authoritative.
ln_cli bkpr-editdescriptionbyoutpoint \
  outpoint="$outpoint" \
  description="Manual accounting override" >/dev/null
description_conflict="$(ln_cli tracker-sync-descriptions name=treasury)"
assert_jq "$description_conflict" \
  '.synced == [] and (.conflicts | length) == 1 and .conflicts[0].tracker_annotation == "Quarterly reserve" and .conflicts[0].bookkeeper_description == "Manual accounting override"' \
  'description sync did not preserve and report a Bookkeeper conflict'
assert_jq "$(ln_cli bkpr-listaccountevents account=treasury)" \
  "[.events[] | select(.outpoint == \"$outpoint\" and .credit_msat != 0 and .credit_msat != \"0msat\")][0].description == \"Manual accounting override\"" \
  'non-overwriting description sync changed a manual Bookkeeper edit'
description_overwrite="$(ln_cli tracker-sync-descriptions name=treasury overwrite=true)"
assert_jq "$description_overwrite" \
  ".conflicts == [] and .synced == [\"$outpoint\"]" \
  'overwriting description sync did not restore Tracker metadata'
wait_for_jq \
  ".events | map(select(.account == \"treasury\" and .outpoint == \"$outpoint\" and .credit_msat != 0 and .credit_msat != \"0msat\" and .description == \"Quarterly reserve\")) | length == 1" \
  'overwriting description sync did not update Bookkeeper' \
  ln_cli bkpr-listaccountevents >/dev/null

# Real concurrent allocations are serialized per descriptor and must never
# return the same index.
concurrent_one="$test_root/concurrent-address-one.json"
concurrent_two="$test_root/concurrent-address-two.json"
ln_cli tracker-newaddr name=treasury branch=0 annotation="Concurrent one" >"$concurrent_one" &
concurrent_one_pid=$!
ln_cli tracker-newaddr name=treasury branch=0 annotation="Concurrent two" >"$concurrent_two" &
concurrent_two_pid=$!
wait "$concurrent_one_pid"
wait "$concurrent_two_pid"
concurrent_indexes="$(jq -cs '[.[].index] | sort' "$concurrent_one" "$concurrent_two")"
[[ "$concurrent_indexes" == '[1,2]' ]] \
  || fail "concurrent tracker-newaddr calls reused or skipped an index (indexes=$concurrent_indexes)"

# Reproduce a crash immediately after the independent address record is
# durable but before the descriptor cursor and watches are updated. Startup
# must recover the cursor from the address namespace, install the branch's
# missing watches, and never issue the stranded index again.
tracker_plugin="$(command -v cln-tracker)"
ln_cli plugin stop "$tracker_plugin" >/dev/null
crashed_index=7
crashed_address="$(btc_cli deriveaddresses "$descriptor" "[$crashed_index,$crashed_index]" | jq -er '.[0]')"
crashed_script="$(wallet_cli getaddressinfo "$crashed_address" | jq -er '.scriptPubKey')"
crashed_record="$(jq -cn \
  --arg name treasury \
  --arg address "$crashed_address" \
  --arg scriptpubkey "$crashed_script" \
  --arg annotation "Crash boundary" \
  --argjson index "$crashed_index" \
  --argjson issued_at "$(date +%s)" \
  '{name: $name, branch: 0, index: $index, address: $address, scriptpubkey: $scriptpubkey, annotation: $annotation, issued_at: $issued_at}')"
crashed_key="$(jq -cn --arg index "$crashed_index" '["tracker", "addresses", "treasury", "0", $index]')"
crashed_record_hex="$(printf '%s' "$crashed_record" | od -An -v -tx1 | tr -d ' \n')"
ln_cli datastore \
  key="$crashed_key" \
  hex="$crashed_record_hex" \
  mode=must-create >/dev/null
assert_jq "$(ln_cli listdatastore key="$crashed_key")" \
  '.datastore[0].string | fromjson | .index == 7 and .annotation == "Crash boundary"' \
  'could not persist the simulated address-allocation crash record'
ln_cli plugin start "$tracker_plugin" >/dev/null
wait_for_jq \
  '.status == "active" and .next_indexes["0"] == 8 and .range_ends["0"] == 13' \
  'startup did not recover the address allocation crash boundary' \
  ln_cli tracker-inspect name=treasury >/dev/null
assert_jq "$(ln_cli tracker-listaddresses name=treasury branch=0 start=0 limit=10)" \
  '[.addresses[].index] == [0,1,2,7]' \
  'address listing did not include the independently persisted crash record'
post_crash_address="$(ln_cli tracker-newaddr name=treasury branch=0 annotation="After crash")"
assert_jq "$post_crash_address" \
  '.index == 8 and .next_index == 9 and .range_ends["0"] == 14' \
  'address allocation reused an index after crash recovery'
expected_script_watches=14
assert_jq "$(ln_cli listwatch)" \
  "[.watches[].owners[] | select(startswith(\"plugin/tracker/treasury/spk/0/\"))] | length == $expected_script_watches" \
  'crash recovery did not install the recovered branch frontier'

# Remove one expected watch behind Tracker's back, then prove startup
# reconciliation restores it without duplicating the Bookkeeper deposit.
watch_to_remove="$(ln_cli listwatch | jq -cer \
  '[.watches[] | select(any(.owners[]; startswith("plugin/tracker/treasury/spk/"))) | {scriptpubkey, owner: (.owners[] | select(startswith("plugin/tracker/treasury/spk/")))}][0]')"
ln_cli delscriptpubkeywatch \
  owner="$(jq -r '.owner' <<<"$watch_to_remove")" \
  scriptpubkey="$(jq -r '.scriptpubkey' <<<"$watch_to_remove")" >/dev/null
ln_cli plugin stop "$tracker_plugin" >/dev/null
ln_cli plugin start "$tracker_plugin" >/dev/null
wait_for_jq '.status == "active" and (.tracked_utxos | length == 1)' \
  'tracker did not restore its descriptor record after restart' \
  ln_cli tracker-inspect name=treasury >/dev/null
wait_for_jq \
  "[.watches[].owners[] | select(startswith(\"plugin/tracker/treasury/spk/\"))] | length == $expected_script_watches" \
  'startup reconciliation did not restore the missing watch' \
  ln_cli listwatch >/dev/null
wait_for_jq \
  ".events | map(select(.account == \"treasury\" and .outpoint == \"$outpoint\" and .credit_msat != 0 and .credit_msat != \"0msat\")) | length == 1" \
  'restart duplicated or lost the Bookkeeper deposit' \
  ln_cli bkpr-listaccountevents >/dev/null

# Spend exactly the discovered outpoint, rather than allowing wallet coin
# selection to choose an unrelated coinbase output.
outpoint_txid="${outpoint%:*}"
outpoint_vout="${outpoint##*:}"
spend_address="$(wallet_cli getnewaddress)"
raw_spend="$(btc_cli createrawtransaction \
  "[{\"txid\":\"$outpoint_txid\",\"vout\":$outpoint_vout}]" \
  "[{\"$spend_address\":0.099}]")"
signed_spend="$(wallet_cli signrawtransactionwithwallet "$raw_spend" | jq -er '.hex')"
spend_txid="$(btc_cli sendrawtransaction "$signed_spend")"
btc_cli generatetoaddress 1 "$mining_address" >/dev/null
wait_for_jq \
  ".tracked_utxos[0].spent_by == \"$spend_txid\" and .pending_movements == []" \
  'tracker did not discover and deliver the spend' \
  ln_cli tracker-inspect name=treasury >/dev/null
wait_for_jq \
  ".events | map(select(.account == \"treasury\" and .outpoint == \"$outpoint\" and .debit_msat != 0 and .debit_msat != \"0msat\")) | length == 1" \
  'Bookkeeper did not receive exactly one spend' \
  ln_cli bkpr-listaccountevents >/dev/null
wait_for_jq \
  ".events | map(select(.account == \"external\" and .origin == \"treasury\" and (.outpoint | startswith(\"$spend_txid:\")) and .description == \"$spend_address\" and .credit_msat != 0 and .credit_msat != \"0msat\")) | length == 1" \
  'Bookkeeper did not receive the external recipient output' \
  ln_cli bkpr-listaccountevents >/dev/null
assert_jq "$(ln_cli bkpr-listincome consolidate_fees=true)" \
  "def msat: if type == \"number\" then . elif type == \"string\" then sub(\"msat$\"; \"\") | tonumber else .msat end; [.income_events[] | select(.account == \"treasury\" and .tag == \"onchain_fee\" and .txid == \"$spend_txid\") | .debit_msat | msat] == [100000000]" \
  'Bookkeeper did not consolidate the Tracker spend fee after receiving its external output'
external_outpoint="$(ln_cli bkpr-listaccountevents account=external \
  | jq -er ".events[] | select(.origin == \"treasury\" and (.outpoint | startswith(\"$spend_txid:\"))) | .outpoint")"
ln_cli bkpr-editdescriptionbyoutpoint \
  outpoint="$external_outpoint" \
  description='' >/dev/null
ln_cli -N none tracker-rescan name=treasury start_block="$start_height" >/dev/null
assert_jq "$(ln_cli bkpr-listaccountevents account=external)" \
  ".events | map(select(.origin == \"treasury\" and .outpoint == \"$external_outpoint\" and .description == \"$spend_address\")) | length == 1" \
  'replaying a known spend did not repair its address or duplicated its external recipient output'

# Unregister removes every owned watch and the persisted Tracker view, while
# Bookkeeper history remains intact.
assert_jq "$(ln_cli tracker-unregister name=treasury)" \
  '.name == "treasury" and .removed == true' \
  'tracker-unregister did not report removal'
assert_jq "$(ln_cli tracker-list)" '.descriptors == []' \
  'unregistered descriptor remained in tracker-list'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/treasury/"))] | length == 0' \
  'unregistration left tracker-owned bwatch entries'
treasury_address_key='["tracker","addresses","treasury"]'
assert_jq "$(ln_cli listdatastore key="$treasury_address_key")" \
  '.datastore == []' \
  'unregistration left issued address metadata'
if ln_cli tracker-inspect name=treasury >/dev/null 2>&1; then
  fail 'tracker-inspect succeeded after unregistration'
fi

# Multipath descriptors maintain independent branch frontiers: issuing a far
# receive address must not expand the change branch to the same index.
descriptor_body="${descriptor%#*}"
multipath_body="$(jq -nr --arg descriptor "$descriptor_body" \
  '$descriptor | sub("/0/\\*"; "/<0;1>/*")')"
[[ "$multipath_body" != "$descriptor_body" ]] \
  || fail 'integration wallet descriptor could not be converted to multipath form'
multipath_checksum="$(btc_cli getdescriptorinfo "$multipath_body" | jq -er '.checksum')"
multipath_descriptor="${multipath_body}#${multipath_checksum}"
multipath_start=$(( $(btc_cli getblockcount) + 1 ))
ln_cli tracker-register \
  name=branch-frontiers \
  descriptor="$multipath_descriptor" \
  birthheight="$multipath_start" \
  lookahead=2 \
  confirmations=1 >/dev/null
branch_receive="$(ln_cli tracker-newaddr \
  name=branch-frontiers branch=0 minimum_index=10 annotation="Far receive")"
jq -e '.index == 10 and .range_ends["0"] == 13 and .range_ends["1"] == 2' \
  >/dev/null <<<"$branch_receive" \
  || fail "receive allocation incorrectly expanded every descriptor branch (response=$branch_receive)"
branch_change="$(ln_cli tracker-newaddr \
  name=branch-frontiers branch=1 annotation="First change")"
jq -e '.index == 0 and .range_ends["0"] == 13 and .range_ends["1"] == 3' \
  >/dev/null <<<"$branch_change" \
  || fail "change allocation did not maintain an independent branch frontier (response=$branch_change)"
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/branch-frontiers/spk/0/"))] | length == 13' \
  'receive branch installed the wrong number of watches'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/branch-frontiers/spk/1/"))] | length == 3' \
  'change branch installed the wrong number of watches'
ln_cli tracker-unregister name=branch-frontiers >/dev/null

# Reproduce the durable snapshots left when Tracker exits immediately after
# persisting registration, rescan, and deletion intent.  A restarted plugin
# must expose the interrupted scan identity, accept only the matching retry,
# clear that identity on success, and finish a pending deletion automatically.
crash_name=crash-recovery
crash_start=$(( $(btc_cli getblockcount) + 1 ))
ln_cli tracker-register \
  name="$crash_name" \
  descriptor="$descriptor" \
  birthheight="$crash_start" \
  lookahead=3 \
  confirmations=1 >/dev/null

ln_cli plugin stop "$tracker_plugin" >/dev/null
replace_tracker_record "$crash_name" \
  '.status = "syncing" | .initial_scan_complete = false | .scan_operation_id = "crashed-registration"'
ln_cli plugin start "$tracker_plugin" >/dev/null
crashed_registration="$(ln_cli tracker-inspect name="$crash_name")"
assert_jq "$crashed_registration" \
  '.status == "syncing" and .initial_scan_complete == false and .scan_operation_id == "crashed-registration"' \
  "startup did not preserve the interrupted registration identity (inspect=$crashed_registration)"
assert_jq "$(ln_cli -N none tracker-register \
  name="$crash_name" \
  descriptor="$descriptor" \
  birthheight="$crash_start" \
  lookahead=3 \
  confirmations=1)" \
  '.status == "active" and .scan_operation_id == null' \
  'registration retry did not replace and clear the crashed scan identity'

ln_cli plugin stop "$tracker_plugin" >/dev/null
replace_tracker_record "$crash_name" \
  ".status = \"syncing\" | .pending_rescan = {start_block: $crash_start} | .scan_operation_id = \"crashed-rescan\""
ln_cli plugin start "$tracker_plugin" >/dev/null
crashed_rescan="$(ln_cli tracker-inspect name="$crash_name")"
assert_jq "$crashed_rescan" \
  '.status == "syncing" and .pending_rescan != null and .scan_operation_id == "crashed-rescan"' \
  "startup did not preserve the interrupted rescan identity (inspect=$crashed_rescan)"
assert_jq "$(ln_cli -N none tracker-rescan \
  name="$crash_name" \
  start_block="$crash_start" \
  lookahead=3)" \
  '.status == "active" and .pending_rescan == null and .scan_operation_id == null' \
  'rescan retry did not replace and clear the crashed scan identity'

ln_cli plugin stop "$tracker_plugin" >/dev/null
replace_tracker_record "$crash_name" \
  '.status = "deleting" | .scan_operation_id = null'
ln_cli plugin start "$tracker_plugin" >/dev/null
wait_for_jq \
  "[.descriptors[] | select(.name == \"$crash_name\")] | length == 0" \
  'startup did not finish the deletion left by a crash' \
  ln_cli tracker-list >/dev/null
assert_jq "$(ln_cli listwatch)" \
  "[.watches[].owners[] | select(startswith(\"plugin/tracker/$crash_name/\"))] | length == 0" \
  'crash-recovered deletion left tracker-owned watches'

# Create wallet activity before registration and prove the birthheight rescan
# discovers it. A following block supplies the normal processed boundary used
# to deliver confirmation-mature historical movements.
historical_address="$(btc_cli deriveaddresses "$descriptor" '[2,2]' | jq -er '.[0]')"
historical_txid="$(wallet_cli sendtoaddress "$historical_address" 0.02)"
historical_blockhash="$(btc_cli generatetoaddress 1 "$mining_address" | jq -er '.[0]')"
historical_height="$(btc_cli getblockheader "$historical_blockhash" | jq -er '.height')"
ln_cli tracker-register \
  name=historical \
  descriptor="$descriptor" \
  birthheight="$historical_height" \
  lookahead=4 \
  confirmations=1 >/dev/null
btc_cli generatetoaddress 1 "$mining_address" >/dev/null
historical="$(wait_for_jq \
  '(.tracked_utxos | length) == 1 and .pending_movements == [] and .last_used_index == 2' \
  'historical rescan did not discover and deliver the old deposit' \
  ln_cli tracker-inspect name=historical)"
historical_outpoint="$(jq -er '.tracked_utxos[0].outpoint' <<<"$historical")"
[[ "$historical_outpoint" == "$historical_txid:"* ]] \
  || fail 'historical rescan found the wrong transaction'
wait_for_jq \
  ".events | map(select(.account == \"historical\" and .outpoint == \"$historical_outpoint\" and .credit_msat != 0 and .credit_msat != \"0msat\")) | length == 1" \
  'Bookkeeper did not receive the historical deposit' \
  ln_cli bkpr-listaccountevents >/dev/null
ln_cli tracker-unregister name=historical >/dev/null

# A deposit removed before its requested confirmation depth must be discarded
# without ever reaching Bookkeeper. generateblock explicitly controls block
# contents so the invalidated transaction cannot be mined again from mempool.
shallow_start=$(( $(btc_cli getblockcount) + 1 ))
ln_cli tracker-register \
  name=shallow-reorg \
  descriptor="$descriptor" \
  birthheight="$shallow_start" \
  lookahead=6 \
  confirmations=3 >/dev/null
shallow_address="$(btc_cli deriveaddresses "$descriptor" '[4,4]' | jq -er '.[0]')"
shallow_txid="$(wallet_cli sendtoaddress "$shallow_address" 0.03)"
shallow_block="$(btc_cli generateblock "$mining_address" "[\"$shallow_txid\"]" | jq -er '.hash')"
shallow="$(wait_for_jq \
  '(.tracked_utxos | length) == 1 and (.pending_movements | length) == 1' \
  'shallow-reorg deposit did not remain pending at one confirmation' \
  ln_cli tracker-inspect name=shallow-reorg)"
shallow_outpoint="$(jq -er '.tracked_utxos[0].outpoint' <<<"$shallow")"
[[ "$shallow_outpoint" == "$shallow_txid:"* ]] \
  || fail 'shallow-reorg tracker state contains the wrong transaction'
assert_jq "$(ln_cli tracker-update name=shallow-reorg confirmations=2)" \
  '.confirmations == 2 and (.pending_movements | length) == 1' \
  'confirmation update did not apply to the pending movement'
assert_jq "$(ln_cli bkpr-listaccountevents)" \
  '[.events[] | select(.account == "shallow-reorg")] | length == 0' \
  'an under-confirmed deposit reached Bookkeeper'
btc_cli invalidateblock "$shallow_block"
btc_cli generateblock "$mining_address" '[]' >/dev/null
btc_cli generateblock "$mining_address" '[]' >/dev/null
wait_for_jq \
  '.tracked_utxos == [] and .pending_movements == [] and .incident == null' \
  'shallow reorg did not discard the unbooked deposit cleanly' \
  ln_cli tracker-inspect name=shallow-reorg >/dev/null
assert_jq "$(ln_cli bkpr-listaccountevents)" \
  '[.events[] | select(.account == "shallow-reorg")] | length == 0' \
  'shallow reorg created Bookkeeper history'
ln_cli tracker-unregister name=shallow-reorg >/dev/null

# Once a deposit has reached Bookkeeper, Tracker cannot reverse it. A reorg
# must repair Tracker's UTXO view, retain the accounting event, and surface an
# operator-action-required incident.
deep_start=$(( $(btc_cli getblockcount) + 1 ))
ln_cli tracker-register \
  name=deep-reorg \
  descriptor="$descriptor" \
  birthheight="$deep_start" \
  lookahead=6 \
  confirmations=1 >/dev/null
deep_address="$(btc_cli deriveaddresses "$descriptor" '[5,5]' | jq -er '.[0]')"
deep_txid="$(wallet_cli sendtoaddress "$deep_address" 0.04)"
deep_block="$(btc_cli generateblock "$mining_address" "[\"$deep_txid\"]" | jq -er '.hash')"
deep="$(wait_for_jq \
  '(.tracked_utxos | length) == 1 and .pending_movements == []' \
  'deep-reorg deposit did not reach Tracker' \
  ln_cli tracker-inspect name=deep-reorg)"
deep_outpoint="$(jq -er '.tracked_utxos[0].outpoint' <<<"$deep")"
wait_for_jq \
  ".events | map(select(.account == \"deep-reorg\" and .outpoint == \"$deep_outpoint\" and .credit_msat != 0 and .credit_msat != \"0msat\")) | length == 1" \
  'deep-reorg deposit did not reach Bookkeeper' \
  ln_cli bkpr-listaccountevents >/dev/null
btc_cli invalidateblock "$deep_block"
btc_cli generateblock "$mining_address" '[]' >/dev/null
btc_cli generateblock "$mining_address" '[]' >/dev/null
wait_for_jq \
  '.tracked_utxos == [] and .pending_movements == [] and .incident.operator_action_required == true and .incident.code == "bookkeeper_reorg"' \
  'deep reorg did not create the required accounting incident' \
  ln_cli tracker-inspect name=deep-reorg >/dev/null
assert_jq "$(ln_cli bkpr-listaccountevents)" \
  "[.events[] | select(.account == \"deep-reorg\" and .outpoint == \"$deep_outpoint\")] | length == 1" \
  'deep reorg removed or duplicated immutable Bookkeeper history'
ln_cli tracker-ack-incident name=deep-reorg >/dev/null
assert_jq "$(ln_cli tracker-inspect name=deep-reorg)" '.incident == null' \
  'tracker-ack-incident did not clear the deep-reorg incident'
ln_cli tracker-unregister name=deep-reorg >/dev/null

wait_for_jq '.healthy == true and .descriptors.active == 0' \
  'tracker was unhealthy after lifecycle tests' \
  ln_cli tracker-health >/dev/null

printf 'tracker lifecycle integration tests passed\n'
