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
  rm -rf "$test_root"
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

for _attempt in $(seq 1 60); do
  if lightning-cli --lightning-dir="$lightning_dir" --network=regtest getinfo >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

ln_cli getinfo >/dev/null
plugins="$(ln_cli plugin list)"
echo "$plugins" | jq -e '.plugins[] | select(.name | endswith("/cln-bwatch")) | .active == true' >/dev/null
echo "$plugins" | jq -e '.plugins[] | select(.name | endswith("/cln-tracker")) | .active == true' >/dev/null
ln_cli tracker-list | jq -e '.descriptors == []' >/dev/null
ln_cli tracker-health | jq -e '.healthy == true' >/dev/null

# Build a descriptor wallet and give it mature regtest funds.
btc_cli -named createwallet wallet_name=tracker-wallet descriptors=true >/dev/null
mining_address="$(wallet_cli getnewaddress)"
btc_cli generatetoaddress 101 "$mining_address" >/dev/null
wait_for_jq '.bwatch.caught_up == true and .healthy == true' \
  'bwatch did not catch up after initial mining' ln_cli tracker-health >/dev/null

descriptor="$(wallet_cli listdescriptors \
  | jq -er '.descriptors | map(select(.active == true and .internal == false)) | first | .desc')"
start_height=$(( $(btc_cli getblockcount) + 1 ))

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
registered="$(ln_cli tracker-register \
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

# Fund index zero and wait for Tracker plus Bookkeeper to process the block.
tracked_address="$(btc_cli deriveaddresses "$descriptor" '[0,0]' | jq -er '.[0]')"
deposit_txid="$(wallet_cli sendtoaddress "$tracked_address" 0.1)"
btc_cli generatetoaddress 1 "$mining_address" >/dev/null
treasury="$(wait_for_jq \
  '(.tracked_utxos | length) == 1 and .pending_movements == [] and .last_used_index == 0' \
  'tracker did not discover and deliver the deposit' \
  ln_cli tracker-inspect name=treasury)"
outpoint="$(jq -er '.tracked_utxos[0].outpoint' <<<"$treasury")"
expected_script_watches="$(jq -er '.range_end' <<<"$treasury")"
[[ "$outpoint" == "$deposit_txid:"* ]] || fail 'tracked outpoint has the wrong deposit txid'
assert_jq "$(ln_cli listwatch)" \
  '[.watches[].owners[] | select(startswith("plugin/tracker/treasury/outpoint/"))] | length == 1' \
  'deposit did not create an outpoint watch'
wait_for_jq \
  ".events | map(select(.account == \"treasury\" and .outpoint == \"$outpoint\" and .credit_msat != 0 and .credit_msat != \"0msat\")) | length == 1" \
  'Bookkeeper did not receive exactly one deposit' \
  ln_cli bkpr-listaccountevents >/dev/null

# Remove one expected watch behind Tracker's back, then prove startup
# reconciliation restores it without duplicating the Bookkeeper deposit.
watch_to_remove="$(ln_cli listwatch | jq -cer \
  '[.watches[] | select(any(.owners[]; startswith("plugin/tracker/treasury/spk/"))) | {scriptpubkey, owner: (.owners[] | select(startswith("plugin/tracker/treasury/spk/")))}][0]')"
ln_cli delscriptpubkeywatch \
  owner="$(jq -r '.owner' <<<"$watch_to_remove")" \
  scriptpubkey="$(jq -r '.scriptpubkey' <<<"$watch_to_remove")" >/dev/null
tracker_plugin="$(command -v cln-tracker)"
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
if ln_cli tracker-inspect name=treasury >/dev/null 2>&1; then
  fail 'tracker-inspect succeeded after unregistration'
fi

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

assert_jq "$(ln_cli tracker-health)" '.healthy == true and .descriptors.active == 0' \
  'tracker was unhealthy after lifecycle tests'

printf 'tracker lifecycle integration tests passed\n'
