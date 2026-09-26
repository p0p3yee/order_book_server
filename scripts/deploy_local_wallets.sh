#!/usr/bin/env bash
# Run manually on the Linux node host. Builds before replacing only the WS container.
set -euo pipefail
revision=${1:?Usage: WS_WALLETS=0xADDRESS[,0xADDRESS] bash deploy_local_wallets.sh FULL_COMMIT_SHA}
[[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo 'Expected full lowercase commit SHA' >&2; exit 1; }
: "${WS_WALLETS:?Set WS_WALLETS to the comma-separated trading wallet addresses}"
: "${HL_DATA_DIR:?Set HL_DATA_DIR to the host node output directory}"
: "${HL_NODE_SNAPSHOT_PATH:?Set HL_NODE_SNAPSHOT_PATH to the snapshot file path inside hl-node}"
HL_NODE_OUTPUT_MODE=${HL_NODE_OUTPUT_MODE:-batch}
[[ "$HL_NODE_OUTPUT_MODE" = batch || "$HL_NODE_OUTPUT_MODE" = stream ]] || {
  echo 'HL_NODE_OUTPUT_MODE must be batch or stream' >&2
  exit 1
}
[[ "$HL_DATA_DIR" = /* && -d "$HL_DATA_DIR" ]] || { echo 'HL_DATA_DIR must be an existing absolute directory' >&2; exit 1; }
[[ "$HL_NODE_SNAPSHOT_PATH" = /* ]] || { echo 'HL_NODE_SNAPSHOT_PATH must be absolute' >&2; exit 1; }
IFS=',' read -r -a selected_wallets <<< "$WS_WALLETS"
(( ${#selected_wallets[@]} <= 16 )) || { echo 'At most 16 wallets' >&2; exit 1; }
for wallet in "${selected_wallets[@]}"; do
  [[ "$wallet" =~ ^0x[0-9a-fA-F]{40}$ ]] || { echo 'Invalid wallet address' >&2; exit 1; }
done
build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT
curl --fail --show-error --location \
  "https://raw.githubusercontent.com/p0p3yee/order_book_server/$revision/Dockerfile.ws-low-latency" \
  -o "$build_dir/Dockerfile"
image="hyperliquid-ws:wallets-${revision:0:7}"
docker build --build-arg "SOURCE_REVISION=$revision" -t "$image" "$build_dir"
mode_args=()
if [[ "$HL_NODE_OUTPUT_MODE" = stream ]]; then
  mode_args+=(--stream-with-block-info)
fi
# Build failure exits above, leaving the running container intact.
if docker container inspect hyperliquid-ws-low-latency >/dev/null 2>&1; then
  docker stop hyperliquid-ws-low-latency
  docker rm hyperliquid-ws-low-latency
fi
docker run -d \
  --name hyperliquid-ws-low-latency \
  --init --network host --restart unless-stopped \
  --log-driver json-file --log-opt max-size=20m --log-opt max-file=3 \
  -e "WS_WALLETS=$WS_WALLETS" \
  -e "WS_WALLET_POLL_INTERVAL_MS=${WS_WALLET_POLL_INTERVAL_MS:-30000}" \
  -e "WS_WALLET_ACCOUNT_INTERVAL_MS=${WS_WALLET_ACCOUNT_INTERVAL_MS:-1000}" \
  -e "WS_WALLET_EVENT_INTERVAL_MS=${WS_WALLET_EVENT_INTERVAL_MS:-100}" \
  -e "WS_WALLET_HISTORY_EVENTS=${WS_WALLET_HISTORY_EVENTS:-100000}" \
  -e "WS_WALLET_HISTORY_DAYS=${WS_WALLET_HISTORY_DAYS:-7}" \
  -v "$HL_DATA_DIR:/node-data" \
  "$image" \
  --address 0.0.0.0 --port 8000 \
  --node-data-dir /node-data \
  --snapshot-path /node-data/ws-snapshot.json \
  --snapshot-node-path "$HL_NODE_SNAPSHOT_PATH" \
  --info-url http://127.0.0.1:3001/info \
  --markets BTC,HYPE,AERO,xyz:NVDA,xyz:DRAM \
  --websocket-compression-level 0 --integrity-interval-secs 0 --poll-interval-ms 5 \
  "${mode_args[@]}"
echo 'Replacement started. Check docker logs and /health for Ready before reconnecting the bot.'
