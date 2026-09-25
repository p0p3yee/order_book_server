#!/usr/bin/env bash
# Run manually on the Linux node host. Builds before replacing only the WS container.
set -euo pipefail
revision=${1:?Usage: WS_WALLETS=0xADDRESS[,0xADDRESS] bash deploy_local_wallets.sh FULL_COMMIT_SHA}
[[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo 'Expected full lowercase commit SHA' >&2; exit 1; }
: "${WS_WALLETS:?Set WS_WALLETS to the comma-separated trading wallet addresses}"
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
  -v /path/to/node-data:/node-data \
  "$image" \
  --address 0.0.0.0 --port 8000 \
  --node-data-dir /node-data \
  --snapshot-path /node-data/ws-snapshot.json \
  --snapshot-node-path /path/in/node/data/ws-snapshot.json \
  --info-url http://127.0.0.1:3001/info \
  --markets BTC,HYPE,AERO,xyz:NVDA,xyz:DRAM \
  --websocket-compression-level 0 --integrity-interval-secs 0 --poll-interval-ms 5
echo 'Replacement started. Check docker logs and /health for Ready before reconnecting the bot.'
