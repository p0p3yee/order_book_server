# Public repository configuration

Use synthetic addresses in examples and fixtures. Actual wallet allowlists, LAN
endpoints and host/container paths belong in private environment configuration.
`.env` and `.env.*` files (except `.env.example`) are excluded from Git and Docker
build contexts. These ignore rules do not remove already tracked files or history.

For a private deployment, copy `.env.example` to `.env` on the node host, fill in
its actual paths and add `WS_WALLETS`. The standalone script requires
`HL_DATA_DIR`, `HL_NODE_SNAPSHOT_PATH` and `WS_WALLETS` to be exported. Source only
your own trusted environment file:

```sh
set -a
. ./.env
set +a
bash scripts/deploy_local_wallets.sh FULL_COMMIT_SHA
```

Compose reads `.env` automatically and requires both path variables. Probe a
remote node by passing `--local-ws` and `--local-info`; defaults are loopback.
Do not commit filled-in environment files or diagnostic captures.

Before committing:

```sh
python3 scripts/check_public_config.py
# Enable the staged-file check in this clone:
git config core.hooksPath .githooks
```

The check detects literal non-synthetic wallet addresses, private IPv4 addresses,
and common machine-specific paths in tracked text. It is a guardrail, not a
complete secret scanner. It does not inspect historical commits or encoded values.
