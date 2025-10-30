#!/bin/bash
set -euo pipefail

NODE_ID="${TRINO_NODE_ID:-${HOSTNAME:-}}"
if [[ -z "${NODE_ID}" ]]; then
  echo "Unable to determine node ID; set TRINO_NODE_ID or ensure HOSTNAME is available" >&2
  exit 1
fi

NODE_CONFIG=/tmp/node.properties
sed "s/{{NODE_ID}}/${NODE_ID}/" /etc/trino-worker/node.properties.tpl > "${NODE_CONFIG}"

exec /usr/lib/trino/bin/run-trino \
  --config /etc/trino-worker/config.properties \
  --node-config "${NODE_CONFIG}" \
  "$@"
