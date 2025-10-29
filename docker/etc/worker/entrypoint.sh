#!/bin/bash
set -euo pipefail

if [[ -z "${TRINO_NODE_ID:-}" ]]; then
  echo "TRINO_NODE_ID environment variable must be set for worker nodes" >&2
  exit 1
fi

NODE_CONFIG=/tmp/node.properties
sed "s/{{NODE_ID}}/${TRINO_NODE_ID}/" /etc/trino-worker/node.properties.tpl > "${NODE_CONFIG}"

exec /usr/lib/trino/bin/run-trino \
  --config /etc/trino-worker/config.properties \
  --node-config "${NODE_CONFIG}" \
  "$@"
