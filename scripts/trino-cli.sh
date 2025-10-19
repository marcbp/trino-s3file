#!/usr/bin/env bash
set -euo pipefail

VERSION="476"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_JAR="${PROJECT_ROOT}/bin/trino-cli-${VERSION}.jar"
JAR_PATH="${TRINO_CLI_JAR:-$DEFAULT_JAR}"
SERVER_URL="${TRINO_CLI_SERVER:-http://localhost:8080}"

if [ ! -f "$JAR_PATH" ]; then
  echo "[trino-cli] Téléchargement du client Trino ${VERSION}..." >&2
  mkdir -p "$(dirname "$JAR_PATH")"
  curl -fL "https://repo1.maven.org/maven2/io/trino/trino-cli/${VERSION}/trino-cli-${VERSION}-executable.jar" -o "$JAR_PATH"
  chmod +x "$JAR_PATH"
fi

exec java ${TRINO_CLI_JAVA_OPTS:-} -jar "$JAR_PATH" --server "$SERVER_URL" "$@"
