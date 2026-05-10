#!/usr/bin/env bash
# Chaos test over TLS. Same flow as chaos-smoke (v1) but every client connection
# runs a real SSL_connect against the router's TLS-terminating acceptor.
# Asserts dropped == 0 end-to-end.
#
# Usage: bench/chaos-tls.sh [clients] [requests] [sigterm_ms]
set -euo pipefail

CLIENTS="${1:-10}"
REQUESTS="${2:-50}"
SIGTERM_MS="${3:-1500}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${ROOT}/build"
CERT_DIR="${ROOT}/tests/certs"
CERT="${CERT_DIR}/cert.pem"
KEY="${CERT_DIR}/key.pem"
OUT="${ROOT}/bench/chaos-tls.json"

if [ ! -x "${BUILD_DIR}/chaos" ]; then
    echo "chaos binary not found at ${BUILD_DIR}/chaos — run cmake build first" >&2
    exit 1
fi

# Generate a self-signed cert if missing. CI regenerates per run; locally we
# keep whatever's there so reruns are fast.
if [ ! -f "${CERT}" ] || [ ! -f "${KEY}" ]; then
    mkdir -p "${CERT_DIR}"
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -keyout "${KEY}" -out "${CERT}" \
        -subj "/C=US/O=inference-router/CN=localhost" \
        >/dev/null 2>&1
fi

"${BUILD_DIR}/chaos" \
    --clients "${CLIENTS}" --requests "${REQUESTS}" \
    --sigterm-after-ms "${SIGTERM_MS}" \
    --threads 4 --pool-size 4 --shutdown-grace 30000 \
    --tls --tls-cert "${CERT}" --tls-key "${KEY}" \
    --out "${OUT}"

cat "${OUT}"

DROPPED=$(grep -oE '"dropped_total": [0-9]+' "${OUT}" | awk '{print $2}')
echo "dropped=${DROPPED}"
test "${DROPPED}" = "0"
