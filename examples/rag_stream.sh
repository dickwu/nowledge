#!/bin/sh
set -eu

base_url=${NOWLEDGE_URL:-http://127.0.0.1:14242}
question=${1:-What changed?}

if [ -n "${NOWLEDGE_TOKEN:-}" ]; then
  set -- -H "Authorization: Bearer ${NOWLEDGE_TOKEN}"
else
  set --
fi

payload=$(printf '%s' "$question" | python3 -c \
  'import json, sys; print(json.dumps({"question": sys.stdin.read()}))')

curl --fail-with-body --no-buffer \
  -H 'Accept: text/event-stream' \
  -H 'Content-Type: application/json' \
  "$@" \
  --data "$payload" \
  "${base_url%/}/v1/rag/stream"
