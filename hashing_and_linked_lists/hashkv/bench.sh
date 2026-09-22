#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YCSB_DIR="${YCSB_DIR:-$ROOT/../YCSB-cpp}"
RECORDS="${1:-100000}"
OPERATIONS="${2:-100000}"
THREADS="${3:-4}"
DB_PATH="${HASHKV_PATH:-/tmp/hashkv-first-draft-${UID}.data}"
SCAN_CACHE_SLOTS="${HASHKV_SCAN_CACHE_SLOTS:-0}"

make -C "$ROOT" YCSB_DIR="$YCSB_DIR" build/ycsb_hashkv

"$ROOT/build/ycsb_hashkv" -load -run -db hashkv \
  -P "$YCSB_DIR/workloads/workloada" \
  -P "$ROOT/ycsb.properties" \
  -threads "$THREADS" \
  -p "recordcount=$RECORDS" \
  -p "operationcount=$OPERATIONS" \
  -p "hashkv.path=$DB_PATH" \
  -p "hashkv.scan_cache_slots=$SCAN_CACHE_SLOTS" \
  -p hashkv.destroy=true

echo "Data files: $DB_PATH and $DB_PATH.wal"
