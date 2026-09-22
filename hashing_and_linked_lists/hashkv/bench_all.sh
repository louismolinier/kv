#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YCSB_DIR="${YCSB_DIR:-$ROOT/../YCSB-cpp}"
RECORDS="${1:-100000}"
OPERATIONS="${2:-100000}"
THREADS="${3:-4}"
DB_PATH="${HASHKV_PATH:-/tmp/hashkv-all-workloads-${UID}.data}"
SCAN_CACHE_SLOTS="${HASHKV_SCAN_CACHE_SLOTS:-0}"

make -C "$ROOT" YCSB_DIR="$YCSB_DIR" build/ycsb_hashkv >/dev/null

printf 'YCSB A-F, %s records, %s operations, %s threads\n' \
  "$RECORDS" "$OPERATIONS" "$THREADS"
printf '| Workload | Load ops/s | Run ops/s | Run seconds |\n'
printf '|---|---:|---:|---:|\n'

for workload in a b c d e f; do
  output=$("$ROOT/build/ycsb_hashkv" -load -run -db hashkv \
    -P "$YCSB_DIR/workloads/workload$workload" \
    -P "$ROOT/ycsb.properties" \
    -threads "$THREADS" \
    -p "recordcount=$RECORDS" \
    -p "operationcount=$OPERATIONS" \
    -p "hashkv.path=$DB_PATH" \
    -p "hashkv.scan_cache_slots=$SCAN_CACHE_SLOTS" \
    -p hashkv.destroy=true \
    -p hashkv.stats=false)
  load_rate=$(printf '%s\n' "$output" | awk '/^Load throughput\(ops\/sec\):/ {print $3}')
  run_rate=$(printf '%s\n' "$output" | awk '/^Run throughput\(ops\/sec\):/ {print $3}')
  run_seconds=$(printf '%s\n' "$output" | awk '/^Run runtime\(sec\):/ {print $3}')
  printf '| %s | %s | %s | %s |\n' \
    "$workload" "$load_rate" "$run_rate" "$run_seconds"
done

printf 'Data files retained at: %s and %s.wal\n' "$DB_PATH" "$DB_PATH"
