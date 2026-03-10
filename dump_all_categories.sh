#!/usr/bin/env bash
set -euo pipefail

REPO="/Volumes/Extreme Pro/FKG/SHACL-API-Docker"
DUMP_DIR="/Volumes/Extreme Pro/FKG/tsv_dumps"
SCRIPT="$REPO/dump_ttl_to_tsv.py"

mkdir -p "$DUMP_DIR"
cd "$REPO"

CATEGORIES=(
  Creative_Work
  Quantity_Dimension
  Location
  Food
  Language
  Organism
  Equity
  Index
  Corporate_Bond
  Government_Bond
)

for cat in "${CATEGORIES[@]}"; do
  lower=$(echo "$cat" | tr '[:upper:]' '[:lower:]')
  entry="${lower}_entry.ttl"
  entity="${lower}_entity.ttl"
  output="$DUMP_DIR/${lower}_dump.tsv"

  if [ -f "$output" ]; then
    lines=$(wc -l < "$output")
    echo "⏭  $cat: already dumped ($lines lines), skipping"
    continue
  fi

  if [ ! -f "$entry" ] || [ ! -f "$entity" ]; then
    echo "⚠️  $cat: missing TTL files, skipping"
    continue
  fi

  python3 "$SCRIPT" "$cat" "$entry" "$entity" "$output"
  echo ""
done

echo "═══════════════════════════════════════════"
echo "All dumps complete. Summary:"
echo "═══════════════════════════════════════════"
for cat in "${CATEGORIES[@]}"; do
  lower=$(echo "$cat" | tr '[:upper:]' '[:lower:]')
  output="$DUMP_DIR/${lower}_dump.tsv"
  if [ -f "$output" ]; then
    lines=$(wc -l < "$output")
    printf "  %-25s %s lines\n" "$cat" "$lines"
  else
    printf "  %-25s MISSING\n" "$cat"
  fi
done
