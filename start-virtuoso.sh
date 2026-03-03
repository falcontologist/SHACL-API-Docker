#!/bin/bash
set -e

DB_DIR="/opt/virtuoso-opensource/database"
DB_PATH="$DB_DIR/virtuoso.db"
DB_URL="https://storage.googleapis.com/fkg/virtuoso.db"
EXPECTED_SIZE=4000000000  # ~4GB - adjust if your baked DB changes size

echo "[start-virtuoso] Checking database state..."

# Check if the DB file is missing or smaller than expected (empty default is ~68-71MB)
if [ ! -f "$DB_PATH" ] || [ $(stat -c%s "$DB_PATH") -lt "$EXPECTED_SIZE" ]; then
    CURRENT_SIZE=0
    [ -f "$DB_PATH" ] && CURRENT_SIZE=$(stat -c%s "$DB_PATH")
    echo "[start-virtuoso] Current DB is ${CURRENT_SIZE} bytes — too small. Downloading master from GCS..."

    # Remove empty default files
    rm -f "$DB_DIR/virtuoso.db"
    rm -f "$DB_DIR/virtuoso.trx"
    rm -f "$DB_DIR/virtuoso.lck"

    # Download with retry and resume support
    curl -L -C - --retry 3 --retry-delay 5 -o "$DB_PATH" "$DB_URL"

    # Verify download
    ACTUAL_SIZE=$(stat -c%s "$DB_PATH")
    echo "[start-virtuoso] Download complete: ${ACTUAL_SIZE} bytes."

    if [ "$ACTUAL_SIZE" -lt "$EXPECTED_SIZE" ]; then
        echo "[start-virtuoso] ERROR: Download appears incomplete (${ACTUAL_SIZE} < ${EXPECTED_SIZE}). Aborting."
        exit 1
    fi

    # Clean up any stale lock/transaction files that could cause reinitialization
    rm -f "$DB_DIR/virtuoso.trx"
    rm -f "$DB_DIR/virtuoso.lck"

    echo "[start-virtuoso] Master database ready."
else
    echo "[start-virtuoso] Master database already exists ($(stat -c%s "$DB_PATH") bytes). Skipping download."
fi

# Patch virtuoso.ini for Render memory limits (512 MB)
INI_FILE="$DB_DIR/virtuoso.ini"
if [ -f "$INI_FILE" ]; then
    echo "[start-virtuoso] Patching virtuoso.ini for Render memory limits..."

    # Set NumberOfBuffers and MaxDirtyBuffers if not already set
    if ! grep -q "^NumberOfBuffers" "$INI_FILE"; then
        sed -i '/^\[Parameters\]/a NumberOfBuffers          = 20000\nMaxDirtyBuffers          = 15000' "$INI_FILE"
    fi

    # Cap MaxQueryMem
    sed -i 's/^MaxQueryMem.*=.*/MaxQueryMem              = 50M/' "$INI_FILE"

    echo "[start-virtuoso] virtuoso.ini patched."
else
    echo "[start-virtuoso] WARNING: virtuoso.ini not found at $INI_FILE — Virtuoso will create defaults."
fi

# Start Virtuoso in foreground mode (required for Docker)
echo "[start-virtuoso] Starting Virtuoso..."
exec /opt/virtuoso-opensource/bin/virtuoso-t -f