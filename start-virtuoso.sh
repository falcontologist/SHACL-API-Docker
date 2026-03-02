#!/bin/bash

DB_PATH="/opt/virtuoso-opensource/database/virtuoso.db"
DB_URL="https://storage.googleapis.com/fkg/virtuoso.db"

# Check if the file is smaller than ~100MB (indicating an empty default DB)
if [ ! -f "$DB_PATH" ] || [ $(stat -c%s "$DB_PATH") -lt 100000000 ]; then
    echo "Downloading 3.8GB master database from GCS..."
    
    # Remove the empty default files safely
    rm -f /opt/virtuoso-opensource/database/virtuoso.db
    rm -f /opt/virtuoso-opensource/database/virtuoso.trx
    
    # Download the master file
    curl -L -o "$DB_PATH" "$DB_URL"
    
    echo "Download complete!"
else
    echo "Master database already exists. Skipping download."
fi

# Hand control back to the original OpenLink Virtuoso startup sequence
exec /virtuoso.sh