#!/bin/bash

# Your local IP and port
YOUR_IP="192.168.86.91"
PORT="8000"
VIRTUOSO_URL="https://virtuoso-sparql-service.onrender.com/sparql"
GRAPH="http://shacl-demo.org/type"

# Virtuoso admin credentials (change after loading)
DBA_USER="dba"
DBA_PASSWORD="D9qTgVVc"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting load from http://${YOUR_IP}:${PORT} to Virtuoso${NC}"
echo "========================================"

# Files in correct order from manifest.ttl
FILES=(
  "conceptual.ttl"
  "structural.ttl"
  "lexical.ttl"
  "gpe_entity.ttl"
  "gpe_entry.ttl"
  "product_entity.ttl"
  "product_entry.ttl"
  "person_entity.ttl"
  "person_entry.ttl"
  "organization_entity.ttl"
  "organization_entry.ttl"
)

SUCCESS_COUNT=0
FAIL_COUNT=0

for file in "${FILES[@]}"; do
  echo -e "\n${YELLOW}Loading $file...${NC}"
  
  # Check if file exists locally
  if [ ! -f "$file" ]; then
    echo -e "${RED}  ⚠️  File not found locally: $file (skipping)${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    continue
  fi
  
  # Construct the LOAD query
  LOAD_URL="http://${YOUR_IP}:${PORT}/$file"
  QUERY="LOAD <$LOAD_URL> INTO GRAPH <$GRAPH>"
  
  echo "  From: $LOAD_URL"
  
  # Send SPARQL UPDATE with authentication
  RESPONSE=$(curl -s -w "\n%{http_code}" \
    -u "$DBA_USER:$DBA_PASSWORD" \
    -X POST "$VIRTUOSO_URL" \
    -H "Content-Type: application/sparql-update" \
    --data-binary "$QUERY")
  
  # Extract status code (last line)
  HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
  BODY=$(echo "$RESPONSE" | sed '$d')
  
  if [ "$HTTP_CODE" -eq 200 ]; then
    echo -e "${GREEN}  ✅ Loaded $file (HTTP $HTTP_CODE)${NC}"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo -e "${RED}  ❌ Failed to load $file (HTTP $HTTP_CODE)${NC}"
    echo "  Response: $BODY"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  
  echo "  Waiting 2 seconds..."
  sleep 2
done

echo -e "\n${YELLOW}========================================${NC}"
echo -e "${GREEN}Successfully loaded: $SUCCESS_COUNT files${NC}"
if [ $FAIL_COUNT -gt 0 ]; then
  echo -e "${RED}Failed: $FAIL_COUNT files${NC}"
fi