#!/bin/sh
set -e

DREMIO_URL="http://dremio:9047"
NESSIE_ENDPOINT="http://nessie:19120/api/v2"
MINIO_ENDPOINT="minio:9000"
S3_ACCESS_KEY="${NESSIE_S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${NESSIE_S3_SECRET_KEY:-minioadmin}"

echo "Waiting for Dremio to be ready..."
until curl -sf "$DREMIO_URL/apiv2/server_status" > /dev/null 2>&1; do
  sleep 5
done
echo "Dremio is ready"

# Bootstrap first user (idempotent — fails silently if already created)
echo "Bootstrapping first user..."
BOOTSTRAP_RESP=$(curl -s -o /dev/stderr -w "%{http_code}" -X PUT "$DREMIO_URL/apiv2/bootstrap/firstuser" \
  -H "Content-Type: application/json" \
  -d '{
    "userName": "admin",
    "firstName": "Admin",
    "lastName": "User",
    "email": "admin@urbanpulse.local",
    "createdAt": 0,
    "password": "urbanpulse123"
  }' 2>&1) || true
echo "Bootstrap response code: $BOOTSTRAP_RESP"

# Login and get token — use -s (not -sf) so we can see error responses
echo "Logging in..."
LOGIN_RESP=$(curl -s -X POST "$DREMIO_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  -d '{"userName": "admin", "password": "urbanpulse123"}')

if [ -z "$LOGIN_RESP" ]; then
  echo "ERROR: Empty login response"
  exit 1
fi

# Extract token, show response on failure
TOKEN=$(echo "$LOGIN_RESP" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data['token'])
except Exception as e:
    print(f'Parse error: {e}', file=sys.stderr)
    sys.exit(1)
") || {
  echo "ERROR: Failed to parse login response: $LOGIN_RESP"
  exit 1
}

if [ -z "$TOKEN" ]; then
  echo "ERROR: Failed to get auth token"
  exit 1
fi
echo "Got auth token"

# --- Helper: create source if not exists ---
create_source() {
  SOURCE_NAME="$1"
  SOURCE_JSON="$2"

  EXISTING=$(curl -s "$DREMIO_URL/api/v3/catalog/by-path/$SOURCE_NAME" \
    -H "Authorization: Bearer $TOKEN" 2>/dev/null || echo "")

  if echo "$EXISTING" | grep -q '"entityType"'; then
    echo "Source '$SOURCE_NAME' already exists — skipping"
    return 0
  fi

  echo "Creating source '$SOURCE_NAME'..."
  CREATE_RESP=$(curl -s -w "\n%{http_code}" -X POST "$DREMIO_URL/api/v3/catalog" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "$SOURCE_JSON")

  HTTP_CODE=$(echo "$CREATE_RESP" | tail -1)
  BODY=$(echo "$CREATE_RESP" | sed '$d')

  if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
    echo "Source '$SOURCE_NAME' created successfully"
  else
    echo "ERROR: Failed to create source '$SOURCE_NAME' (HTTP $HTTP_CODE): $BODY"
    return 1
  fi
}

# --- Nessie source (Iceberg catalog) ---
create_source "nessie" "{
  \"entityType\": \"source\",
  \"type\": \"NESSIE\",
  \"name\": \"nessie\",
  \"config\": {
    \"nessieEndpoint\": \"$NESSIE_ENDPOINT\",
    \"nessieAuthType\": \"NONE\",
    \"credentialType\": \"ACCESS_KEY\",
    \"storageProvider\": \"AWS\",
    \"awsAccessKey\": \"$S3_ACCESS_KEY\",
    \"awsAccessSecret\": \"$S3_SECRET_KEY\",
    \"awsRootPath\": \"/warehouse\",
    \"secure\": false,
    \"propertyList\": [
      {\"name\": \"fs.s3a.endpoint\", \"value\": \"$MINIO_ENDPOINT\"},
      {\"name\": \"fs.s3a.path.style.access\", \"value\": \"true\"},
      {\"name\": \"dremio.s3.compat\", \"value\": \"true\"}
    ]
  },
  \"metadataPolicy\": {
    \"authTTLMs\": 86400000,
    \"namesRefreshMs\": 3600000,
    \"datasetRefreshAfterMs\": 3600000,
    \"datasetExpireAfterMs\": 10800000,
    \"datasetUpdateMode\": \"PREFETCH_QUERIED\",
    \"deleteUnavailableDatasets\": true
  }
}"

# --- MinIO S3 source (direct access to buckets) ---
create_source "minio" "{
  \"entityType\": \"source\",
  \"type\": \"S3\",
  \"name\": \"minio\",
  \"config\": {
    \"credentialType\": \"ACCESS_KEY\",
    \"accessKey\": \"$S3_ACCESS_KEY\",
    \"accessSecret\": \"$S3_SECRET_KEY\",
    \"secure\": false,
    \"externalBucketList\": [\"urban-pulse\", \"warehouse\"],
    \"rootPath\": \"/\",
    \"propertyList\": [
      {\"name\": \"fs.s3a.endpoint\", \"value\": \"$MINIO_ENDPOINT\"},
      {\"name\": \"fs.s3a.path.style.access\", \"value\": \"true\"},
      {\"name\": \"dremio.s3.compat\", \"value\": \"true\"}
    ]
  },
  \"metadataPolicy\": {
    \"authTTLMs\": 86400000,
    \"namesRefreshMs\": 3600000,
    \"datasetRefreshAfterMs\": 3600000,
    \"datasetExpireAfterMs\": 10800000,
    \"datasetUpdateMode\": \"PREFETCH_QUERIED\",
    \"deleteUnavailableDatasets\": true
  }
}"

echo ""
echo "Dremio setup complete"
