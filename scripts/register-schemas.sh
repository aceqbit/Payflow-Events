#!/bin/bash
# Registers every schema in common/src/main/resources/schemas in Apicurio Registry 3 with BACKWARD compatibility.
# A new version that is incompatible is rejected by the registry (HTTP 409) and this script fails.
set -euo pipefail
REG=${REGISTRY:-http://localhost:8081}/apis/registry/v3
GROUP=payflow
DIR="$(dirname "$0")/../common/src/main/resources/schemas"
for f in "$DIR"/*.json; do
  id=$(basename "$f" .json)
  body=$(python3 -c 'import json,sys; print(json.dumps({"artifactId":sys.argv[1],"artifactType":"JSON","firstVersion":{"content":{"content":open(sys.argv[2]).read(),"contentType":"application/json"}}}))' "$id" "$f")
  code=$(curl -s -o /tmp/reg.out -w '%{http_code}' -X POST -H 'Content-Type: application/json' "$REG/groups/$GROUP/artifacts" -d "$body")
  if [ "$code" = 409 ]; then
    curl -s -X PUT -H 'Content-Type: application/json' "$REG/groups/$GROUP/artifacts/$id/rules/COMPATIBILITY" -d '{"ruleType":"COMPATIBILITY","config":"BACKWARD"}' >/dev/null
    vbody=$(python3 -c 'import json,sys; print(json.dumps({"content":{"content":open(sys.argv[1]).read(),"contentType":"application/json"}}))' "$f")
    code=$(curl -s -o /tmp/reg.out -w '%{http_code}' -X POST -H 'Content-Type: application/json' "$REG/groups/$GROUP/artifacts/$id/versions" -d "$vbody")
  else
    curl -s -X POST -H 'Content-Type: application/json' "$REG/groups/$GROUP/artifacts/$id/rules" -d '{"ruleType":"COMPATIBILITY","config":"BACKWARD"}' >/dev/null
  fi
  [ "$code" -lt 300 ] || { echo "FAIL $id ($code): $(cat /tmp/reg.out)"; exit 1; }
  echo "ok $id"
done
