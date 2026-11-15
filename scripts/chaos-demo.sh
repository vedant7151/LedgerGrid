#!/usr/bin/env bash
# LedgerGrid chaos demo: kill ledger-service mid-flight, show Kafka buffering,
# restart it and watch the backlog drain, then tamper with a ledger row and show
# /ledger/verify pinpoint it.
#
# Prereq: a running stack, from the repo root:
#   docker compose -f docker-compose.yml -f docker-compose.apps.yml up -d --build
# Usage:  ./scripts/chaos-demo.sh
#
# Env overrides: ACCOUNTS_URL, TX_URL, LEDGER_URL, DEMO_USER, DEMO_PASS, PAUSE (seconds between steps)
set -euo pipefail
# Stop Git Bash on Windows from rewriting /opt/... paths passed to docker exec.
export MSYS_NO_PATHCONV=1

ACCOUNTS_URL="${ACCOUNTS_URL:-http://localhost:8081}"
TX_URL="${TX_URL:-http://localhost:8082}"
LEDGER_URL="${LEDGER_URL:-http://localhost:8083}"
DEMO_USER="${DEMO_USER:-admin}"
DEMO_PASS="${DEMO_PASS:-admin123}"
PAUSE="${PAUSE:-2}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-120}"

cd "$(dirname "$0")/.."
# Same project/files the stack was started with.
DC=(docker compose -f docker-compose.yml -f docker-compose.apps.yml)

if [ -t 1 ]; then B=$'\033[1m'; G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; C=$'\033[36m'; N=$'\033[0m'
else B=""; G=""; R=""; Y=""; C=""; N=""; fi

step()  { echo; echo "${B}${C}=== STEP $1: $2 ===${N}"; }
info()  { echo "  ${Y}->${N} $*"; }
ok()    { echo "  ${G}OK${N}  $*"; }
fail()  { echo "  ${R}FAIL${N} $*" >&2; exit 1; }
pause() { sleep "$PAUSE"; }

# json_get <key>: read a top-level scalar from JSON on stdin (no jq needed).
json_get() { sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\{0,1\}\([^\",}]*\)\"\{0,1\}.*/\1/p" | head -1; }

# Tag the Kafka-written entries by description so we can count them.
TAG="chaos-demo-$(date +%s)"

api() { # api METHOD URL [BODY]
  local m="$1" u="$2" b="${3:-}"
  if [ -n "$b" ]; then
    curl -sS -X "$m" "$u" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "$b"
  else
    curl -sS -X "$m" "$u" -H "Authorization: Bearer $TOKEN"
  fi
}

transfer() { # transfer <label> <amount>
  local resp id status
  resp=$(api POST "$TX_URL/transactions" \
    "{\"fromAccount\":\"$FROM\",\"toAccount\":\"$TO\",\"amount\":$2,\"description\":\"$TAG $1\",\"merchant\":\"ChaosMart\"}")
  id=$(echo "$resp" | json_get id); status=$(echo "$resp" | json_get status)
  [ "$status" = "COMPLETED" ] || fail "transfer '$1' did not complete: $resp"
  ok "transfer '$1' ($2) -> txn $id status=$status"
}

demo_count() { # number of ledger entries created by this run
  # Tolerate the ledger being mid-restart (curl/grep failures just mean "0 so far").
  { api GET "$LEDGER_URL/ledger" 2>/dev/null || true; } | { grep -o "$TAG" || true; } | wc -l | tr -d ' '
}

kafka_lag() { # consumer lag of ledger-service group, summed over partitions
  "${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group ledger-service 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}'
}

# ---------------------------------------------------------------------------
step 0 "Preflight and login"
for u in "$ACCOUNTS_URL" "$TX_URL" "$LEDGER_URL"; do
  curl -sf "$u/actuator/health" >/dev/null || fail "$u is not healthy. Start the stack first (see script header)."
done
ok "all three services healthy"
if [ "$(curl -sS "$LEDGER_URL/ledger/verify" | json_get valid)" != "true" ]; then
  fail "ledger chain is already broken (a previous demo run tampered with it). Reset with: ${DC[*]} down -v && ${DC[*]} up -d --build"
fi
TOKEN=$(curl -sS -X POST "$TX_URL/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"$DEMO_USER\",\"password\":\"$DEMO_PASS\"}" | json_get token)
[ -n "$TOKEN" ] || fail "login failed"
ok "logged in as $DEMO_USER (JWT obtained)"
FROM=$(api POST "$ACCOUNTS_URL/accounts" '{"ownerName":"Chaos Alice","initialBalance":10000,"currency":"INR"}' | json_get id)
TO=$(api POST "$ACCOUNTS_URL/accounts" '{"ownerName":"Chaos Bob","initialBalance":0,"currency":"INR"}' | json_get id)
[ -n "$FROM" ] && [ -n "$TO" ] || fail "could not create demo accounts"
ok "created accounts  Alice=$FROM  Bob=$TO"

# ---------------------------------------------------------------------------
step 1 "Normal transfer while everything is up"
transfer "before-outage" 100
for i in $(seq 1 30); do [ "$(demo_count)" -ge 1 ] && break; sleep 1; done
[ "$(demo_count)" -ge 1 ] || fail "first transfer never reached the ledger"
ok "ledger entry appeared (ledger entries from this demo: $(demo_count))"
pause

# ---------------------------------------------------------------------------
step 2 "Kill ledger-service (simulated crash)"
"${DC[@]}" stop ledger-service >/dev/null 2>&1
ok "ledger-service stopped. Transactions + accounts services keep running."
pause

# ---------------------------------------------------------------------------
step 3 "Transfers while the ledger is DOWN: events buffer in Kafka"
transfer "during-outage-1" 200
transfer "during-outage-2" 300
transfer "during-outage-3" 400
sleep 3
LAG=$(kafka_lag)
info "Kafka consumer lag for group 'ledger-service': ${B}${LAG}${N} unconsumed message(s)"
info "(also visible in Kafka UI: http://localhost:8080)"
[ "${LAG:-0}" -ge 3 ] && ok "backlog of $LAG events is safely buffered in Kafka" \
  || info "lag reading was $LAG (expected >= 3); continuing, the drain check below is the real proof"
pause

# ---------------------------------------------------------------------------
step 4 "Restart ledger-service"
"${DC[@]}" start ledger-service >/dev/null 2>&1
ok "ledger-service starting..."

# ---------------------------------------------------------------------------
step 5 "Wait for the backlog to drain into the ledger"
TOKEN=$(curl -sS -X POST "$TX_URL/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"$DEMO_USER\",\"password\":\"$DEMO_PASS\"}" | json_get token)
START=$(date +%s)
while :; do
  N_ENTRIES=$(demo_count)
  echo "  ... ledger entries from this demo: $N_ENTRIES / 4  (lag: $(kafka_lag 2>/dev/null || echo '?'))"
  [ "$N_ENTRIES" -ge 4 ] && break
  [ $(( $(date +%s) - START )) -ge "$DRAIN_TIMEOUT" ] && fail "backlog not drained within ${DRAIN_TIMEOUT}s"
  sleep 3
done
ok "all 4 transfers are in the ledger: nothing was lost while the service was down"
V=$(curl -sS "$LEDGER_URL/ledger/verify")
echo "  verify -> $(echo "$V" | json_get message)"
[ "$(echo "$V" | json_get valid)" = "true" ] || fail "chain should be valid before tampering: $V"
ok "hash chain is intact after recovery"
pause

# ---------------------------------------------------------------------------
step 6 "Tamper demo: edit a ledger row directly in Postgres"
VICTIM=$("${DC[@]}" exec -T postgres psql -U payflow -d ledger_db -At \
  -c "select id from ledger_entries where description like '$TAG during-outage-2' limit 1;" | tr -d '\r')
[ -n "$VICTIM" ] || fail "could not find the row to tamper with"
info "attacker runs:  UPDATE ledger_entries SET amount = 1.00 WHERE id = '$VICTIM';"
"${DC[@]}" exec -T postgres psql -U payflow -d ledger_db -c \
  "update ledger_entries set amount = 1.00 where id = '$VICTIM';" | sed 's/^/  psql: /'
# Cached reads of /ledger/verify are not used, but be safe: verify reads the DB.
V=$(curl -sS "$LEDGER_URL/ledger/verify")
echo
echo "  ${B}GET /ledger/verify${N}"
echo "  $V"
if [ "$(echo "$V" | json_get valid)" = "false" ]; then
  echo
  fail_id=$(echo "$V" | json_get firstInvalidEntryId); fail_ix=$(echo "$V" | json_get firstInvalidChainIndex)
  ok "TAMPERING DETECTED: entry $fail_id at chain index $fail_ix"
  [ "$fail_id" = "$VICTIM" ] && ok "and it is exactly the row we edited"
else
  fail "verify still reports valid after tampering"
fi

# ---------------------------------------------------------------------------
echo
echo "${B}${G}Demo complete:${N} crash survived (Kafka buffered 3 events, all drained), tampering caught."
echo "Note: the edited row was left tampered, so /ledger/verify will keep failing until the DB is reset"
echo "  (docker compose down -v)."
