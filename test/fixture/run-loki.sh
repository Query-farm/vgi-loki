#!/usr/bin/env bash
# Start the live fixture: grafana/loki (monolithic, filesystem storage) with
# deterministic streams pushed through /loki/api/v1/push.
#
#   test/fixture/run-loki.sh start   # prints the base URL
#   test/fixture/run-loki.sh stop
#
# The data (test/fixture/gen_push.py) has fixed 2026-01-01 timestamps, so the
# recorded responses are stable. The ruler loads test/fixture/rules/fake.
# Memory is capped at 768 MB (it idles near 150 MB).
set -euo pipefail
IMAGE="${LOKI_IMAGE:-grafana/loki:3.7.0}"
NAME="${LOKI_CONTAINER:-vgi-loki-server}"
PORT="${LOKI_PORT:-13100}"
HERE="$(cd "$(dirname "$0")" && pwd)"
case "${1:-start}" in
  stop) docker rm -f "$NAME" >/dev/null 2>&1 || true; exit 0 ;;
  start) ;;
  *) echo "usage: $0 start|stop"; exit 2 ;;
esac
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "127.0.0.1:$PORT:3100" -m 768m \
  -v "$HERE/loki.yml:/etc/loki/loki.yml:ro" \
  -v "$HERE/rules:/etc/loki/rules:ro" \
  "$IMAGE" -config.file=/etc/loki/loki.yml >/dev/null
BASE="http://127.0.0.1:$PORT"
for _ in $(seq 1 90); do
  curl -fsS "$BASE/ready" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "$BASE/ready" >/dev/null || { docker logs "$NAME" >&2; exit 1; }
python3 - "$BASE" "$HERE/gen_push.py" <<'PY'
import json, subprocess, sys, urllib.request
base, gen = sys.argv[1], sys.argv[2]
bodies = json.loads(subprocess.check_output([sys.executable, gen]))
for b in bodies:
    req = urllib.request.Request(base + "/loki/api/v1/push", data=json.dumps(b).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        assert r.status == 204, r.status
print(f"pushed {len(bodies)} bodies", file=sys.stderr)
PY
# This Loki only answers queries for old data from the store (ingesters are
# skipped for ranges this far in the past), so flush the ingester's chunks and
# wait until every pushed line is readable.
curl -fsS -X POST "$BASE/flush" >/dev/null
for _ in $(seq 1 60); do
  n="$(curl -fsS -G "$BASE/loki/api/v1/query" \
        --data-urlencode 'query=sum(count_over_time({job=~".+"}[4h]))' \
        --data-urlencode time=1767236400000000000 \
      | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else 0)')"
  [ "$n" = "6383" ] && { echo "$BASE"; exit 0; }
  sleep 2
done
echo "only $n of 6383 lines readable" >&2; exit 1
