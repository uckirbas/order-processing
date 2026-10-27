#!/usr/bin/env bash
# How fast do the workers clear a backlog? Stops the invoicing and notification workers, places N orders (the
# relay queues them), starts the workers again and times the drain: until every order is invoiced and both of its
# e-mails are recorded. Reports drain time, commits per order per database and average CPU per service.
#
#   loadtest/drain.sh 60000                                                   batched (default settings)
#   loadtest/drain.sh 60000 -f docker-compose.yml -f loadtest/per-message.yml one message per handler call
#
# Env: RATE (orders/s while building the backlog, default 3000).
set -euo pipefail
cd "$(dirname "$0")/.."
N=$1; shift
RATE=${RATE:-3000}
# Extra compose files (e.g. the per-message override); empty-array-safe for macOS bash 3.2.
FILES=("$@")
app() { docker compose ${FILES[@]+"${FILES[@]}"} --profile app "$@"; }

sql() { docker compose exec -T postgres psql -U postgres -tAqc "$2" "$1"; }
progress() {
  echo "$(sql ordering "select count(*) from order_status where status = 'Invoiced'")" \
       "$(sql notification "select count(*) filter (where type = 'OrderReceived' and status in ('Sent','Dead')),
                                   count(*) filter (where type = 'InvoiceReady' and status in ('Sent','Dead'))
                            from notifications" | tr '|' ' ')"
}
commits() {
  sql postgres "select string_agg(datname || '=' || xact_commit, ' ' order by datname)
                from pg_stat_database where datname in ('ordering', 'invoicing', 'notification')"
}
now() { python3 -c 'import time; print(time.time())'; }

echo "== starting the services, invoicing and notification workers stopped"
# Images must be built already (docker compose --profile app build): rebuilding would recreate the API containers,
# and cold replicas under full load spiral into timeouts (DESIGN.md, §12).
app up -d --scale invoicing-worker=0 --scale notification-worker=0 > /dev/null 2>&1
sleep 5
# Warm-up with a ramp: JIT and empty connection pools make freshly started replicas slow for a while.
docker compose run --rm -e RATE=1500 -e RAMP=20s -e HOLD=5s k6 run --quiet /scripts/orders.js > /dev/null 2>&1 || true
read -r inv0 rec0 rdy0 <<< "$(progress)"

echo "== placing $N orders at $RATE/s"
docker compose run --rm -e PROFILE=steady -e RATE="$RATE" -e DURATION="$((N / RATE))s" k6 run --quiet /scripts/orders.js \
  | grep -E "orders_(created|shed|failed)\.|http_req_duration\{" | sed 's/^/   /' || true
until [[ $(sql ordering "select count(*) from outbox_messages") == 0 ]]; do sleep 1; done
backlog=$(docker compose exec -T rabbitmq rabbitmqctl list_queues name messages -q | awk '$1 == "invoicing.order-placed" { print $2 }')
echo "   backlog in invoicing.order-placed: $backlog (includes the warm-up orders)"
created=$backlog

sleep 11 # pg_stat counters of idle backends are flushed within 10 s
c0=$(commits)
t0=$(now)
echo "== starting the workers"
app up -d --scale invoicing-worker=2 --scale notification-worker=2 > /dev/null 2>&1

stats=$(mktemp)
round=0
while true; do
  round=$((round + 1))
  docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' | sed "s/^/$round /" >> "$stats"
  read -r inv rec rdy <<< "$(progress)"
  (( inv - inv0 >= created && rec - rec0 >= created && rdy - rdy0 >= created )) && break
  sleep 1
done
t1=$(now)
sleep 11
c1=$(commits)

python3 - "$c0" "$c1" "$created" "$t0" "$t1" "$stats" <<'EOF'
import re, sys, collections
before = dict(kv.split('=') for kv in sys.argv[1].split())
after = dict(kv.split('=') for kv in sys.argv[2].split())
n, t0, t1 = int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5])
print(f"== drained {n} orders in {t1 - t0:.1f} s ({n / (t1 - t0):.0f} orders/s, {4 * n / (t1 - t0):.0f} messages/s)")
for db in ('ordering', 'invoicing', 'notification'):
    d = int(after[db]) - int(before[db])
    print(f"   commits {db:<13} {d:>8}  ({d / n:.2f} per order)")
# One line per container per sampling round; the replicas of a service add up within a round.
per_round = collections.defaultdict(lambda: collections.defaultdict(float))
for line in open(sys.argv[6]):
    round_no, name, cpu = line.split()
    service = re.sub(r'^order-processing-|-\d+$', '', name)
    per_round[service][round_no] += float(cpu.rstrip('%'))
print("   average CPU while draining (100% = one core):")
for service in sorted(per_round):
    rounds = per_round[service]
    print(f"     {service:<20} {sum(rounds.values()) / len(rounds):6.0f}%")
EOF
rm -f "$stats"
