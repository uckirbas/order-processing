#!/usr/bin/env bash
# The whole system under load: orders arrive at RATE for DURATION while every worker runs. Reports the API's latency
# and shedding during the load, CPU per service during the load, and how long after the start the last order was
# invoiced and both of its e-mails recorded.
#
#   loadtest/full.sh 3000 60s                                                   batched (default settings)
#   loadtest/full.sh 3000 60s -f docker-compose.yml -f loadtest/per-message.yml one message per handler call
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=$1; DURATION=$2; shift 2
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
now() { python3 -c 'import time; print(time.time())'; }

echo "== starting the services (images must be built already)"
app up -d > /dev/null 2>&1
sleep 5
# Warm-up with a ramp: JIT and empty connection pools make freshly started replicas slow for a while.
docker compose run --rm -e RATE=1500 -e RAMP=20s -e HOLD=5s k6 run --quiet /scripts/orders.js > /dev/null 2>&1 || true
# Let the warm-up's orders clear, so the measurement starts from empty queues.
until [[ -z $(docker compose exec -T rabbitmq rabbitmqctl list_queues name messages -q | awk '$1 !~ /dlq|retry|^name$/ && $2 > 0') ]]; do sleep 2; done
read -r inv0 rec0 rdy0 <<< "$(progress)"
total0=$(sql ordering "select count(*) from orders")

echo "== $RATE orders/s for $DURATION with every worker running"
stats=$(mktemp)
(
  sleep 10
  for round in 1 2 3 4 5; do
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' | sed "s/^/$round /" >> "$stats"
    sleep 5
  done
) &
t0=$(now)
docker compose run --rm -e PROFILE=steady -e RATE="$RATE" -e DURATION="$DURATION" k6 run --quiet /scripts/orders.js \
  | grep -E "orders_(created|shed|failed)\.\.|http_req_duration\.\.\." | sed 's/^ */   /' || true
wait
created=$(( $(sql ordering "select count(*) from orders") - total0 ))

backlog=$(docker compose exec -T rabbitmq rabbitmqctl list_queues name messages -q | awk '$1 !~ /dlq|retry|^name$/ { s += $2 } END { print s }')
echo "   messages still queued when the load stopped: $backlog"
while true; do
  read -r inv rec rdy <<< "$(progress)"
  (( inv - inv0 >= created && rec - rec0 >= created && rdy - rdy0 >= created )) && break
  sleep 2
done
t1=$(now)

python3 - "$created" "$t0" "$t1" "$stats" <<'EOF'
import re, sys, collections
n, t0, t1 = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
print(f"== all {n} orders invoiced and both e-mails recorded {t1 - t0:.0f} s after the load started")
per_round = collections.defaultdict(lambda: collections.defaultdict(float))
for line in open(sys.argv[4]):
    round_no, name, cpu = line.split()
    if not name.startswith('order-processing-'):
        continue
    service = re.sub(r'^order-processing-|-\d+$', '', name)
    per_round[service][round_no] += float(cpu.rstrip('%'))
print("   average CPU during the load (100% = one core):")
for service in sorted(per_round, key=lambda s: -sum(per_round[s].values())):
    rounds = per_round[service]
    print(f"     {re.sub(r'-run-.*', '', service):<20} {sum(rounds.values()) / len(rounds):6.0f}%")
EOF
rm -f "$stats"
