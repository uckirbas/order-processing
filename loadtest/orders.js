// Places orders through nginx at a target arrival rate. Open model: k6 keeps the rate even when the API slows
// down, so saturation shows up as latency and 503s instead of k6 quietly sending fewer requests.
//
//   docker compose run --rm k6 run /scripts/orders.js                      ramp to 8000/s, then hold
//   docker compose run --rm -e PROFILE=steady -e RATE=3000 -e DURATION=20s k6 run /scripts/orders.js
//
// Env: PROFILE (ramp | steady), RATE (orders/s), RAMP, HOLD, DURATION, CURRENCY, BASE_URL.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 8000);
const CURRENCY = __ENV.CURRENCY || 'TRY';

const scenarios = {
  ramp: {
    executor: 'ramping-arrival-rate',
    startRate: Math.max(1, Math.round(RATE / 10)),
    timeUnit: '1s',
    preAllocatedVUs: 500,
    maxVUs: 4000,
    stages: [
      { target: RATE, duration: __ENV.RAMP || '60s' },
      { target: RATE, duration: __ENV.HOLD || '60s' },
    ],
  },
  steady: {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration: __ENV.DURATION || '20s',
    preAllocatedVUs: 500,
    maxVUs: 4000,
  },
};

export const options = {
  scenarios: { orders: scenarios[__ENV.PROFILE || 'ramp'] },
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // Latency of accepted orders; shed requests (503) are fast by design and would flatter the numbers.
    'http_req_duration{status:201}': ['p(95)<250', 'p(99)<500'],
    orders_shed: ['count>=0'],
    orders_failed: ['count==0'],
  },
};

const created = new Counter('orders_created');
const shed = new Counter('orders_shed');
const failed = new Counter('orders_failed');

const products = [
  ['SKU-1', 'Kablosuz Kulaklık', 1499.9],
  ['SKU-2', 'Şarj Kablosu', 149.5],
  ['SKU-3', 'Telefon Kılıfı', 249.0],
  ['SKU-4', 'Powerbank 20000 mAh', 899.99],
];

export default function () {
  const lines = 1 + Math.floor(Math.random() * 3);
  const items = products.slice(0, lines).map(([productId, productName, unitPrice]) => ({
    productId,
    productName,
    quantity: 1 + Math.floor(Math.random() * 3),
    unitPrice,
  }));

  const response = http.post(
    `${BASE_URL}/orders`,
    JSON.stringify({
      customerId: crypto.randomUUID(),
      customerEmail: `load-${crypto.randomUUID()}@example.com`,
      currency: CURRENCY,
      items,
    }),
    {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
      // 503 is the API shedding load on purpose: counted separately, not as a failure.
      responseCallback: http.expectedStatuses(201, 503),
    },
  );

  if (response.status === 201) created.add(1);
  else if (response.status === 503) shed.add(1);
  else failed.add(1);

  check(response, { 'accepted (201) or shed (503)': (r) => r.status === 201 || r.status === 503 });
}
