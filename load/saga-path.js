// Scenario B (spec #44, T4): full saga path through the REST gateway —
// POST /v1/orders (payment sandbox + reservation + allocation) with a
// follow-up GET asserting CONFIRMED. The hard path: DB saga, gRPC fan-out,
// outbox CDC. No-oversell under load is asserted by scripts/audit-oversell.sql.
//
// Run:
//   k6 run -e GATEWAY=http://localhost:8080 -e SECRET=load-secret \
//     -e TENANT=<uuid> load/saga-path.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { mintToken, headers, opt, expectStatus } from './lib.js';

export const options = {
  scenarios: {
    saga: {
      executor: 'constant-arrival-rate',
      rate: Number(opt('RATE', '100')), // sagas per second
      timeUnit: '1s',
      duration: opt('DURATION', '60s'),
      preAllocatedVUs: 50,
      maxVUs: 300,
    },
  },
  thresholds: {
    http_req_duration: ['p(50)<50', 'p(95)<100', 'p(99)<100'],
    http_req_failed: ['rate<0.01'],
  },
};

const tenant = opt('TENANT', '11111111-1111-4111-8111-111111111111');
const token = mintToken(tenant, opt('SECRET', 'load-secret'));
const base = opt('GATEWAY', 'http://localhost:8080');
const H = headers(token);
// SPREAD_SKUS>1 distributes load across seeded SKUs (realistic traffic);
// SPREAD_SKUS=1 concentrates on LOAD-SKU for the contention ceiling mode.
const spread = Number(opt('SPREAD_SKUS', '50'));
const skuFor = () => (spread > 1 ? `LOAD-SKU-${__VU % spread}` : 'LOAD-SKU');

export default function () {
  const orderId = `saga-load-${__VU}-${__ITER}`;
  const create = http.post(
    `${base}/v1/orders`,
    JSON.stringify({
      orderId,
      lines: [{ skuId: skuFor(), warehouseId: 'W1', quantity: 1, unitPrice: 100 }],
    }),
    H
  );
  const created = check(create, {
    'create 2xx/4xx-domain': (r) =>
      expectStatus(r, 200, 'create') || r.status === 409 || r.status === 400,
  });

  // Confirm the terminal state on the happy path. CONFIRMED arrives via
  // outbox -> Debezium -> Kafka -> fulfillment consumer, so poll like a
  // real client instead of racing the pipeline with one immediate GET.
  if (created && create.status === 200) {
    let confirmed = false;
    for (let i = 0; i < 10 && !confirmed; i++) {
      const get = http.get(`${base}/v1/orders/${orderId}`, H);
      if (get.status === 200) {
        try {
          confirmed = JSON.parse(get.body).sagaState === 'CONFIRMED';
        } catch {
          confirmed = false;
        }
      }
      if (!confirmed) sleep(0.5);
    }
    check({ confirmed }, {
      'saga CONFIRMED': (s) => s.confirmed,
    });
  }
  sleep(0.01);
}
