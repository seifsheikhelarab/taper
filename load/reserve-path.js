// Scenario A (spec #44, T4): direct reservation path through the REST
// gateway — POST /v1/stock/reserve at sustained load on a small hot set.
// Latency thresholds guard the spec's sub-100ms p99 goal; correctness is
// asserted separately by scripts/audit-oversell.sql after the run.
//
// Setup: gateway running with a raised load-profile limit, e.g.
//   GATEWAY_RATE_PER_TENANT=500 GATEWAY_BURST_PER_TENANT=1000 \
//   GATEWAY_JWT_SECRET=load-secret go run ./cmd/gateway
// Run:
//   k6 run -e GATEWAY=http://localhost:8080 -e SECRET=load-secret \
//     -e TENANT=<uuid> load/reserve-path.js
// Seed SPREAD_SKUS distinct SKUs (LOAD-SKU-0 .. LOAD-SKU-<n-1>) or set
// SPREAD_SKUS=1 for the single-row contention mode.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { mintToken, headers, opt, expectStatus } from './lib.js';

export const options = {
  scenarios: {
    reserve: {
      executor: 'constant-arrival-rate',
      rate: Number(opt('RATE', '200')), // requests per second
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
// SPREAD_SKUS>1 distributes load across that many seeded SKUs (realistic
// traffic). SPREAD_SKUS=1 concentrates every request on LOAD-SKU, turning
// the run into a deliberate single-row contention ceiling measurement.
const spread = Number(opt('SPREAD_SKUS', '50'));
const skuFor = () => (spread > 1 ? `LOAD-SKU-${__VU % spread}` : 'LOAD-SKU');

export default function () {
  const orderId = `load-${__VU}-${__ITER}`;
  const res = http.post(
    `${base}/v1/stock/reserve`,
    JSON.stringify({
      orderId,
      lines: [{ skuId: skuFor(), warehouseId: 'W1', quantity: 1 }],
    }),
    headers(token)
  );
  check(res, {
    'reserve 2xx/4xx-domain': (r) =>
      expectStatus(r, 200, 'reserve') || r.status === 409 || r.status === 400,
  });
  sleep(0.01);
}
