// Shared helpers for the taper load harness (spec #44, T4).
//
// Tokens: the gateway verifier (pkg/auth) validates standard HS256 JWTs —
// base64url(header).base64url(payload).base64url(HMAC-SHA256(secret,
// "header.payload")) with claims {tenant_id, exp, iat, role}. k6 mints
// these directly via k6/crypto; a real IdP would replace this helper, not
// the scripts.
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';

const B64 = 'base64rawurl';

// mintToken returns an HS256 JWT for tenantID using the gateway secret.
// The role defaults to read-write, which covers the harness's order saga
// surface.
export function mintToken(tenantID, secret, role = 'read-write') {
  const header = encoding.b64encode(JSON.stringify({ alg: 'HS256', typ: 'JWT' }), B64);
  const payload = encoding.b64encode(
    JSON.stringify({
      tenant_id: tenantID,
      role: role,
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 3600,
    }),
    B64
  );
  const input = `${header}.${payload}`;
  const sig = crypto.hmac('sha256', secret, input, B64);
  return `${input}.${sig}`;
}

// headers returns the Authorization + JSON headers for a tenant.
export function headers(token) {
  return {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
  };
}

// env reads a config var with a default (k6 -e or env vars).
export function opt(name, def) {
  return __ENV[name] && __ENV[name] !== '' ? __ENV[name] : def;
}

// checkJSON runs resp.ok-style checks shared by the scenarios.
export function expectStatus(resp, status, name) {
  const ok = resp.status === status;
  if (!ok) {
    // Surface the failure body once per unique status for debugging.
    console.warn(`${name}: got ${resp.status}: ${resp.body.slice(0, 200)}`);
  }
  return ok;
}
