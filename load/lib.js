// Shared helpers for the taper load harness (spec #44, T4).
//
// Tokens: the gateway's sandbox verifier (pkg/auth) signs
// base64url(payload) + "." + base64url(HMAC-SHA256(secret, body)) with
// claims {"tenant_id","exp"}. k6 mints these directly via k6/crypto; a
// real IdP would replace this helper, not the scripts.
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';

const B64 = 'base64rawurl';

// mintToken returns a sandbox token for tenantID using the gateway secret.
export function mintToken(tenantID, secret) {
  const claims = JSON.stringify({
    tenant_id: tenantID,
    exp: Math.floor(Date.now() / 1000) + 3600,
  });
  const body = encoding.b64encode(claims, B64);
  // Sign the base64 body as text bytes (matching Go's mac.Write([]byte(body)))
  // and emit base64rawurl directly — no intermediate byte arrays.
  const sig = crypto.hmac('sha256', secret, body, B64);
  return `${body}.${sig}`;
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
