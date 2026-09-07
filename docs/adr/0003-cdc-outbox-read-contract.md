# Debezium CDC requires an explicit taper_cdc read contract on outbox tables

Debezium connectors snapshot `public.outbox` as `taper_cdc` using plain
`SELECT`s before switching to WAL streaming, but the role had no table
privileges and outbox tenant-RLS applied to every role — so fresh stacks
produced connectors that report RUNNING while streaming nothing (snapshot
stuck retrying `permission denied`, or silently reading zero rows). The
long-running local stack masked this because manual grants from earlier
debugging were never captured in migrations. Decision: every service
migration ships the full CDC read contract for its outbox table —
`GRANT SELECT ON outbox TO taper_cdc`, the tenant-isolation policy scoped
`TO taper_app`, and an explicit permissive `outbox_cdc_read_policy TO
taper_cdc USING (true)` — and `TestCDCRoleCanReadOutbox` canaries the
contract in CI. RLS is default-deny for roles matching no policy, which is
why scoping the tenant policy alone is insufficient: permissive policies OR
together, so CDC needs its own. The replication slots use the `decoderbufs`
plugin, which never touches publications; anything citing
`publication.autocreate.mode` as the failure mode is misdiagnosing.

## Considered Options

- **Superuser CDC role** — rejected: violates the least-privilege principle
  the role system exists for, and would silently bypass tenant RLS on every
  table, not just the outbox.
- **Tenant-scoped policy only, CDC excluded** — rejected: default-deny means
  the CDC role sees zero rows; the snapshot comes back empty and the
  connector streams nothing, the exact outage this ADR closes.
- **BYPASSRLS on the CDC role** — rejected: conflates the maintenance
  role's scope (`taper_sweeper`) with the CDC role's; bypass hides future
  RLS mistakes on CDC reads instead of surfacing them as policy questions.

## Consequences

- New services must copy all three parts of the contract (grant, scoped
  tenant policy, CDC read policy) or CI streaming tests will fail on fresh
  stacks only — the canary test makes that failure fast and local.
- The contract is enforced per migration, not globally; a database created
  outside the migrations path will not have it.
