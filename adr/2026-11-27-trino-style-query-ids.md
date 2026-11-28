# ADR: Trino-style query ids

Date: 2026-11-27 · PR #2572

## Context

The server issued a ULID per query. ULIDs (and UUIDv7s) put the timestamp first and the random part last, so queries created close together share their first 8–10 characters and people can't tell them apart at a glance. More than one server will issue ids later.

## Decision

Issue `yyyyMMdd_HHmmss_<seq>_<server>` ids, as Trino does, e.g. `20261127_155437_00042_x7k2p`: UTC creation time to the second, a 5-digit per-process sequence, and a 5-character random base-32 server instance id chosen at startup. The API carries them as `String`.

## Consequences

- Ids sort by time, show when a query ran, and neighbors differ at a fixed position.
- The server id keeps ids of different servers, and of one server across restarts, apart without a persisted counter.
- `QueryInfo`, `QueryResponse`, `QueryInfoRequest`, and `QueryCancelRequest` changed from `ULID` to `String`; old clients can't talk to a new server. Request ids stay ULIDs (client-generated).

## Alternatives

- Random UUIDv4 (ClickHouse, Athena): differs from the first character but does not sort by time and is long to quote.
- A persisted integer (Treasure Data jobs, Redshift): easiest to quote, but multiple servers need a shared counter.
- A `QueryId` value class: uni's router encodes return values with runtime weavers that ignore custom givens, so it would go over the wire as an object.
