# ADR: Homebrew is the recommended wvlet install, and npm is an alternative

Date: 2026-11-27

## Context

- [adr/2026-11-26-distribution-roles.md](2026-11-26-distribution-roles.md) made npm the default install, because the JVM CLI needed a JDK.
- The JVM CLI now ships with a bundled jlink Java runtime (#2538), and the Homebrew formula moves to it, so Homebrew users no longer set up a JDK.
- The JVM CLI is the complete `wvlet`: `ui`, flows, `catalog`, JDBC connectors, and parallel builds. npm lacks these.

## Decision

- **Homebrew is recommended.** `install.md`, `for-ai-agents.md`, and the bundled skill list `brew install wvlet/wvlet/wvlet` first, then the release packages for platforms without Homebrew.
- **npm is an alternative** for machines with Node.js but no Homebrew. Its JVM-only-command message links to the Homebrew section of the install page.
- **The install page doesn't feature DuckDB.** The full package with the DuckDB and Snowflake connectors is listed once under release packages; the engine runs queries with no database.

## Consequences

- This supersedes the "npm is the default" decision of adr/2026-11-26-distribution-roles.md. Its other decisions (no Native distribution, JVM-only commands named on npm) stand.
