# ADR: Modules resolve from local folders, not remote repositories

Date: 2026-11-25 · PR #2432

## Context

- The parser accepted `import example.* from "github.com/..."`, but the compiler ignored the source string, and `VirtualFile.GitHubArchive` was an unimplemented stub (`???`). The docs said GitHub imports were "not supported yet", which read as a plan.
- Most data pipelines live in one repository. The main case for sharing, one CDP pipeline template run for each tenant, differs by settings (database names, schema mappings, thresholds), which model parameters, `extends` / `like` table shapes, partial queries, and profiles already cover in one repository.
- dbt packages are popular largely because Jinja macros make up for SQL's lack of abstraction. Wvlet's typed models and functions remove much of that need.
- Fetching remote code needs version pinning and a lockfile, login for private repositories (CDP templates are usually private), caching and offline builds, resolving version conflicts, and trust in compiled third-party code. Putting a hosting URL in `import` also ties query code to one host.

## Decision

- Remove the `from "..."` clause of `import` and the `GitHubArchive` stub. The parser rejects `import ... from "..."` with a syntax error instead of reading `from "..."` as the start of a query.
- Modules resolve from the local project folder. Projects share code by keeping it in one repository or by vendoring it (a copy or a git submodule).

## Consequences

- `import` names a package only, so a query reads the same wherever its module lives.
- If cross-repository sharing becomes a real need, add it in steps without changing `import`: first a module search path in project config (`dependencies: [ path: ../cdp-common ]`), then, if users ask, a `git:` source pinned to a ref and downloaded into a cache. Widely shared models can ship packaged, as the stdlib does.
