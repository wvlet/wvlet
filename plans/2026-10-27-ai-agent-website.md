# Make wvlet.org/wvlet readable by AI agents and AI search

## Goals

- AI coding agents are becoming Wvlet's main authors of queries. They pick a language when their model already knows it, or when a search or doc fetch at task time teaches them enough to write a correct query on the first try. Today the site gives them neither a compact entry point nor machine-readable docs.
- Ship track 1 of the AI-first marketing plan (`wvlet/uni` `plans/2026-10-27-ai-first-marketing.md`) for Wvlet, mirroring wvlet/uni#755 with Docusaurus equivalents.
- Keep every artifact generated from the docs we already write, so it does not drift.

## Background

- The site is Docusaurus 3.10 under `website/`, served at `https://wvlet.org/wvlet/` by `.github/workflows/website.yml` (already checks out with `fetch-depth: 0`).
- Uni used `vitepress-plugin-llms`; the Docusaurus counterpart is `docusaurus-plugin-llms` (rachfop), which generates `llms.txt`, `llms-full.txt`, and per-page `.md` copies.
- Some docs are MDX (`import Tabs ...`, `<Tabs>`); a plugin that copies sources verbatim leaks these into the Markdown output, the same class of problem as Uni's `__UNI_VERSION__` tokens.
- `robots.txt` is only read at the `wvlet.org` root, served by `wvlet/wvlet.github.io`; that is a separate follow-up.
- The release version is in `vscode-wvlet/package.json` (bumped in every release commit), which gives the JSON-LD a version without a manual edit.

## Design

**Lane: Standard** — adds a new user-facing page and site-wide head tags. The user pre-approved the direction in the launch prompt; the prototype gate on a local preview of the built site still applies.

1. **llms.txt** — add `docusaurus-plugin-llms@0.6.1` (exact pin; released 2026-10-03) with `generateLLMsTxt`, `generateLLMsFullTxt`, and `generateMarkdownFiles`. Exclude the blog and contributor-only `development/` pages from `llms-full.txt` if the file gets large; list the AI-agent page first. After build, check that no MDX `import` lines or JSX tags remain in the generated Markdown; strip them in a small `postBuild` step if the plugin does not.
2. **Search signals** in `docusaurus.config.ts`:
   - Sitemap from the classic preset with `lastmod: 'date'` (git-based; CI already fetches full history), plus `showLastUpdateTime` so Docusaurus reads git dates.
   - Canonical URLs: Docusaurus already emits `<link rel="canonical">` per page; verify in the built HTML, set `trailingSlash` only if needed.
   - Site-wide JSON-LD (`SoftwareApplication` + `SoftwareSourceCode`: name, description, repo, Apache-2.0, version from `vscode-wvlet/package.json`, programming language, OS) via `headTags`.
   - `description` frontmatter on key pages: intro, syntax index, quick start, install, DuckDB, Trino, CLI, and the new page.
3. **New page `docs/for-ai-agents.md`** ("Wvlet for AI Agents"), answer-first:
   - When to choose Wvlet (and when plain SQL is fine).
   - Why agents edit it well: flow-style queries are line-local and append-only.
   - Compact cheat sheet of flow operators; SQL→Wvlet mapping table.
   - How to run: `wv` REPL, `wvlet run` / `wvlet compile`, DuckDB default, Trino profile.
   - Common mistakes agents make (e.g., `select` placement, string quoting, `=` vs `==`, `group by` then `agg`).
   - Every syntax claim checked against the parser and `spec/` tests in this repo.
4. **Links** — navbar item, sidebar position near the top, README links to the page and `https://wvlet.org/wvlet/llms.txt`.
5. **Agent rules** — extend `AGENTS.md` (and a one-line pointer in `CLAUDE.md`): when syntax or CLI changes, update `for-ai-agents.md` in the same PR; llms files are generated; new pages get a `description`.
6. **PR body only** — suggest GitHub topics/description for `wvlet/wvlet`; do not apply.

## Alternatives and Why Not?

- **`@signalwire/docusaurus-plugin-llms-txt`**: converts rendered HTML (no MDX leakage), but the stable 1.x line has been idle and 2.x is alpha. `docusaurus-plugin-llms` is actively released and its Markdown output is closer to the sources.
- **Hand-written `llms.txt`**: drifts within weeks.
- **Swizzling `Root` for JSON-LD**: heavier than a static `headTags` entry for site-wide data.
