# Repository Guidelines

## Project Structure & Modules
- Root SBT multi-project for Scala 3; UI pieces use npm workspaces.
- Scala modules: `wvlet-api`, `wvlet-lang`, `wvlet-runner`, `wvlet-client`, `wvlet-server`, `wvlet-cli`, `wvlet-spec` (tests).
- UI/Tools: `wvlet-ui`, `wvlet-ui-main`, `wvlet-ui-playground`, `vscode-wvlet`, `prismjs-wvlet`, `highlightjs-wvlet`, `website`.
- Test data and examples: `spec/` and `wvlet-stdlib/` (`.wv` files). Build logic in `build.sbt`, SBT plugins in `project/`.

## Build, Test, and Dev Commands
- Compile all: `./sbt compile`
- Run tests (all projects): `./sbt test`
- Format code: `./sbt scalafmtAll`
- Run server (dev): `./sbt server/reStart` (stop with `server/reStop`).
- Package CLI: `./sbt cli/pack` (faster: `./sbt cli/packQuick`). Binaries under `wvlet-cli/target/pack/bin/`.
- UI dev: `npm run ui` (runs `wvlet-ui-main` via Vite). Playground: `npm run playground`. Build UI: `npm run build-ui`.

## Coding Style & Naming
- Language: Scala 3 (new syntax). Auto-format with Scalafmt (`.scalafmt.conf`, 100 col limit). Run `scalafmtAll` before commits and PRs.
- Indentation: default Scalafmt (spaces). Use CamelCase for types/objects; lowerCamelCase for vals/defs; test classes end with `*Test` or `*Spec`.
- JS/TS: Vite + TypeScript in UI workspaces; keep modules ESModule style.

## Testing Guidelines
- Framework: AirSpec (`wvlet.airspec.Framework`) via SBT. Place tests in `*/src/test/scala/...`.
- Run: `./sbt test`, or per project (e.g., `./sbt wvlet-runner/test`).
- Coverage: keep meaningful assertions; include `.wv` samples in `spec/` when relevant.
- Some JS packages use Jest; run in that package (e.g., `cd prismjs-wvlet && npm test`).
  - Single test examples:
    - Class: `./sbt "runner/testOnly *BasicSpec"`
    - Specific .wv: `./sbt "runner/testOnly *BasicSpec -- spec:basic:hello.wv"`
    - Wildcard: `./sbt "runner/testOnly *BasicSpec -- spec:basic:query*.wv"`
  - JVM-specific module:
    - Run a single JVM test class in `wvlet-lang`: `./sbt "langJVM/testOnly *SqlParserTest"`
    - Note: keep quotes to prevent shell globbing of `*`.
  - Enable debug logs:
    - Append `-- -l debug` to test commands to see detailed logs.
    - Example: `./sbt "runner/testOnly *BasicSpec -- -l debug"`

## Commit & Pull Requests
- Commit style: conventional prefixes seen in history (e.g., `fix: ...`, `feature: ...`, `build(deps): ...`, `docs:`). Use present tense, concise scope.
- PRs must include: clear description, linked issues (`Fixes #123`), and screenshots/GIFs for UI changes.
- Before committing/opening: run `./sbt scalafmtAll` (or `scalafmtCheck`) and `./sbt compile test`; for UI changes, also `npm run build-ui`.
- Optional pre-commit: `echo '#!/bin/sh\n./sbt -batch scalafmtCheck || { echo "Run ./sbt scalafmtAll"; exit 1; }' > .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit`

## Security & Configuration
- Do not commit credentials. Use environment variables and reference them in `profile.yml` as `${VAR_NAME}`.
- Prefer local `.env` or shell exports during development; document required vars in PRs.
