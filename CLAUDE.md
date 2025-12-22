# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Wvlet is a cross-SQL flow-style query language for functional data modeling and interactive data exploration. It compiles .wv query files into SQL for various database engines (DuckDB, Trino, Snowflake, Hive). The project consists of a language compiler, runtime system, web UI, and multi-platform bindings.

## Key Development Commands

### Building and Installing
```bash
# Enter SBT shell
./sbt

# Install wvlet CLI command to ~/local/bin/wv
sbt:wvlet> cli/packInstall

# Build native library (requires clang, llvm, libgc)
sbt:wvlet> wvcLib/nativeLink

# Build standalone native compiler
sbt:wvlet> wvc/nativeLink
```

Note: The `wv` command is implemented in WvletREPLMain, while `wvlet` is implemented in WvletMain.

### Code Formatting

Ensure the code is formatted with `scalafmtAll` command for consistent code style. CI will check formatting on pull requests.

```bash
# Format code
./sbt scalafmtAll

# Check formatting
./sbt scalafmtCheck
```

### Testing
```bash
# Run all tests
./sbt test

# Run specific module tests
./sbt "runner/test"
./sbt "langJVM/test"

# Test specific module for Scala.js
./sbt "langJS/test"

# Compile all projects for individual platforms
./sbt "projectJVM/Test/compile"
./sbt "projectJS/Test/compile"
./sbt "projectNative/Test/compile"

# Parsing test for a specific Wvlet in spec/sql/basic folder:
./sbt "langJVM/testOnly *ParserSpecBasic -- spec:basic:query.sql"

# Parsing test for a specific SQL in spec/sql/basic folder:
./sbt "langJVM/testOnly *ParserSpecSqlBasic -- spec:sql:basic:query.sql"

# Run test spec queries in spec/basic folder 
./sbt "runner/testOnly *RunnerSpecBasic"

# Run a specific .wv spec file 
./sbt "runner/testOnly *RunnerSpecBasic -- spec:basic:hello.wv"

# Run a specific .wv spec files with wild card pattern
./sbt "runner/testOnly *RunnerSpecBasic -- spec:basic:query*.wv"

# Run a specific .sql spec file 
./sbt "runner/testOnly *RunnerSpecSqlBasic -- spec:sql:basic:query.sql"

# Run test and stay in SBT shell
./sbt
sbt:wvlet> test
sbt:wvlet> runner/test
sbt:wvlet> testOnly *SpecRunner*

# Test native library with various languages
cd wvc-lib && make test
```

### UI Development
```bash
# Start main UI development server
npm run ui

# Start playground development server
npm run playground

# Build UI for production
npm run build-ui
npm run build-playground
```

### Documentation
```bash
# Start documentation server at localhost:3000
cd website && npm start

# Build documentation
cd website && npm run build
```

## Architecture Overview

### Multi-Module SBT Structure
- **Core Language**: `wvlet-lang` (compiler, parser, analyzer), `wvlet-api` (cross-platform APIs)
- **Execution**: `wvlet-runner` (query executor with DB connectors), `wvlet-cli` (command-line interface)
- **Web Stack**: `wvlet-server` (HTTP API), `wvlet-ui*` (React/Scala.js components)
- **Multi-Platform**: JVM, JavaScript (Scala.js), Native (Scala Native) support
- **Language Bindings**: `wvc-lib` for C/C++/Rust integration

### Compiler Pipeline
1. **Parser**: Wvlet syntax (.wv files) → AST using custom parser combinators
2. **Analyzer**: Type resolution, symbol resolution, dependency analysis
3. **Code Generator**: AST → SQL for target database engines
4. **Runner**: SQL execution via database-specific connectors

### Database Connectors
- **DuckDB**: Default for testing and lightweight execution
- **Trino**: Production distributed query engine
- **Snowflake**: Cloud data warehouse via JDBC
- **Delta Lake**: Support for Delta table format

## Testing Framework

- Uses AirSpec testing framework https://wvlet.org/airspec/
- Test files end with `Test.scala` or `Spec.scala`
- Avoid using mock as it increases maintenance cost and creates brittle tests that break when internal implementation changes
- Ensure tests cover new functionality and bug fixes with good test coverage
- Test names should be concise and descriptive, written in plain English
    - Good: `"should parse JSON with nested objects"`, `"should handle connection timeout gracefully"`
    - Avoid: `"testParseJSON"`, `"test1"`, `"shouldWork"`


### Spec-Driven Testing

The project uses a unique **spec-driven testing approach** where `.wv` files in `spec/` directory serve as executable test cases:

- `spec/basic/`: Core functionality tests (.wv files)
- `spec/tpch/`: TPC-H benchmark queries
- `spec/neg/`: Negative test cases (expect compilation/execution errors)
- `spec/cdp_*/`: Customer Data Platform behavior tests

For testing SqlParser, use `spec/sql/basic` directory:
- `spec/sql/basic`: SQL-parser tests (.sql files) 
- `spec/sql/tpc-h`: TPC-H benchmark queries

In SQL specs, use `VALUES` expressions to avoid referencing non-existing tables, as these specs are tested against DuckDB.

- **Embedded Assertions**: `.wv` files contain `test` statements for validation
- **SpecRunner**: Core engine that compiles and executes .wv files as test cases


### Test Assertions in .wv Files
```wv
from 'data.json'
test _.size should be 10
test _.columns should contain 'user_id'
test _.output should be """
| user_id | name     |
|---------|----------|
| 1       | Alice    |
"""
```

## Development Patterns

### File Organization
- `.wv` files: Wvlet query language source files
- Scala sources: Follow standard Maven/SBT directory structure
- Cross-platform code: Use `%%%` for multi-platform dependencies
- **Platform specific code needs to be placed in .jvm/src/main/scala, .js/src/main/scala, .native/src/main/scala folders**

### Code Style
- **Scala 3**: Latest Scala version (check `SCALA_VERSION` file). No Scala 2 support needed.
- For cross-platform projects, use .jvm, .js, and .native folders for platform-specific code
- Omit `new` for object instantiation (e.g., `StringBuilder()` instead of `new StringBuilder()`)
- Always enclose expressions in string interpolation with brackets: `${...}`
- Document public APIs (classes, methods, objects) with [Scaladoc comments](https://docs.scala-lang.org/style/scaladoc.html)
- Avoid returning Try[A] as it forces monadic-style usage
- Configuration case classes should have `withXXX(...)` methods for all fields and `noXXX(...)` methods for optional fields
    - Example: `case class Config(host: String, port: Int, timeout: Option[Duration])` should have:
        - `def withHost(host: String): Config = copy(host = host)`
        - `def withPort(port: Int): Config = copy(port = port)`
        - `def withTimeout(timeout: Duration): Config = copy(timeout = Some(timeout))`
        - `def noTimeout(): Config = copy(timeout = None)`

### Multi-Platform Considerations
- Use `%%%` for cross-platform library dependencies (JVM/JS/Native)
- To add platform-specific code, use XXXCompat trait, e.g., IOCompat. IOCompat consumes file I/O differences between Scala.js, which has no file I/O support, and others. No need to support file I/O in Scala.js code.
- Native builds require specific C library dependencies
- **In Scala.js code, avoid using Java-specific libraries**
- Platform-specific implementations:
  - Shared trait in `src/main/scala`: `trait FileIOCompat`
  - JVM implementation in `.jvm/src/main/scala`: `trait FileIOCompatImpl extends FileIOCompat`
  - JS implementation in `.js/src/main/scala`: `trait FileIOCompatImpl extends FileIOCompat`
  - Native implementation in `.native/src/main/scala`: `trait FileIOCompatImpl extends FileIOCompat`
  - Shared object: `object Compat extends FileIOCompatImpl`

### Performance
- Parser uses efficient TokenBuffer for lookahead
- Compiler phases are designed for incremental compilation
- DuckDB used for fast test execution

## Environment Requirements

- **JDK**: Minimum JDK 17, JDK 25+ required for Trino connector testing
- **Native Builds**: clang, llvm, libstdc++-12-dev, libgc (Boehm GC)
- **Node.js**: 22+ for UI development and documentation
- **SBT**: 1.11.1 (specified in project/build.properties)

## Release Process

The project follows semantic versioning and uses SBT plugins for cross-platform publishing. Check `project/release.rb` for release automation scripts.

## Git & Development Workflow

### Development Process
- **Before addressing a new task, switch to main and pull, and then create a new branch**
- Create a new branch and create a pull request for code development
- In git worktree environment, create a new branch based on origin/main
- **Before making changes, always create a new branch for pull request**

### Branching
- Create descriptive branches with timestamp for uniqueness
- Pattern: `<prefix>/<description>-$(date +"%Y%m%d_%H%M%S")` or `<prefix>/$(date +"%Y%m%d_%H%M%S")-<description>`
- Use appropriate prefixes: `feature/`, `fix/`, `doc/`, `internal/`
- Examples:
  - `feature/add-claude-guidance-20250605_205837`
  - `fix/20250605_210000-correct-sbt-syntax`
  - `doc/improve-testing-docs-20250605_210100`

### Commit Messages
- Use prefixes: `feature` (new features), `fix` (bug fixes), `internal` (non-user facing), `doc` (documentation)
- Focus on "why" rather than "what" or "how"
- Good example: `feature: Add XXX to improve user experience`
- Avoid: `feature: Add XXX class`
- **Include CLAUDE.md changes as needed to the commit**
  - If modifying project structure, development processes, or adding new guidelines, update this file
  - Ensure the guidance remains clear, concise, and helpful for developers

### Pull Requests
- Use [`gh pr create`](https://cli.github.com/manual/gh_pr_create) with clear title and detailed body
- Follow .github/pull_request_template.md format
- Check PR status and fix issues like code format, compilation failure, test failures
- After merging PR, update the related issues to reflect completed and remaining tasks
- **After PR is merged, switch to main branch and pull to get latest changes**
- **NEVER merge a PR or enable auto-merge without explicit user approval**
  - After CI passes and reviews are addressed, ask the user before merging
  - Only run `gh pr merge --squash --auto` when the user explicitly requests it

### Code Reviews
- Gemini will review pull requests for code quality, adherence to guidelines, and test coverage. Reflect on feedback and make necessary changes
- After creating a PR, wait for review from Gemini for a while, and reflect on the suggestions, and update the PR
- To ask Gemini review the code change again, comment `/gemini review` to the pull request

### GitHub CLI Commands
- Read PR review comments: `gh api repos/OWNER/REPO/pulls/PR_NUMBER/comments`
- Check PR status: `gh pr status`
- View PR checks: `gh pr checks PR_NUMBER`

## Error Handling

For error reporting, use WvletLangException and StatusCode enum. If necessary error code is missing, add to StatusCode

## Deployment and Documentation

- For new features, update the documentation at website/docs folder

## Development Checklist
- Before commiting changes, confirm compilation passes for src/main, src/test, and Scala.js

## Memory
- For creating temporary files, use target folder, which will be ignored in git
- `vscode-wvlet` is VS Code plugin folder

## CI Optimization

### Python SDK Testing
- Quick tests run on every PR (~2 minutes)
- Full wheel tests run only on tags, PRs with `test-wheels` label, or weekly schedule
- Add `test-wheels` label for platform-specific changes

## Debugging
- To monitor debug logs, use `-l debug` option. For example, ./sbt "langJVM/testOnly *Test -- -l debug"

## Testing Notes
- Use `shouldContain "(keyword)"` for checking string fragment in AirSpec
- To debug SQL generator, add -L *GenSQL=trace to the test option

