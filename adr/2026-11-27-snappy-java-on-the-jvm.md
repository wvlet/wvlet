# ADR: Decode Snappy with snappy-java on the JVM

Date: 2026-11-27 · PR #2535 (uni 2026.3.18, wvlet/uni#1212)

## Context

- Snappy is 27–31% of engine CPU on TPC-H SF10 q6, q17, and q21. uni's pure decoder reaches about 2/3 of snappy-java's speed on the JVM (page decode 2.2–2.4 vs 3.5 GB/s, wvlet/uni#1212), and uni's 2026-11-24 ADR found C2 can't close that gap.
- [2026-11-24-engine-parquet-decode-pure-scala](2026-11-24-engine-parquet-decode-pure-scala.md) kept decode pure Scala everywhere to avoid a native library in the engine and the CLI.
- uni 2026.3.18 uses snappy-java's native decoder when snappy-java is on the classpath and its native library loads, and declares it as `Provided`, so applications opt in.

## Decision

- `engine.jvm` depends on `org.xerial.snappy:snappy-java`, so the runner, server, and both CLI packs decode Snappy pages natively. This supersedes the 2026-11-24 ADR's first decision for the JVM only.
- Scala.js and Scala Native keep uni's pure-Scala decoder; uni ships a stub there.
- `-Dwvlet.uni.snappy.useSnappyJava=false` keeps the pure decoder, as does a platform with no snappy-java native build, so the dependency can't break a scan.

## Consequences

- The JVM CLI carries one native library (snappy-java bundles builds for the common OS/CPU pairs). JDK 25 prints a restricted-method warning unless `--enable-native-access=ALL-UNNAMED` is set, which the CLI launchers already pass.
- uni reads the flag once at class load, so the SF10 A/B runs alternating JVMs (on/off) rather than a one-JVM toggle. Results are recorded on PR #2535.
- `EngineSnappyJavaTest` fails if the JVM engine silently falls back to the pure decoder.
