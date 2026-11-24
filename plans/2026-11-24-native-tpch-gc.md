# Run TPC-H on Scala Native 0.5.13 and settle the GC choice

## Goals

- Show that the engine runs TPC-H on Scala Native with the default Immix GC, now that 0.5.13 fixes the multi-threaded Immix/Commix corruption (scala-native#5046, #5160) that 0.5.12 had.
- Compare Immix and Boehm (wall time, peak RSS) on one machine, and keep or drop Boehm based on the numbers.
- Make the run repeatable, so a future Scala Native bump can be checked the same way.

## Background

- uni 2026.3.9 and sbt-scala-native 0.5.13 already landed on main in #2365 (`project/plugin.sbt` and `build.sbt` pin `UNI_VERSION`; the `wvlet` resolver at repo.wvlet.org is already there). This PR has no version bump left to do.
- The only GC workaround in the repo is `wvcLib` (the `libwvlet` C library) linking with `GC.boehm`, added in #925 (2024) because Immix segfaulted inside a host process loading the DLL. That is a different problem from the 0.5.12 multi-thread bug. `wvc`, `projectNative/test`, and the engine use Immix today.
- CI installs libgc only because of `wvcLib` (`test.yml`, `native.yml`, `flake.nix`, docs).
- The engine runs scans, join probes, and aggregations on a fixed thread pool on Native too (#2210), so TPC-H is exactly the multi-threaded allocation pattern that broke 0.5.12.
- All TPC benchmarks (`EngineTPCHBench`, `EngineTPCDSBench`) are JVM tests. On Native the engine is reachable through `wvc run --target wvlet`.

## Design

Lane: Standard by content (the `wvcLib` GC switch touches packaging), run without a design gate because the launch prompt fixed the direction (remove GC workarounds when results support it). Launch prompt says do not merge, so the PR is left for review.

- Add a script that runs the TPC-H queries (`spec/tpch/q*.wv`) through a native `wvc` binary on the in-process engine, several rounds, and prints per-query wall time and the process's peak RSS (`/usr/bin/time -l` on macOS, `-v` on Linux). Data: SF1 Parquet from `target/tpch-sf1` (same as `EngineTPCHBench`), or the bundled tiny data. Tables are bound with generated `model` files, because the native CLI resolves `~/.wvlet` from the passwd entry and ignores `$HOME`.
- Build the binary in release-fast mode once per GC (`SCALANATIVE_GC=immix|boehm`), same commit, same machine, alternating rounds.
- Cap the Immix heap only if RSS shows the 0.5.13 over-growth. Outcome: Immix peaks at 981 MB against 583 MB for Boehm, but SF1 already OOMs at a 512 MB cap. The engine's live data scales with the input, so `wvc` gets no built-in cap, and `GC_MAXIMUM_HEAP_SIZE` is documented for users who want a bound.
- `wvcLib` keeps Boehm unless a DLL test (`wvc-lib` `make test`) passes with Immix; the #925 crash is about a foreign host thread model, which the TPC-H run does not cover. Outcome: main's Native workflow has failed since #2365. Windows cannot link Boehm's glue (`GC_least_plausible_heap_addr`), and zlib dropped from the Windows link. So `wvcLib` moves to Immix, zlib is linked explicitly for Windows, and libgc leaves the build deps. Windows, mac-arm64, and linux-arm64 pass. linux-x64's Rust host segfault is GC-independent: 0.5.13's `initLibrary` preallocates an OutOfMemoryError whose stack walk crashes in the host. That one is upstream.
- Update `design/platforms.md` and the build docs with the measured result and which GC each artifact uses.

## Alternatives and Why Not?

- **Port `EngineTPCHBench` to Native.** It drives DuckDB as an oracle through JDBC; Native has no JDBC. The CLI path exercises the real shipped binary instead.
- **Switch everything to Boehm.** Boehm is not the default, needs libgc at run time, and the bug it would dodge is fixed in 0.5.13.
