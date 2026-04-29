# Fix Scala Native Windows build: missing `localtime_r`

## Problem

The "Native" workflow's `Build native on Windows (x64)` job fails when linking
`wvc-lib`'s `wvlet.dll`:

```
f0ef958.ll.o : error LNK2019: unresolved external symbol localtime_r
              referenced in function _SM24wvlet.uni.log.LogSource$RE
wvlet.dll : fatal error LNK1120: 1 unresolved externals
clang++: error: linker command failed with exit code 1120
```

CI run: https://github.com/wvlet/wvlet/actions/runs/25133797459/job/73666762135

## Root cause

`org.wvlet.uni:uni:2026.1.6` ships
`uni-core/.native/src/main/scala/wvlet/uni/log/LogTimestampFormatter.scala`,
which calls `scala.scalanative.posix.time.localtime_r`. `localtime_r` is a
POSIX extension; MSVC's CRT only provides `localtime_s` (with reversed
argument order), so on Windows the extern fails to resolve at link time.
Scala Native 0.5.x's `posixlib` declares `localtime_r` as a plain `extern`
without a Windows shim, and `windowslib`'s `time.c` only exposes
`scalanative_localtime_s` etc. — not `localtime_r`. The bug therefore
surfaces in any Scala Native consumer of `uni-core` that builds on
Windows/MSVC. wvlet's own CI is the first place this is exercised because
`uni`'s CI doesn't run a Windows native job.

## Fix

Patch `uni-core/.native/src/main/scala/wvlet/uni/log/LogTimestampFormatter.scala`
in `wvlet/uni` so the `localtime_r`-based `strftime` path is gated to
non-Windows targets via `scala.scalanative.meta.LinktimeInfo.isWindows`.
On Windows, fall back to a pure-Scala UTC formatter that uses Howard
Hinnant's civil-from-days algorithm for correct leap-year handling.

Trade-off: the Windows path emits UTC (`Z`) instead of the system-local
offset that `strftime("%z")` would produce. This is acceptable for a
logger and matches what the JS path already does (`Date.toISOString()`).

`LinktimeInfo.isWindows` is a final boolean resolved at link time, so the
unused branch is dead-code-eliminated and POSIX builds keep their existing
`localtime_r` + `strftime` behavior unchanged.

## Steps

1. Land the fix in `wvlet/uni` (open PR there, get merged + released).
2. Bump `UNI_VERSION` in `wvlet/wvlet`'s `build.sbt` to the released patch.
3. Verify the `Build native on Windows (x64)` job goes green.

## Follow-ups

- Add a `windows-latest` Scala Native job to `uni`'s `test.yml` so this
  class of regression is caught at the source.
