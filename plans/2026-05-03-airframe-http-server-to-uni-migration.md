# Plan: Port airframe-http server-side into wvlet

## 0. Key finding that shapes the plan

`uni` already ships `wvlet.uni.http.*` (in `org.wvlet.uni:uni`):
`Http`, `Request`, `Response`, `HttpHeader`, `HttpStatus`, `HttpMethod`,
`HttpMultiMap`, `HttpFilter`, `HttpContext`, `ServerSentEvent`, plus
`wvlet.uni.http.router.{Router, Route, RouteMatcher, Endpoint, ControllerProvider, HttpRequestMapper, ResponseConverter}`,
`wvlet.uni.http.rpc.{RPCStatus, RPCException, RPCRoute, RPCRouter, RPCClient, MethodCodec}`,
plus `wvlet.uni.http.codegen.*` and an `sbt-uni-codegen` plugin.
Also: `wvlet.uni.design.{Design, Session}`, `wvlet.uni.surface.Surface`,
`wvlet.uni.util.{ULID, ElapsedTime}`.

Per the user's vendoring memo (`feedback_jdbc_codec_lives_in_wvlet.md`),
this migration **must not** push code into `uni` — code lives inside the
wvlet repo.

**Conclusion**: this is *not* a wholesale port of `airframe-http` core
types — uni already has them. What's missing is the **server runtime**:
the Netty backend, RxRouter→Router conversion path that wvlet uses,
`StaticContent`, the request-dispatcher pipeline, and HTTP access
logging. We port those into a new wvlet sub-module that depends on uni's
HTTP types.

## 1. Scope

### In scope (port into wvlet, ~10–14 files)

From `airframe-http-netty/src/main/scala/wvlet/airframe/http/netty/`
(1079 LOC, 7 files): all of `Netty.scala`, `NettyServer.scala`,
`NettyServerConfig`, `NettyBackend.scala`, `NettyRequestHandler.scala`,
`NettyResponseHandler.scala`, `NettyRPCContext.scala`,
`RxNettyBackend.scala`.

From `airframe-http/.jvm/src/main/scala/wvlet/airframe/http/`:
`Router.scala` (legacy router used by the dispatcher),
`StaticContent.scala`, `HttpAccessLogWriter.scala`; in `router/`:
`Automaton.scala`, `ControllerProvider.scala`,
`HttpEndpointExecutionContext.scala`, `HttpRequestDispatcher.scala`,
`HttpRequestMapper.scala`, `ResponseHandler.scala`, `Route.scala`,
`RouteMatcher.scala`; in `internal/`: `LogRotationHttpLogger.scala`,
`LocalRPCContext.scala`, `TLSSupport.scala`. (~2270 LOC total.)

From `airframe-http/src/main/scala/wvlet/airframe/http/`: only the bits
uni doesn't already cover and that the server runtime needs —
`RxRouter.scala`, `RxRouterProvider.scala`, `RxHttpBackend.scala`,
`RxHttpEndpoint.scala`, `RxHttpFilter.scala`, `HttpBackend.scala`,
`HttpServer.scala`, `HttpLogger.scala`, `RPCContext.scala`,
`RPCMethod.scala`, `RPCEncoding.scala`, `internal/{HttpLogs.scala,
HttpMultiMapCodec.scala, HttpResponseBodyCodec.scala,
RPCCallContext.scala, RPCResponseFilter.scala}`,
`router/{RedirectToRxEndpoint.scala, RxRoute.scala}`,
`filter/CorsFilter.scala`, plus the Scala-3 macro shim
`RxRouterBase.scala` / `RxRouterMacros.scala`. The RPC trait annotation
(`RPC.java`) and `Endpoint.java` may be substituted by uni equivalents
(`wvlet.uni.http.router.Endpoint` exists; verify whether uni ships an
`RPC` annotation — if not, port it).

### Out of scope, never to be ported

`airframe-http-finagle`, `airframe-http-grpc`, `airframe-http-okhttp`,
`airframe-http-recorder` — wvlet doesn't use them (zero hits).

### Ambiguous / decisions needed

- **RPC codegen.** wvlet uses `AirframeHttpPlugin` /
  `airframeHttpClients := Seq("wvlet.lang.api.v1.frontend:rpc:FrontendRPC")`
  in `wvlet-client`. uni already publishes `sbt-uni-codegen` and
  `uni-http-codegen`. Switch to those rather than re-vendoring
  `airframe-http-codegen` into wvlet. This is independent enough to do
  as its own phase.
- **`RPC` and `Endpoint` annotations.** `wvlet.airframe.http.RPC` and
  `wvlet.airframe.http.Endpoint` are referenced from `wvlet-api`.
  Confirm uni publishes equivalents (`wvlet.uni.http.router.Endpoint`
  is in the jar; need to verify `RPC`). If uni lacks `RPC`, it's small
  enough to vendor as a wvlet annotation under a chosen package.
- **`RxRouter` macros (Scala 3).** `RxRouter.of[T]` is macro-driven.
  Whether the macro ports cleanly is the highest-risk unknown — see
  Risks.
- **`HttpAccessLogWriter` + `LogRotationHttpLogger`.** Pull in alongside
  Netty server, even though wvlet doesn't directly reference them —
  `NettyServer` constructs them.

## 2. Dependency map

| Airframe dep used by ported code | Status |
|---|---|
| `wvlet.airframe.codec.*` (MessageCodec, MessageCodecFactory, PrimitiveCodec) | Stays as airframe dep initially. uni ships `wvlet.uni.weaver.codec.*` but it's a different API — full swap is its own future migration. Mark TODO. |
| `wvlet.airframe.surface.*` | uni has `wvlet.uni.surface.Surface` — but the airframe-http router code uses many internal surface APIs (`MethodSurface`, `Parameter`, `TypeName`, `Primitive`). Audit case by case; likely keep `airframe-surface` as a dep for ported router code in phase 1 and migrate to `uni.surface` later. |
| `wvlet.airframe.di.*` (`Design`, `Session`) | uni has `wvlet.uni.design.{Design, Session}` — direct swap. |
| `wvlet.airframe.rx.*` | Stays as airframe dep (uni doesn't yet ship Rx). RxRouter and Netty's Rx response handling depend on it. |
| `wvlet.airframe.control.*` (Control, Parallel, ThreadUtil) | uni has `wvlet.uni.control.{Control, IO}` and `wvlet.uni.util.ThreadUtil` — swap; `Parallel` may need to stay airframe for now. |
| `wvlet.airframe.json.*` | uni has `wvlet.uni.json.JSON` — swap. |
| `wvlet.airframe.msgpack.spi.*` | uni has `wvlet.uni.msgpack.spi.*` — swap. |
| `wvlet.airframe.ulid.ULID` | uni has `wvlet.uni.util.ULID` — swap. Crosses into `wvlet-api` types (`QueryRequest`, `QueryInfo`, `FrontendApi.QueryInfoRequest`). |
| `wvlet.airframe.metrics.{ElapsedTime, Count}` | uni has `wvlet.uni.util.{ElapsedTime, Count}` — swap. Also crosses into `wvlet-api` (`ServerStatus.upTime`). |
| `wvlet.airframe.launcher` | already swapped to `wvlet.uni.cli.launcher` in commit `48bab6fe`. |
| Netty | external library `io.netty:netty-all`, version pinned to `4.2.10.Final` in airframe — bring same pin into wvlet. |

## 3. Target structure in wvlet

New sub-module: `wvlet-http-server` (JVM-only project, mirroring
`wvlet-server`/`wvlet-runner` setup). All code under package
`wvlet.lang.http.server` to make ownership obvious. Sub-packages:

```
wvlet.lang.http.server         <- Netty.scala, NettyServer.scala, NettyServerConfig
wvlet.lang.http.server.netty   <- backend, request/response handlers, NettyRPCContext
wvlet.lang.http.server.router  <- Route, RouteMatcher, ControllerProvider,
                                   HttpRequestDispatcher, HttpRequestMapper,
                                   ResponseHandler, Automaton, RedirectToRxEndpoint, RxRoute
wvlet.lang.http.server.rx      <- RxRouter, RxRouterProvider, RxRouterBase, RxRouterMacros,
                                   RxHttpBackend, RxHttpEndpoint, RxHttpFilter
wvlet.lang.http.server.filter  <- CorsFilter
wvlet.lang.http.server.log     <- HttpLogger, HttpAccessLogWriter, LogRotationHttpLogger, HttpLogs
wvlet.lang.http.server.static  <- StaticContent
wvlet.lang.http.server.internal <- TLSSupport, RPCResponseFilter, RPCCallContext, codecs
```

Rationale for renaming `wvlet.airframe.http.*` →
`wvlet.lang.http.server.*`: (a) avoids package collision with both
`wvlet.airframe.http` (still on classpath via codecs) and
`wvlet.uni.http` (used for client types), (b) matches wvlet's existing
`wvlet.lang.*` namespace convention. The renamed code consumes
`wvlet.uni.http.{Request, Response, Http, ...}` types as its public
surface where possible.

## 4. Migration phases

Each phase ships independently with green builds.

**Phase 0 — wvlet-api wire types (smallest viable slice).** Replace
`wvlet.airframe.ulid.ULID` and `wvlet.airframe.metrics.ElapsedTime` in
`FrontendApi.scala`, `QueryRequest.scala`, `QueryInfo.scala`,
`FrontendApiImpl.scala` with `wvlet.uni.util.{ULID, ElapsedTime}`. Also
replace `wvlet.airframe.http.{RPC, RxRouter, RxRouterProvider}`
references in `FileApi.scala`/`FrontendApi.scala` only if uni
equivalents exist; otherwise defer to Phase 2. Verify
`client/test/compile` for cross-build (JVM/JS/Native). Codegen still
produces from the airframe-http surface here — only the leaf data types
change.

> **Phase 0 outcome (#1663, merged 2026-05-03).** Done. Key empirical
> finding: `airframe-codec` correctly round-trips
> `wvlet.uni.util.ULID` over the airframe-http RPC stack via its
> surface-derived generic codec, even though no built-in
> `MessageCodec[wvlet.uni.util.ULID]` exists. Initial concern (raised by
> Codex review) was that the generic-codec fallback would null-encode
> the private constructor field; `UlidRpcRoundTripTest` disproved this
> with a real Netty + `FrontendRPC` round-trip. This unblocks Phase 1
> without first having to migrate the codec layer. The test is pinned
> in `wvlet-server` and must remain green through Phase 4.
>
> Also touched (forced by the api-type change):
> `wvlet-server/.../FrontendApiImpl.scala`, `QueryService.scala`,
> `wvlet-ui-main/.../FileNav.scala`. Drop of `airframe-metrics` from
> `wvlet-api` deps was safe — wvlet-labs still pulls it transitively
> via `airframe-launcher`.
>
> **Test isolation gotcha.** AirSpec's per-test session scoping calls
> `close()` on `AutoCloseable` singletons between tests in the same
> spec. `QueryService` extends `AutoCloseable` and shuts down its
> thread pool on close, so a second test in the same spec that issues
> `submitQuery` hits a terminated executor. Future Phase 2+ tests that
> want to share a Netty boot across multiple test cases must either
> rebind `QueryService` per test or split into separate specs.

**Phase 1 — Create `wvlet-http-server` module, copy Netty + router code
verbatim under new package.** Add Netty dep. Wire it into the `server`
project (replacing `airframe-http-netty`). Adjust imports in copied
files only enough to compile (still consuming airframe-http types from
the still-present `airframe-http` library to avoid a big-bang). Output:
a wvlet module that produces an equivalent `NettyServer` symbol the
consumers can switch to. Build green; `wvlet-server` still uses
airframe-http.

> **Phase 1a outcome (#1664, merged 2026-05-03).** Done.
> `wvlet-http-server` module wired in (JVM-only, depends on
> `airframe-http` + `uni`); `wvlet-server` now `dependsOn(httpServer)`
> but consumes nothing from it yet. Ported `StaticContent` under
> `wvlet.lang.http.server.static` with a 3-test smoke suite (200 / 404
> / 403). Codex/Gemini reviews surfaced three issues that landed as a
> follow-up commit: literal NUL byte in source (replaced with the
> ` ` escape so the file isn't classified as binary by git),
> hard-coded fixture path (switched to classpath-resource lookup), and
> CRLF-on-Windows test brittleness (normalize line endings before
> comparing). Gemini's second pass raised quality issues about the
> *original airframe code* (OOM on `IOUtil.readFully`, no cache
> headers, missing MIME types) — left for a phase 6+ follow-up to
> preserve the verbatim-port invariant.

> **Phase 1b lesson learned.** A first attempt to slice Phase 1
> further (1b = Automaton + Route + RouteMatcher only) failed to
> compile. `Route.scala` accesses
> `wvlet.airframe.http.HttpBackend.TLS_KEY_RPC`, which is
> `private[http]` in airframe — and once the file moves to
> `wvlet.lang.http.server.router`, the access check fires. Closing
> that hole requires pulling `HttpBackend` along too, which in turn
> drags in `compat.defaultExecutionContext` (cross-platform) and the
> rest of the http core types. The verbatim-port files in airframe
> are tightly interlocked across `private[http]`-protected internals;
> any slice that doesn't include the full set of files protected by a
> shared access scope hits the same wall.
>
> **Revised plan: Phase 1 ships as a single PR**, with the staging
> below for sequencing within the branch (not separate PRs):
>
> - **1a (done):** module skeleton + `StaticContent` (#1664).
> - **1b–1d (remaining):** in one PR, port the rest as one
>   transactionally consistent set —
>   - `airframe-http/.jvm/.../router/*` (8 files: Automaton,
>     Route, RouteMatcher, ControllerProvider,
>     HttpEndpointExecutionContext, HttpRequestDispatcher,
>     HttpRequestMapper, ResponseHandler) plus `Router.scala`,
>   - `airframe-http/src/main/scala/wvlet/airframe/http/{HttpBackend,
>     HttpFilter, HttpContext, HttpServer, HttpLogger, RPCContext,
>     RPCMethod, RPCEncoding, RxRouter, RxRouterProvider,
>     RxHttpBackend, RxHttpEndpoint, RxHttpFilter}`,
>   - `airframe-http/src/main/scala/wvlet/airframe/http/internal/{HttpLogs,
>     HttpMultiMapCodec, HttpResponseBodyCodec, RPCCallContext,
>     RPCResponseFilter}`,
>   - `airframe-http/src/main/scala/wvlet/airframe/http/filter/CorsFilter`,
>   - the Scala-3 `RxRouterBase` + Scala-2 `RxRouterMacros`,
>   - `airframe-http-netty/.../netty/*` (7 files),
>   - `airframe-http/.jvm/.../HttpAccessLogWriter`,
>     `LogRotationHttpLogger`, `TLSSupport`, `LocalRPCContext`.
>
>   That's ~25 files / ~3.3k LOC. Mechanical. Reviewers can diff
>   each file against its airframe counterpart — the changes will be
>   limited to package renames, intra-package import rewrites, and
>   widening of `private[http]` modifiers (probably to `private[server]`
>   inside `wvlet.lang.http.server` or just public).

**Phase 2 — Switch `wvlet-server` and `wvlet-api` to the new module's
symbols.** Replace `wvlet.airframe.http.netty.{Netty, NettyServer}` with
`wvlet.lang.http.server.{Netty, NettyServer}` in `WvletServer.scala`.
Replace `wvlet.airframe.http.{Http, RxRouter, Endpoint, HttpMessage,
StaticContent}` with the wvlet-http-server equivalents. Replace
`wvlet.airframe.Design` with `wvlet.uni.design.Design`. Run
`server/test`.

**Phase 3 — Decouple ported code from airframe-http core types.** Inside
`wvlet-http-server`, swap consumption from
`wvlet.airframe.http.{HttpMessage, HttpHeader, HttpStatus, HttpMethod,
HttpMultiMap, HttpFilter, HttpContext, ServerSentEvent, RPCStatus,
RPCException}` to the `wvlet.uni.http.*` equivalents. This is the bulk
of the rename work and where API-shape mismatches surface (e.g., uni
has top-level `Request`/`Response`, airframe nests them under
`HttpMessage`). Add small adapter shims where uni's API is different.

**Phase 4 — Switch RPC codegen from `AirframeHttpPlugin` to
`sbt-uni-codegen`.** Update `project/plugin.sbt`, replace the
`airframeHttpClients` setting in the `client` project. Regenerate
`FrontendRPC`. Update consumers (`WvletServer.scala`,
`WvletUIMain.scala`, `WvletServerTest.scala`, etc.).

**Phase 5 — Drop airframe-http transitive deps.** Remove
`airframe-http`/`airframe-http-netty` from `build.sbt`. Remaining
airframe deps (rx, codec, surface, scalatest) stay as separate
concerns. Run full build matrix.

**Phase 6 (optional cleanup) — Migrate codec/surface/rx away from
airframe** as separate follow-up PRs (not in this plan's scope, but
document the path).

## 5. Risks and unknowns

- **Scala 3 macros for `RxRouter.of[T]`.** This is the highest risk. The
  macro inspects the target trait via Surface and constructs an
  `RxRouter`. `RxRouterMacros.scala` (Scala 2) and `RxRouterBase.scala`
  (Scala 3) need careful porting — even a verbatim copy may break if it
  captures `wvlet.airframe.http` qualified names in quoted code. Plan:
  copy under new package, run `lang.jvm/test/compile` early in Phase 1
  and fix immediately.
- **Scala.js / Native cross-build.** wvlet-server is JVM-only, but
  `wvlet-api` (which currently imports `wvlet.airframe.http.{RPC,
  RxRouter}`) is a `crossProject(JVMPlatform, JSPlatform,
  NativePlatform)`. The new server module is JVM-only, so the
  cross-built API code must depend only on uni's already-cross-built
  `wvlet.uni.http.*` types and the `RPC` annotation. If uni's `RPC`
  annotation doesn't exist, port it as a Pure cross-built shared
  module (`wvlet-http-api`).
- **Netty version.** airframe pins `4.2.10.Final`; pin the same in
  wvlet to avoid behavioral drift, then bump as a separate change.
- **`MessageCodecFactory` coupling in `NettyServerConfig`.** Replacing
  `airframe-codec` is out of scope for this migration — keep it as a
  runtime dep through Phase 5.
- **`wvlet.airframe.http.RPC` annotation cross-Scala-version
  coupling.** The Java annotation is referenced by codegen tooling. If
  uni's codegen reads a different annotation symbol, the wvlet-api
  types must use uni's. Verify before Phase 0.
- **`HttpRequestDispatcher` + `Session` integration.** The dispatcher
  uses `airframe.Session` to build controller instances. After moving
  to `wvlet.uni.design.Session`, instance lookup APIs may differ —
  check `Session.build[T]` / `Session.getInstance` parity.
- **`StaticContent.fromDirectory`** uses classloader resource
  resolution; behavior on graal native-image / packaged JAR layouts of
  wvlet-server should be re-verified.

## 6. Verification strategy

Per phase, run in this order: `./sbt projectJVM/Test/compile`,
`./sbt projectJS/Test/compile`, `./sbt projectNative/Test/compile`,
`./sbt server/test`, `./sbt cli/test`. Match the test plan from prior
commit `48bab6fe`.

Targeted suites:
- **`wvlet-server/src/test/.../WvletServerTest.scala`** — boots the full
  Netty server with the production design, issues a real RPC call to
  `FrontendApi.status`. This is the single best end-to-end gate.
- **`wvlet-server/src/test/.../FileApiImplTest.scala`** — exercises the
  controller path through router.
- After Phase 4, hand-verify `wvlet ui` boots in dev mode and the Web
  UI loads (`StaticContentApi` path).
- Port a *small* subset of `airframe-http-netty/src/test/.../Netty*Test.scala`
  (RESTServerTest, RPCServerTest, StaticContentTest) into the new
  module to lock in behavior of the ported runtime — these are missing
  from wvlet today and are the only true regression net for the server
  runtime itself.

## Critical files

In this repository:

- `wvlet-server/src/main/scala/wvlet/lang/server/WvletServer.scala`
- `wvlet-api/src/main/scala/wvlet/lang/api/v1/frontend/FrontendApi.scala`
- `build.sbt`

In the `wvlet/airframe` repository (sources to port):

- `airframe-http-netty/src/main/scala/wvlet/airframe/http/netty/NettyServer.scala`
- `airframe-http/.jvm/src/main/scala/wvlet/airframe/http/router/HttpRequestDispatcher.scala`

## 7. Major course correction (2026-05-03)

**Critical revision: uni already ships the full HTTP server runtime.**
The Section 1 / Phase 1b–1d plan to port ~3.3k LOC of airframe-http
into `wvlet.lang.http.server.*` is **abandoned**. Inspecting Maven
Central revealed:

- `uni-netty_3` (2026.1.8) publishes a complete Netty server runtime —
  `NettyServer`, `NettyServerConfig`, `NettyHttpServer`,
  `RouterHandler`, `RxHttpHandler`, `RxHttpFilter`, `RPCHandler`,
  `NettyRequestHandler`.
- `uni_3` already ships the full router stack
  (`Router, Route, RouteMatcher, RxRouter, RxRouterProvider,
  ControllerProvider, HttpRequestMapper, ResponseConverter,
  RouterMacros`) and the full RPC stack (`RPCStatus, RPCException,
  RPCRoute, RPCRouter, RPCClient, MethodCodec`).

Two simplifying findings the user surfaced:

1. **No `@RPC` annotation needed.** uni's `RPCRouter.of[T]` scans a
   service trait via Surface; no annotation is required. So the
   migration drops `wvlet.airframe.http.RPC` from `FrontendApi` /
   `FileApi` rather than vendor-or-substitute.
2. **airframe's `Netty.server.withRouter(...).design.bindXxx` chain
   is just sugar for a regular `Design`.** uni-netty exposes
   `NettyServerConfig` directly; equivalent wvlet code constructs the
   config and binds it via uni's `Design` plus lifecycle hooks.
3. **Codegen.** uni publishes `sbt-uni_sbt2_3` (sbt 2.x) with HTTP
   codegen. wvlet currently uses sbt 1.11.1, so adopting the uni
   codegen plugin is gated on a separate sbt 1→2 migration —
   investigate before Phase 4.

### Revised phase plan

Each phase is one PR. The remaining work shrinks dramatically.

- **Phase 1b (this PR).** Re-vendor `StaticContent` against uni HTTP
  types (`wvlet.uni.http.{Response, HttpStatus, HttpHeader}`,
  `wvlet.uni.control.{Control, IO}`, `wvlet.uni.log.LogSupport`).
  Replace airframe's `wvlet.log.io.Resource.find` with classloader
  `getResource`. Drop `airframe-http` from `wvlet-http-server` deps;
  add `uni-netty`. Update `StaticContentTest` to `UniTest` +
  `wvlet.uni.http.HttpStatus`. Replace literal NUL byte with
  `path.indexOf(0) >= 0` (avoids any unicode-escape preprocessing
  that would re-introduce a NUL byte at compile time, while still
  catching path-null injection).
- **Phase 2.** Migrate `wvlet-server/WvletServer.scala` and
  `StaticContentApi.scala` to uni `NettyServer` + uni router/Design.
  Translate the `Netty.server.withRouter(rxRouter).design...` chain
  into `Design.newDesign.bindInstance[NettyServerConfig](...)` plus
  uni-netty lifecycle wiring. Migrate `wvlet-ui-main/WvletUIMain.Http`
  import. Drop `airframe-http-netty` from `wvlet-server` deps.
- **Phase 3.** Migrate `wvlet-api` (`FrontendApi`, `FileApi`) to uni
  router types. Drop `@RPC` annotations entirely; let
  `RPCRouter.of[T]` discover routes from the trait surface. Verify
  the JS/Native cross-builds still link.
- **Phase 4.** Switch RPC codegen from `AirframeHttpPlugin` to uni's
  HTTP codegen. Predicated on `sbt-uni` having a sbt 1.x build, or on
  wvlet migrating to sbt 2.x — investigate first.
- **Phase 5.** Drop `airframe-http` and any remaining airframe HTTP
  transitive deps from `build.sbt` once Phase 4 lands.
