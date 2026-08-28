# Fix wvlet playground crash on load caused by Vite 8 minifier

- Author: Taro L. Saito
- Date: 2026-08-28

## Goals

- Restore the wvlet playground so it loads without the "Invalid seconds: 0" crash reported at load time and can render its editor and query result surfaces again.
- Prevent the same class of miscompilation from silently affecting the other Scala.js Vite bundle in this repo (`wvlet-ui-main`).
- Record why we no longer trust the current Vite 8 default minifier for our Scala.js bundles so a future upgrade does not silently reintroduce the same failure.

## Background

The deployed playground fails during page init with:

```
Uncaught java.time.DateTimeException: Invalid seconds: 0
```

The downstream `Cannot read properties of null (reading 'setValue')` on the Monaco editor is a consequence: `Instant` throws during module initialization, the render pipeline aborts before the Monaco container is populated, and subsequent handlers try to `setText` on a null editor.

Reproducing locally against a production build (`sbt playground/fullLinkJS && pnpm --filter wvlet-ui-playground run build`) and loading `dist/` in headless Chromium via Playwright shows the identical stack the user reported. Feeding the minified stack through the Vite bundle source map, then the Scala.js source map, resolves the frames as:

- `MI` -> `scala-js-java-time/src/main/scala/java/time/Preconditions.scala:9`
- `jl` -> `scala-js-java-time/src/main/scala/java/time/Instant.scala:301`
- `Nl` -> `scala-js-java-time/src/main/scala/java/time/Instant.scala:295`
- `bc` -> `wvlet/uni/io/BrowserFileSystem.scala:41` (the object BrowserFileSystem$ init that calls `Instant.now()` while building its in-memory root file system)
- `Dd.t1` / `cae` -> `wvlet/uni/io/FileSystemImpl.scala:96` / `wvlet/uni/io/FileSystem.scala:684` (the compiler-side `SourceIO` path that reaches the browser filesystem during compilation)

The Scala.js-emitted `Instant` constructor is:

```js
function $c_Ljava_time_Instant(seconds_$_lo, seconds_$_hi, nanos) {
  ...
  if (((seconds_$_hi === x_$_hi) ? ... : (seconds_$_hi > x_$_hi))) {
    ...
    var $x_1 = ((seconds_$_hi === x$1_$_hi) ? ... : (seconds_$_hi < x$1_$_hi));
  } else {
    var $x_1 = false;
  }
  if ((!$x_1)) {
    throw new $c_Ljava_time_DateTimeException("Invalid seconds: " + ...);
  }
  ...
}
```

Vite 8's default minifier (Rolldown's oxc-minify) drops the outer `if ((!$x_1))` guard on the throw. The minified bundle emits:

```js
function MI(e,t,n){
  this.jO=0,this.jP=0,this.o2=0, this.jO=e,this.jP=t,this.o2=n;
  var r=Nl(),i=r.a45,a=r.a46;
  if(t===a?e>>>0>=i>>>0:t>a)
    var o=Nl(),s=o.a43,c=o.a44,l=t===c?e>>>0<=s>>>0:t<c;
  else
    var l=!1;
  throw new MD(`Invalid seconds: `+Xr(this.jO,this.jP))
}
```

Every `Instant` allocation now throws unconditionally. The very first allocation - the module-init `new Instant(0, 0, 0)` inside `Instant$` - is what surfaces the "Invalid seconds: 0" message. This is a minifier miscompilation, not a Scala.js source bug: the same fullLinkJS output, served either unminified (`vite build --minify=false`) or minified via esbuild (`vite build --minify=esbuild`), loads and initialises the playground cleanly (DuckDB-Wasm ready, editor rendered).

`wvlet-ui-main` uses the same Vite 8 default configuration and would be silently exposed to the same class of miscompilation.

## Design

Pin both Vite bundles that ship Scala.js output to esbuild for minification, via `build.minify: 'esbuild'` in each `vite.config.js`, so Rolldown's oxc-minify cannot rewrite the Scala.js `Instant`/`Duration`/similar guarded-throw patterns.

Concrete changes:

- `wvlet-ui-playground/vite.config.js`: add a `build: { minify: 'esbuild' }` block. Keep `sourcemap` off in production (matches current behaviour), keep the existing `koffi` alias, and keep the monaco/tailwind/replace plugin chain unchanged.
- `wvlet-ui-main/vite.config.js`: same `build: { minify: 'esbuild' }` addition. This bundle is not currently deployed with the same crash surfaced, but it links the identical Scala.js runtime and would fail the same way once its output touches an `Instant` allocation.
- Add a short comment above each new `build` block explaining that `oxc-minify` currently miscompiles Scala.js-generated conditional throws (link to the frame decoded above in the plan) so a future maintainer does not "clean up" the pin.
- No production JS bundle is committed; the fix takes effect the next time we run `pnpm --filter wvlet-ui-playground run build` and `pnpm --filter wvlet-ui-main run build`.

Non-goals:

- Not upgrading, downgrading, or replacing Vite / Rolldown itself. Pinning the minifier surface is a smaller, revertable change than moving the whole build tool, and lets us continue to benefit from Rolldown's bundling and dev-server work.
- Not filing or waiting on an upstream oxc-minify fix as a blocker. If/when one lands, we can drop the pin in a follow-up.
- Not touching the Monaco worker warning ("Could not create web worker...") - it is orthogonal to the crash and only surfaces once initialisation succeeds.
- Not addressing the `TablePrinter` `NegativeArraySizeException` that surfaces in `fastopt` (dev) mode when rendering an empty `QueryResult`. That is a real bug, but a distinct one, and mixing it into this fix would slow shipping the crash restoration. It will be captured as a follow-up.

Verification plan for this change:

1. `sbt playground/fullLinkJS` then `pnpm --filter wvlet-ui-playground run build`.
2. Serve `wvlet-ui-playground/dist/` and open it in a real browser: playground loads, the editor renders the sample query, and no "Invalid seconds" appears in the DevTools console.
3. Compare pre/post `main-*.js` size to confirm the bundle is still meaningfully minified (esbuild output should stay comparable to prior oxc-minify output, well below the unminified size).
4. Repeat step 1/2 for `wvlet-ui-main` against a locally-run wvlet server to confirm no regression.

## Alternatives and Why Not?

- **`build.minify: false`**: proves the miscompilation but ships a large unminified bundle in production. Rejected as a permanent option; only used during diagnosis.
- **Downgrade Vite to 7.x**: sidesteps Rolldown entirely, but that is a much larger dependency change than pinning the minifier, drags in cascading dev-dep bumps (plugins, tailwind vite plugin), and gives up unrelated Vite 8 improvements.
- **Add a Scala-side workaround (avoid `Instant.now()` in `BrowserFileSystem` init, precompute `EPOCH` differently, etc.)**: the failure is in downstream bytes that we do not own, and the same pattern will keep surfacing on any Scala.js javalib class that guards a throw the same way (`Duration`, `LocalDate`, ...). Fixing every future occurrence in Scala source is a losing game.
- **Wait for an upstream oxc-minify fix**: playground is broken in production today; we should ship a working build and can remove the pin once oxc-minify has been reverified against Scala.js output.

## Tips

- Final decision maker: Taro L. Saito.
- Open discussion points: none blocking. A follow-up is warranted to track the `TablePrinter` empty-result bug and to periodically re-test the default minifier once oxc-minify stabilises.

## Caveats

- esbuild's minifier is slightly less aggressive than oxc-minify in a few micro-optimisation areas, so bundle size may tick up marginally. Prior comparisons in this build showed the difference well under 5% for the playground bundle.

## Security Considerations

None identified for this change. It only alters build-time minifier selection and does not expand attack surface, credential handling, or data exposure.
