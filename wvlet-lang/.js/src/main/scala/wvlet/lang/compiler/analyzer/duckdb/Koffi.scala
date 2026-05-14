package wvlet.lang.compiler.analyzer.duckdb

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

/**
  * Minimal Scala.js facade for the [koffi](https://www.npmjs.com/package/koffi) FFI library.
  *
  * koffi ships prebuilt N-API addons for every major platform (darwin arm64/x64, linux
  * x64/arm64/musl, windows x64/arm64, freebsd, openbsd), so installing it via npm needs no build
  * toolchain. We use it to call `libduckdb`'s C API synchronously from Node — same 9-function
  * surface we already bind from Scala Native via `@extern @link("duckdb")`.
  *
  * Only the bits we actually use are surfaced here; the full koffi API is much larger.
  */
// koffi is a CommonJS package; under `ModuleKind.ESModule` consumers (like wvlet-cli-core.js)
// the CJS module's exports surface as the ES module's DEFAULT export. Using
// `JSImport.Default` here makes the facade work under both CommonJSModule (langJS tests)
// and ESModule (the cli-node bundle).
@js.native
@JSImport("koffi", JSImport.Default)
private[duckdb] object Koffi extends js.Object:

  /**
    * Load a dynamic library by absolute or system-resolvable path. Throws synchronously on failure
    * (missing file, wrong arch, …).
    */
  def load(path: String): KoffiLib = js.native

  /**
    * Declare a C struct layout so it can be used in function signatures. The returned type handle
    * isn't called directly — koffi looks it up by the struct name when it parses the cdecl strings
    * passed to `KoffiLib.func`.
    */
  def struct(name: String, fields: js.Object): js.Any = js.native

end Koffi

@js.native
private[duckdb] trait KoffiLib extends js.Object:
  /**
    * Bind a C function from this library by its cdecl signature. Returns a callable that runs the C
    * call synchronously. Output-pointer parameters use Windows-SAL-style markers (`_Out_`,
    * `_Inout_`); koffi maps them to single-element JS arrays at the call site.
    */
  def func(signature: String): js.Function = js.native

end KoffiLib

/**
  * Top-level koffi entry points we use directly (outside `Koffi.load(...).func(...)` bindings).
  * `decode` is the key piece for chunk reading — it lets us read typed values at a byte offset
  * within a raw pointer (including reading an inner pointer as `void *` so it stays a koffi pointer
  * rather than getting auto-stringified to JS).
  */
@js.native
@JSImport("koffi", JSImport.Default)
private[duckdb] object KoffiOps extends js.Object:
  def decode(value: js.Any, offset: Int, `type`: js.Any): js.Any = js.native
  def decode(value: js.Any, `type`: js.Any): js.Any              = js.native
  def array(elementType: String, count: Int): js.Any             = js.native
  def address(ptr: js.Any): js.BigInt                            = js.native

end KoffiOps
