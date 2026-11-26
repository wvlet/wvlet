/-
Licensed under the Apache License, Version 2.0.

Write the aggregate windows conformance fixture: `lake exe aggregate-windows-traces [output path]`.
-/
import WvletFormal

def defaultOutput : System.FilePath :=
  "../wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/aggregate-windows-traces.json"

def main (args : List String) : IO Unit := do
  let path : System.FilePath := (args.head?.map System.FilePath.mk).getD defaultOutput
  if let some dir := path.parent then IO.FS.createDirAll dir
  IO.FS.writeFile path Wvlet.AggregateWindows.Traces.fixture
  IO.println s!"Wrote {path}"
