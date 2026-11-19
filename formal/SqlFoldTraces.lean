/-
Licensed under the Apache License, Version 2.0.

Write the SELECT-block folding conformance fixture: `lake exe sql-fold-traces [output path]`.
-/
import WvletFormal

def defaultOutput : System.FilePath :=
  "../wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/codegen/sql-fold-traces.json"

def main (args : List String) : IO Unit := do
  let path : System.FilePath := (args.head?.map System.FilePath.mk).getD defaultOutput
  if let some dir := path.parent then IO.FS.createDirAll dir
  IO.FS.writeFile path Wvlet.SqlFold.Traces.fixture
  IO.println s!"Wrote {path}"
