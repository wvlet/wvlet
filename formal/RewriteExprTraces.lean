/-
Licensed under the Apache License, Version 2.0.

Write the expression rewrite conformance fixture: `lake exe rewrite-expr-traces [output path]`.
-/
import WvletFormal

def defaultOutput : System.FilePath :=
  "../wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/transform/rewrite-expr-traces.json"

def main (args : List String) : IO Unit := do
  let path : System.FilePath := (args.head?.map System.FilePath.mk).getD defaultOutput
  if let some dir := path.parent then IO.FS.createDirAll dir
  IO.FS.writeFile path Wvlet.Rewrite.Traces.fixture
  IO.println s!"Wrote {path}"
