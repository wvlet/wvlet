/-
Licensed under the Apache License, Version 2.0.

Write the operator typing conformance fixture: `lake exe operator-traces [output path]`.
-/
import WvletFormal

def defaultOutput : System.FilePath :=
  "../wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/typer/operator-traces.json"

def main (args : List String) : IO Unit := do
  let path : System.FilePath := (args.head?.map System.FilePath.mk).getD defaultOutput
  if let some dir := path.parent then IO.FS.createDirAll dir
  IO.FS.writeFile path Wvlet.Typer.OperatorTraces.fixture
  IO.println s!"Wrote {path}"
