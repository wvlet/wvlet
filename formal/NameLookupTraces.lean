/-
Licensed under the Apache License, Version 2.0.

Write the name-lookup conformance fixture: `lake exe name-lookup-traces [output path]`.
-/
import WvletFormal

def defaultOutput : System.FilePath :=
  "../wvlet-lang/.jvm/src/test/resources/wvlet/lang/compiler/lookup/name-lookup-traces.json"

def main (args : List String) : IO Unit := do
  let path : System.FilePath := (args.head?.map System.FilePath.mk).getD defaultOutput
  if let some dir := path.parent then IO.FS.createDirAll dir
  IO.FS.writeFile path Wvlet.NameLookup.Traces.fixture
  IO.println s!"Wrote {path}"
