/-
Licensed under the Apache License, Version 2.0.

Write the self-join aggregation conformance fixture: `lake exe self-join-aggregate-traces [output path]`.
-/
import WvletFormal

def defaultOutput : System.FilePath :=
  "../wvlet-engine/.jvm/src/test/resources/wvlet/lang/engine/runtime/self-join-aggregate-traces.json"

def main (args : List String) : IO Unit := do
  let path : System.FilePath := (args.head?.map System.FilePath.mk).getD defaultOutput
  if let some dir := path.parent then IO.FS.createDirAll dir
  IO.FS.writeFile path Wvlet.SelfJoinAggregate.Traces.fixture
  IO.println s!"Wrote {path}"
