/-
Licensed under the Apache License, Version 2.0.

Write the activity conformance fixture: `lake exe activity-traces [output path]`.
-/
import WvletFormal

def defaultOutput : System.FilePath :=
  "../wvlet-runner/.jvm/src/test/resources/wvlet/lang/runner/activity/activity-traces.json"

def main (args : List String) : IO Unit := do
  let path : System.FilePath := (args.head?.map System.FilePath.mk).getD defaultOutput
  if let some dir := path.parent then IO.FS.createDirAll dir
  IO.FS.writeFile path Wvlet.Activity.Traces.fixture
  IO.println s!"Wrote {path}"
