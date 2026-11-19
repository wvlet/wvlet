/-
Licensed under the Apache License, Version 2.0.

Conformance traces for the model build model, exported as JSON so that a Scala test can replay them
against the shipped `ModelOrder`. Models are named `a`, `b`, `c` (materialized) and `e`, `f`
(ephemeral); `x` is a name no model defines.
-/
import WvletFormal.ModelBuild.Order

namespace Wvlet.ModelBuild.Traces

open Wvlet.ModelBuild

private def jstr (s : String) : String := "\"" ++ s ++ "\""

private def jobj (fields : List (String × String)) : String :=
  "{" ++ ", ".intercalate (fields.map fun (k, v) => jstr k ++ ": " ++ v) ++ "}"

private def jarrayLines (items : List String) : String :=
  "[\n" ++ ",\n".intercalate (items.map ("    " ++ ·)) ++ "\n  ]"

private def jnames (ns : List String) : String := "[" ++ ", ".intercalate (ns.map jstr) ++ "]"

/-- Every sublist, in order. -/
def sublists : List String → List (List String)
  | [] => [[]]
  | x :: xs => (sublists xs).flatMap fun s => [x :: s, s]

def lookup (assoc : List (String × List String)) (n : String) : List String :=
  ((assoc.find? (·.1 == n)).map (·.2)).getD []

def models : List String := ["a", "b", "c"]

/-- Every graph on three models: each model reads any subset of them, itself included. -/
def graphs : List (List (List String)) :=
  (sublists models).flatMap fun ua => (sublists models).flatMap fun ub =>
    (sublists models).map fun uc => [ua, ub, uc]

def toGraph (ups : List (List String)) : Graph String :=
  ⟨models, lookup (models.zip ups)⟩

/-- Selections replayed through `buildOrder`: every nonempty subset, plus one out-of-order input. -/
def orderInputs : List (List String) :=
  ((sublists models).filter (· ≠ [])) ++ [["c", "b", "a"]]

def singleSelectors : List String :=
  models.flatMap fun m => [m, "+" ++ m, m ++ "+", "+" ++ m ++ "+"]

def orderCase (g : Graph String) (sel : List String) : String :=
  match buildOrder g.ups sel with
  | .ok o => jobj [("selected", jnames sel), ("order", jnames o)]
  | .error p => jobj [("selected", jnames sel), ("cycle", jnames p)]

def selectResult (g : Graph String) (args : List String) : List (String × String) :=
  match select g args with
  | .ok r => [("result", jnames r)]
  | .error .empty => [("error", jstr "empty")]
  | .error (.notFound n) => [("error", jstr "notFound"), ("name", jstr n)]

def selectCase (g : Graph String) (args : List String) : String :=
  jobj ([("selectors", jnames args)] ++ selectResult g args)

def hasSelfLoop (ups : List (List String)) : Bool :=
  (models.zip ups).any fun (m, us) => us.contains m

/--
One line per graph: the detected cycle, the build order of each input, and, for graphs without
self-loops (a self-loop does not change a closure), every single selector.
-/
def graphCase (ups : List (List String)) : String :=
  let g := toGraph ups
  let cycle := match findCycle g.ups g.models with
    | some p => jnames p
    | none => "null"
  let sels := if hasSelfLoop ups then [] else singleSelectors.map fun s => selectCase g [s]
  jobj [("ups", "[" ++ ", ".intercalate (ups.map jnames) ++ "]"), ("cycle", cycle),
    ("orders", "[" ++ ", ".intercalate (orderInputs.map (orderCase g)) ++ "]"),
    ("select", "[" ++ ", ".intercalate sels ++ "]")]

/-- Selector parsing on the graph `a` reads `b`, and `c` reads nothing. -/
def parsingGraph : Graph String := ⟨models, lookup [("a", ["b"])]⟩

def parsingInputs : List (List String) :=
  [[], [""], [","], [" , "], ["+"], ["++"], ["a"], [" a "], ["+a"], ["a+"], ["+a+"], ["b+"],
   ["+b"], ["+b+"], ["a,c"], ["c,a"], ["a", "c"], ["a,,c"], ["+a,c+"], [" +a , b+ "], ["x"],
   ["a,x"], ["x", ""], ["a++"], ["++a"], ["+x+"], ["a", ""]]

/-- Edge derivation: materialized `a`, `b` and ephemeral `e`, `f`. -/
def edgeDefs : List String := ["a", "b", "e", "f"]

def isMat (n : String) : Bool := n == "a" || n == "b"

def edgeCase (refs : List (String × List String)) : String :=
  let up (m : String) := upstreamsOf edgeDefs isMat (lookup refs) (lookup refs m)
  jobj [("refs", jobj (refs.map fun (n, r) => (n, jnames r))),
    ("upstreams", jobj [("a", jnames (up "a")), ("b", jnames (up "b"))])]

/--
Every reference set over `a`, `b`, `e` for `a`, `b`, and `e`. `a` lists `e` first, so reaching a
model through `e` and directly can happen in either order (breadth-first order matters).
-/
def enumeratedRefs : List (List (String × List String)) :=
  let names := ["a", "b", "e"]
  (sublists ["e", "a", "b"]).flatMap fun ra => (sublists names).flatMap fun rb =>
    (sublists names).map fun re => [("a", ra), ("b", rb), ("e", re)]

/-- Longer chains, reference order, and unknown names. -/
def handpickedRefs : List (List (String × List String)) :=
  [ [("a", ["e"]), ("e", ["f"]), ("f", ["b"])],
    [("a", ["e"]), ("e", ["f"]), ("f", ["a"])],
    [("a", ["e", "b"]), ("e", ["f"]), ("f", ["b", "a"])],
    [("a", ["f", "e"]), ("e", ["b"]), ("f", ["a", "b"])],
    [("a", ["x", "e"]), ("e", ["x", "b"])],
    [("a", ["e", "f"]), ("e", ["f"]), ("f", ["e"])],
    [("a", ["e"]), ("b", ["f", "a"]), ("e", ["e", "b"]), ("f", ["f"])],
    [("a", ["e", "a"]), ("e", ["b"])],
    [("a", ["e", "f", "b"]), ("e", ["a"]), ("f", ["e", "b"])] ]

/-- The fixture document. -/
def fixture : String :=
  "{\n" ++
  "  \"generatedBy\": \"formal: lake exe model-build-traces (do not edit by hand)\",\n" ++
  "  \"models\": " ++ jnames models ++ ",\n" ++
  "  \"graphs\": " ++ jarrayLines (graphs.map graphCase) ++ ",\n" ++
  "  \"parsing\": " ++ jarrayLines (parsingInputs.map (selectCase parsingGraph)) ++ ",\n" ++
  "  \"edges\": " ++ jarrayLines ((enumeratedRefs ++ handpickedRefs).map edgeCase) ++ "\n" ++
  "}\n"

end Wvlet.ModelBuild.Traces
