# Playground as a tutorial and reference manual

The [playground](../website/docs/usage/playground.md) runs every example in the browser, but it reads as a tool for people who already know Wvlet. This plan makes it the place to learn Wvlet (a tutorial) and to look things up (a reference manual), reusing what it already has: the example library, `test` assertions and the Tests tab, share links, and the stdlib index.

## Goals

- A newcomer can learn the flow style step by step, with feedback on each exercise, without leaving the page.
- A user can find a function or operator, read what it does, and run an example of it in a few clicks or by hovering in the editor.
- Docs and playground link to each other.

## Phase 1: reference manual (this PR)

- **Editor hover and completion.** Wire the shared `HoverProvider` and `CompletionProvider` (`wvlet-lang/.../compiler/lsp`, already used by the language server) into the playground's Monaco editor. Monaco providers are registered per language, so `MonacoEditor.registerLanguageServices(hover, complete)` registers them once for `wvlet`, calling back into Scala.js with the text and offset. The playground compiles with its TPC-H catalog, so hovers show column types and stdlib function docs.
- **A function reference organized like DuckDB's.** `StdLibCategories` groups functions by purpose (Text, Pattern Matching, Numeric, Date and Time, Aggregate, Window, Array, ...); `StdLibReference` merges a function defined on several types into one entry (`T.abs: T`, T: int, long, ...). The docs page ([stdlib-reference.md](../website/docs/syntax/stdlib-reference.md)) and the playground both read it: the sidebar's Reference tab lists and searches functions, and the output panel's Docs tab shows the selected one with room for its examples (the sidebar was too narrow for details).
- **Examples with computed results.** `-- @example` lines in the stdlib sources; `StdLibExamples` runs them on the engine, for the docs page at generation time and in the playground live (with a Try button). The docs generator moves to engineJVM so it can run the engine; a failing example fails `StdLibDocFreshnessTest`.
- Syntax and operators stay in the Examples tab, which is already organized by feature.

## Phase 1b: notebook (follow-up PR)

- The editor and the bottom result pane become a notebook of text (markdown) and query cells, each query with its result below it. A notebook is a `.wv` file with `-- %%` cell markers (jupytext's percent format), so it compiles as is and keeps line numbers for tests, debug, plans, and lineage.
- A query cell runs with the cells above it (their vals and models are in scope). The right panel follows the focused cell and can fold away.
- Examples open as a text cell (title, description) and a query cell; a function opens as its docs and one cell per example; links between them are in-page hashes (`#example=`, `#function=`).

## Phase 2: tutorial

- Lessons are `.wv` files under `wvlet-ui-playground/src/main/wvlet/Tutorial/`, in the same leading-comment format as examples, with a longer markdown explanation, a starter query, a hint, and an answer.
- Exercises are checked by hidden `test` lines (e.g. `test _.rows should be [...]`) run through `PlaygroundChecks`; passing shows "Correct" and a Next button.
- A lesson is a notebook: explanation in text cells, exercises in query cells. A Learn mode adds Prev/Next and a progress bar. Progress lives in localStorage; a lesson has a share URL (`#lesson=...`).
- Curriculum: `from` → `where` → `select` (last) → add/exclude/rename → `group by` / `agg` → joins → `test` → models → SQL to Wvlet.
- First visit offers "Start the tutorial" alongside the default example.

## Phase 3: run to cursor

- Click an operator's line (or a gutter button) to run the pipeline up to that line and show its rows, so users see how each step of the flow changes the data. This builds on `QuerySelector` and the subquery run (`Ctrl+Enter`).

## Phase 4: docs links

- "Try it" buttons on `website/docs` code blocks open the playground with the query through the existing `#q=` share hash; the Reference tab links to the full docs pages.

## Testing

- `PlaygroundEngineTest`-style JS tests for the hover and completion bridge (offset conversion, TPC-H columns) and for the reference index (sections, examples that use a function).
- `StdLibDocFreshnessTest` (now in engineJVM) regenerates the page, running every example.
- Check the Reference tab and hovers in Chromium after `playground/fastLinkJS`.
