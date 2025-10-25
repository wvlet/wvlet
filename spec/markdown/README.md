# Markdown Spec Tests

This directory contains spec-driven tests for Markdown parsing support in Wvlet.

## Test Files

- `heading.md` - Tests various heading levels (# to ######)
- `paragraph.md` - Tests paragraphs with inline formatting
- `code-block.md` - Tests fenced code blocks with/without language hints
- `list.md` - Tests ordered, unordered, and nested lists
- `blockquote.md` - Tests blockquote syntax
- `link.md` - Tests links and images
- `mixed.md` - Tests complex documents with mixed elements

## Testing Approach

These tests follow the spec-driven testing pattern used in Wvlet:

1. Each `.md` file is parsed by the `MarkdownParser`
2. The parser builds a CST (Concrete Syntax Tree) with accurate `Span` information
3. Tests verify:
   - Correct CST structure via `plan.pp`
   - Roundtrip capability: original text can be extracted from spans
   - No parsing errors

## Running Tests

```bash
# Run all markdown spec tests
./sbt "langJVM/testOnly *ParserSpecMarkdown"

# Run a specific markdown spec
./sbt "langJVM/testOnly *ParserSpecMarkdown -- spec:markdown:heading.md"
```

## CST Design

The markdown parser uses a **Concrete Syntax Tree** approach:

- **Preserves source location**: Every node has a `Span`
- **Roundtrip support**: Original text can be extracted from `sourceFile.textAt(span)`
- **No text storage**: Nodes store only spans, not extracted/processed text
- **Memory efficient**: 8 bytes per span vs potentially large text strings

Example:
```scala
case class MarkdownHeading(level: Int, span: Span) extends MarkdownBlock:
  // Text is extracted on-demand from the span, not stored
  def text(using ctx: Context): String =
    ctx.sourceFile.textAt(span)
```
