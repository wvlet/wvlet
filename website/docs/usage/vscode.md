---
sidebar_label: VS Code Extension
sidebar_position: 8
---

# VS Code Extension

The Wvlet extension provides syntax highlighting and language support for `.wv` files in Visual Studio Code.

## Installation

Install from the [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet) or search for "Wvlet" in VS Code's Extensions view.

## Features

- **Syntax Highlighting**: Full support for Wvlet keywords, operators, and syntax
- **Bracket Matching**: Automatic matching for `{}`, `[]`, `()`, and `${}`
- **Comment Support**: Single-line (`--`) and multi-line (`---`) comments
- **String Interpolation**: Highlighting for `${...}` expressions
- **Auto-closing**: Brackets and quotes automatically close when typed

## Example

```sql
-- Query with syntax highlighting
from lineitem
where l_shipdate >= '2024-01-01'
group by l_shipmode
agg _.count as cnt,
    l_quantity.sum.round(2) as total_qty
order by cnt desc
```

## Pre-release Versions

To try new features early, switch to the pre-release version from the extension's settings in VS Code.

## Links

- [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet)
- [Report Issues](https://github.com/wvlet/wvlet/issues)