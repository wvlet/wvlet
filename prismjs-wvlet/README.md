# Wvlet Language Support for Prism.js

Syntax highlighting for [Wvlet](https://wvlet.org), a cross-SQL flow-style query language for functional data modeling and interactive data exploration.

## Features

This package provides syntax highlighting for:

- Wvlet keywords (`from`, `select`, `where`, `group by`, `model`, `def`, etc.)
- SQL-like operations and joins
- String literals (single, double, and triple-quoted)
- String interpolation with `${...}`
- Numeric literals (integers, decimals, floats, longs)
- Comments (single-line `--` and doc comments `---`)
- Backquoted identifiers
- Test assertions
- Operators and special symbols
- Function definitions and calls

## Installation

### Using npm

```bash
npm install @wvlet/prismjs-wvlet prismjs
```

### Using CDN

```html
<!-- Using specific version (recommended for production) -->
<script src="https://cdn.jsdelivr.net/npm/@wvlet/prismjs-wvlet@2025.1.13/dist/prism-wvlet.min.js"></script>

<!-- Using latest version -->
<script src="https://cdn.jsdelivr.net/npm/@wvlet/prismjs-wvlet@latest/dist/prism-wvlet.min.js"></script>
```

## Usage

### With Node.js / CommonJS

```javascript
const Prism = require('prismjs');
require('@wvlet/prismjs-wvlet');

const code = `model User = {
  from 'users.json'
  select name, age, email
}`;

const highlightedCode = Prism.highlight(code, Prism.languages.wvlet, 'wvlet');
```

### With ES6 Modules

```javascript
import Prism from 'prismjs';
import '@wvlet/prismjs-wvlet';

const highlightedCode = Prism.highlight(code, Prism.languages.wvlet, 'wvlet');
```

### In the Browser

```html
<!DOCTYPE html>
<html>
<head>
  <!-- Prism.js theme -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/prismjs@1/themes/prism.min.css">
</head>
<body>
  <!-- Your code blocks -->
  <pre><code class="language-wvlet">
  model User = {
    from 'users.json'
    select name, age, email
  }
  </code></pre>

  <!-- Prism.js core -->
  <script src="https://cdn.jsdelivr.net/npm/prismjs@1/components/prism-core.min.js"></script>
  <!-- Wvlet language support -->
  <script src="https://cdn.jsdelivr.net/npm/@wvlet/prismjs-wvlet@2025.1.13/dist/prism-wvlet.min.js"></script>
  
  <script>
    // Apply highlighting
    Prism.highlightAll();
  </script>
</body>
</html>
```

For a complete example, see [examples/cdn-test.html](https://github.com/wvlet/wvlet/tree/main/prismjs-wvlet/examples/cdn-test.html).


## Language Aliases

Both `wvlet` and `wv` are supported:

- `<code class="language-wvlet">` (primary)
- `<code class="language-wv">` (alternative)

## Building

To build the distribution files:

```bash
npm install
npm run build
```

This will create `dist/prism-wvlet.js` and `dist/prism-wvlet.min.js`.

## Testing

Run the tests with:

```bash
npm test
```

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Links

- [Wvlet Documentation](https://wvlet.org)
- [Wvlet GitHub Repository](https://github.com/wvlet/wvlet)
- [Prism.js](https://prismjs.com)
- [highlight.js Wvlet Extension](https://www.npmjs.com/package/@wvlet/highlightjs-wvlet)