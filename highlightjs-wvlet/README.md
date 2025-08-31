# Wvlet Language Support for highlight.js

Syntax highlighting for [Wvlet](https://wvlet.org), a cross-SQL flow-style query language for functional data modeling and interactive data exploration.

## Features

This package provides syntax highlighting for:

- Wvlet keywords (`from`, `select`, `where`, `group by`, `model`, `def`, etc.)
- SQL-like operations and joins
- String literals (single, double, and triple-quoted)
- Numeric literals (integers, decimals, floats, longs)
- Comments (single-line `--` and doc comments `---`)
- Backquoted identifiers
- Test assertions
- Operators and special symbols
- String interpolation with `${...}`

## Installation

### Using npm

```bash
npm install @wvlet/highlightjs-wvlet
```

### Using CDN

```html
<!-- Using specific version (recommended for production) -->
<script src="https://cdn.jsdelivr.net/npm/@wvlet/highlightjs-wvlet@2025.1.13/dist/wvlet.min.js"></script>

<!-- Using latest version -->
<script src="https://cdn.jsdelivr.net/npm/@wvlet/highlightjs-wvlet@latest/dist/wvlet.min.js"></script>
```

## Usage

### With Node.js / CommonJS

```javascript
const hljs = require('highlight.js');
const wvlet = require('@wvlet/highlightjs-wvlet');

hljs.registerLanguage('wvlet', wvlet);

const highlightedCode = hljs.highlight(code, { language: 'wvlet' }).value;
```

### With ES6 Modules

```javascript
import hljs from 'highlight.js';
import wvlet from '@wvlet/highlightjs-wvlet';

hljs.registerLanguage('wvlet', wvlet);
```

### In the Browser

```html
<!DOCTYPE html>
<html>
<head>
  <!-- highlight.js theme -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/highlight.js@11/styles/default.min.css">
</head>
<body>
  <!-- Your code blocks -->
  <pre><code class="language-wvlet">
  model User = 
    from 'users.json'
    select name, age, email
  </code></pre>

  <!-- highlight.js core -->
  <script src="https://cdn.jsdelivr.net/npm/highlight.js@11/lib/core.min.js"></script>
  <!-- Wvlet language support -->
  <script src="https://cdn.jsdelivr.net/npm/@wvlet/highlightjs-wvlet@2025.1.13/dist/wvlet.min.js"></script>
  
  <script>
    // Wvlet language is auto-registered when loaded in browser
    // Just apply highlighting
    hljs.highlightAll();
  </script>
</body>
</html>
```

For a complete example, see [test-cdn.html](test-cdn.html).


## Building

To build the minified version for distribution:

```bash
npm install
npm run build
```

This will create `dist/wvlet.min.js`.

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
- [highlight.js](https://highlightjs.org)