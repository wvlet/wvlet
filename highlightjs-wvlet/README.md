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
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/highlight.js@11/styles/default.min.css">
<script src="https://cdn.jsdelivr.net/npm/highlight.js@11"></script>
<script src="https://cdn.jsdelivr.net/npm/@wvlet/highlightjs-wvlet@latest/dist/wvlet.min.js"></script>
<script>
  hljs.registerLanguage('wvlet', window.hljsDefineWvlet);
  hljs.highlightAll();
</script>
```

## Example

```wvlet
-- Define a model from JSON file
model person = {
  from 'person.json'
}

-- Query with filtering and aggregation
from person
where age >= 18
group by department
agg
  employee_count = count(*),
  avg_salary = avg(salary)
order by employee_count desc
limit 10

-- Join operations
from orders
left join customers on orders.customer_id = customers.id
select
  order_id = orders.id,
  customer_name = customers.name,
  total_amount = orders.amount * (1 - orders.discount)

-- Test assertions
test _.size should be 100
test _.columns should contain 'user_id'
```

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