---
sidebar_position: 8
---

# Syntax Highlighting Support

Wvlet provides syntax highlighting support for popular code highlighting libraries, making it easier to display Wvlet code with proper syntax coloring in documentation, websites, and development tools.

## Available Packages

### Prism.js Support

**Package**: [`@wvlet/prismjs-wvlet`](https://www.npmjs.com/package/@wvlet/prismjs-wvlet)

Prism.js is a lightweight, extensible syntax highlighter used by many documentation sites including Docusaurus.

#### Installation

```bash
npm install @wvlet/prismjs-wvlet prismjs
```

#### Usage

**Node.js / CommonJS:**
```javascript
const Prism = require('prismjs');
require('@wvlet/prismjs-wvlet');

const code = `model User = {
  from 'users.json'
  select name, age, email
}`;

const highlightedCode = Prism.highlight(code, Prism.languages.wvlet, 'wvlet');
```

**ES6 Modules:**
```javascript
import Prism from 'prismjs';
import '@wvlet/prismjs-wvlet';

const highlightedCode = Prism.highlight(code, Prism.languages.wvlet, 'wvlet');
```

**Browser (CDN):**
```html
<!-- Prism.js core and theme -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/prismjs@1/themes/prism.min.css">
<script src="https://cdn.jsdelivr.net/npm/prismjs@1/components/prism-core.min.js"></script>

<!-- Wvlet language support -->
<script src="https://cdn.jsdelivr.net/npm/@wvlet/prismjs-wvlet@latest/dist/prism-wvlet.min.js"></script>

<!-- Your code blocks -->
<pre><code class="language-wvlet">
model User = {
  from 'users.json'
  select name, age, email
}
</code></pre>

<script>
  Prism.highlightAll();
</script>
```

### Highlight.js Support

**Package**: [`@wvlet/highlightjs-wvlet`](https://www.npmjs.com/package/@wvlet/highlightjs-wvlet)

Highlight.js is a popular syntax highlighter with automatic language detection.

#### Installation

```bash
npm install @wvlet/highlightjs-wvlet highlight.js
```

#### Usage

**Node.js:**
```javascript
const hljs = require('highlight.js');
const wvlet = require('@wvlet/highlightjs-wvlet');

hljs.registerLanguage('wvlet', wvlet);

const code = `from orders
where status = 'completed'
group by customer_id
agg total_amount = sum(amount)`;

const result = hljs.highlight(code, { language: 'wvlet' });
```

**Browser:**
```html
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/highlight.js@11/styles/default.min.css">
<script src="https://cdn.jsdelivr.net/npm/highlight.js@11/highlight.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@wvlet/highlightjs-wvlet@latest/dist/wvlet.min.js"></script>

<pre><code class="language-wvlet">
from orders
where status = 'completed'
group by customer_id
agg total_amount = sum(amount)
</code></pre>

<script>
  hljs.highlightAll();
</script>
```

## Language Aliases

Both packages support multiple language aliases for flexibility:

- `wvlet` (primary)
- `wv` (short alias)

You can use either `class="language-wvlet"` or `class="language-wv"` in your HTML.

## Supported Syntax Features

The syntax highlighting packages recognize and highlight:

### Keywords and Operators
- **Flow operators**: `from`, `select`, `where`, `group by`, `order by`, `limit`, `agg`
- **Data operators**: `add`, `rename`, `exclude`, `shift`, `drop`, `describe`
- **Join types**: `join`, `left join`, `right join`, `full join`, `inner join`, `cross join`, `asof join`
- **Set operations**: `concat`, `intersect`, `except`, `union`, `distinct`
- **Control flow**: `if`, `then`, `else`, `case`, `when`

### Definitions
- **Models**: `model ModelName = { ... }`
- **Functions**: `def functionName(...) = { ... }`  
- **Types**: `type TypeName = ...`
- **Variables**: `val variableName = ...`

### Data Types and Literals
- **Numbers**: Integers, decimals, scientific notation (`42`, `3.14`, `1e6`)
- **Strings**: Single, double, and triple-quoted strings
- **String interpolation**: `"Hello ${name}"` and `"""Multi-line ${variable}"""`
- **Booleans**: `true`, `false`, `null`
- **Identifiers**: Backquoted identifiers `` `column name` ``

### Comments and Documentation
- **Line comments**: `-- This is a comment`
- **Doc comments**: `--- Documentation block ---`
- **Doc tags**: `@param`, `@return`, etc.

### Functions and Expressions
- **Built-in functions**: `count`, `sum`, `avg`, `max`, `min`, etc.
- **Aggregations**: `_.count`, `_.sum`, `_.avg`
- **Method chaining**: `column.sum().round(2)`

### Test Syntax
- **Test definitions**: `test "description" should { ... }`
- **Assertions**: `should be`, `should contain`, `should not contain`

## Integration Examples

### Docusaurus Integration

If you're using Docusaurus, you can integrate Wvlet syntax highlighting by swizzling the `prism-include-languages` component:

1. Install the package:
   ```bash
   npm install @wvlet/prismjs-wvlet
   ```

2. Swizzle the component:
   ```bash
   npx docusaurus swizzle @docusaurus/theme-classic prism-include-languages --eject
   ```

3. Update `src/theme/prism-include-languages.ts`:
   ```typescript
   // Add after the additionalLanguages.forEach loop
   // eslint-disable-next-line global-require
   require('@wvlet/prismjs-wvlet');
   ```

4. Use in your markdown files:
   ````markdown
   ```wvlet
   model Sales = {
     from 'sales.parquet'
     where date >= '2024-01-01'
     group by product_category
     agg total_revenue = sum(amount)
   }
   ```
   ````

### GitHub Integration

GitHub automatically detects `.wv` files and applies basic syntax highlighting. For richer highlighting in README files and documentation, you can use the language tags:

````markdown
```wvlet
-- Query example
from users
where age >= 18
select name, email, registration_date
order by registration_date desc
limit 100
```
````

## Development

Both packages are open source and maintained in the [Wvlet repository](https://github.com/wvlet/wvlet):

- **Prism.js**: [`prismjs-wvlet/`](https://github.com/wvlet/wvlet/tree/main/prismjs-wvlet)
- **Highlight.js**: [`highlightjs-wvlet/`](https://github.com/wvlet/wvlet/tree/main/highlightjs-wvlet)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/wvlet/wvlet.git
cd wvlet

# Build Prism.js package
cd prismjs-wvlet
npm install
npm run build

# Build Highlight.js package  
cd ../highlightjs-wvlet
npm install
npm run build
```

### Contributing

Contributions are welcome! If you find syntax highlighting issues or want to add support for new Wvlet language features:

1. Fork the repository
2. Create a feature branch
3. Update the grammar definitions in the respective packages
4. Add test cases
5. Submit a pull request

See the [contribution guidelines](https://github.com/wvlet/wvlet/blob/main/CONTRIBUTING.md) for more details.

## Resources

- **Prism.js Documentation**: [https://prismjs.com/](https://prismjs.com/)
- **Highlight.js Documentation**: [https://highlightjs.org/](https://highlightjs.org/)
- **Wvlet Syntax Guide**: [Query Syntax](../syntax/)
- **Package Source Code**: [GitHub Repository](https://github.com/wvlet/wvlet)