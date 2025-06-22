# TypeScript/JavaScript SDK

The Wvlet TypeScript SDK provides a native JavaScript implementation of the Wvlet compiler, allowing you to compile Wvlet queries to SQL directly in Node.js or browser environments.

## Installation

```bash
npm install wvlet
# or
yarn add wvlet
# or
pnpm add wvlet
```

## Quick Start

```typescript
import { WvletCompiler } from 'wvlet';

const compiler = new WvletCompiler();
const sql = compiler.compile('from users select name, email');
console.log(sql);
// Output: SELECT name, email FROM users
```

## Features

- **Pure JavaScript**: Compiled from Scala.js, no native dependencies required
- **Type Safety**: Full TypeScript support with comprehensive type definitions
- **Browser Support**: Works in both Node.js and modern browsers
- **Multiple Targets**: Support for DuckDB and Trino SQL dialects
- **Detailed Errors**: Rich error messages with source location information
- **Small API Surface**: Simple and intuitive API design

## Usage

### Basic Usage

```typescript
import { WvletCompiler } from 'wvlet';

// Create a compiler instance
const compiler = new WvletCompiler({
  target: 'duckdb' // or 'trino'
});

// Compile queries
const sql = compiler.compile('from orders where total > 100 select *');
```

### Error Handling

```typescript
import { WvletCompiler, CompilationError } from 'wvlet';

const compiler = new WvletCompiler();

try {
  const sql = compiler.compile('invalid query syntax');
} catch (error) {
  if (error instanceof CompilationError) {
    console.error(`Error: ${error.message}`);
    console.error(`Status: ${error.statusCode}`);
    if (error.location) {
      console.error(`Location: line ${error.location.line}, column ${error.location.column}`);
    }
  }
}
```

### Convenience Function

```typescript
import { compile } from 'wvlet';

// Quick compilation with default settings
const sql = compile('from users select count(*)');
```

## API Reference

### `WvletCompiler`

The main compiler class for Wvlet queries.

#### Constructor

```typescript
new WvletCompiler(options?: CompileOptions)
```

**Options:**
- `target`: Target SQL dialect (`'duckdb'` | `'trino'`). Default: `'duckdb'`
- `profile`: Profile name for configuration

#### Methods

##### `compile(query: string, options?: CompileOptions): string`

Compiles a Wvlet query to SQL.

**Parameters:**
- `query`: The Wvlet query string
- `options`: Optional compilation options to override defaults

**Returns:** The compiled SQL string

**Throws:** `CompilationError` if the query is invalid

##### `static getVersion(): string`

Returns the version of the Wvlet compiler.

### Types

#### `CompileOptions`

```typescript
interface CompileOptions {
  target?: 'duckdb' | 'trino';
  profile?: string;
}
```

#### `CompilationError`

```typescript
class CompilationError extends Error {
  statusCode: string;
  location?: ErrorLocation;
}
```

#### `ErrorLocation`

```typescript
interface ErrorLocation {
  path: string;
  fileName: string;
  line: number;      // 1-based
  column: number;    // 1-based
  lineContent?: string;
}
```

## Examples

### Query with JOIN

```typescript
const sql = compiler.compile(`
  from users u
  join orders o on u.id = o.user_id
  select u.name, count(*) as order_count
  group by u.name
`);
```

### Query with CTE

```typescript
const sql = compiler.compile(`
  with recent_orders as (
    from orders
    where created_at > current_date - interval '7 days'
    select *
  )
  from recent_orders
  select customer_id, sum(total) as weekly_total
  group by customer_id
`);
```

### Window Functions

```typescript
const sql = compiler.compile(`
  from sales
  select 
    product_id,
    sale_date,
    amount,
    sum(amount) over (partition by product_id order by sale_date) as running_total
`);
```

## Browser Usage

The SDK works directly in browsers that support ES modules:

```html
<script type="module">
  import { WvletCompiler } from 'https://unpkg.com/wvlet/dist/index.js';
  
  const compiler = new WvletCompiler();
  const sql = compiler.compile('from users select *');
  console.log(sql);
</script>
```

## Performance Considerations

- **Bundle Size**: The compiled Scala.js output is approximately 9MB (uncompressed)
- **Compilation Speed**: Compilation is performed synchronously and is typically fast for most queries
- **Memory Usage**: The compiler maintains minimal state between compilations

## Differences from Python SDK

Unlike the Python SDK which uses a native library, the TypeScript SDK:
- Runs purely in JavaScript (via Scala.js compilation)
- Works in browser environments
- Has no native dependencies
- Provides synchronous-only API

## Troubleshooting

### Module Resolution Issues

If you encounter module resolution issues, ensure your `tsconfig.json` includes:

```json
{
  "compilerOptions": {
    "moduleResolution": "node",
    "esModuleInterop": true
  }
}
```

### Browser Compatibility

The SDK requires browsers that support:
- ES2020 features
- ES modules
- Dynamic imports (for code splitting)

## Source Code

The TypeScript SDK source code is available at:
- [GitHub: sdks/typescript](https://github.com/wvlet/wvlet/tree/main/sdks/typescript)
- [NPM: wvlet](https://www.npmjs.com/package/wvlet)