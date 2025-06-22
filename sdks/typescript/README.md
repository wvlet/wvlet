# Wvlet TypeScript SDK

TypeScript/JavaScript SDK for [Wvlet](https://wvlet.org/) - A flow-style query language that compiles to SQL.

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

// Compile a Wvlet query to SQL
const sql = await compiler.compile('from users select name, email');
console.log(sql);
// Output: SELECT name, email FROM users
```

## Usage

### Basic Compilation

```typescript
import { WvletCompiler } from 'wvlet';

const compiler = new WvletCompiler({
  target: 'duckdb' // or 'trino'
});

// Async compilation
const sql = await compiler.compile('from users where age > 18 select *');

// Sync compilation
const sqlSync = compiler.compileSync('from products select name, price');
```

### Error Handling

```typescript
import { WvletCompiler, CompilationError } from 'wvlet';

const compiler = new WvletCompiler();

try {
  const sql = await compiler.compile('invalid query syntax');
} catch (error) {
  if (error instanceof CompilationError) {
    console.error(`Error at line ${error.location?.line}, column ${error.location?.column}`);
    console.error(`Message: ${error.message}`);
    console.error(`Status: ${error.statusCode}`);
  }
}
```

### Convenience Function

```typescript
import { compile } from 'wvlet';

// Use the default compiler with a single function call
const sql = await compile('from orders select count(*)');
```

## API Reference

### WvletCompiler

#### Constructor

```typescript
new WvletCompiler(options?: CompileOptions)
```

Options:
- `target`: Target SQL dialect ('duckdb' | 'trino'). Default: 'duckdb'
- `profile`: Profile name for configuration

#### Methods

##### compile(query: string, options?: CompileOptions): Promise<string>

Compiles a Wvlet query to SQL asynchronously.

##### compileSync(query: string, options?: CompileOptions): string

Compiles a Wvlet query to SQL synchronously.

##### static getVersion(): string

Returns the version of the Wvlet compiler.

## Examples

### Query with JOIN

```typescript
const sql = await compiler.compile(`
  from users u
  join orders o on u.id = o.user_id
  select u.name, count(*) as order_count
  group by u.name
`);
```

### Query with CTE

```typescript
const sql = await compiler.compile(`
  with active_users as (
    from users
    where last_login > current_date - interval '30 days'
    select *
  )
  from active_users
  select name, email
`);
```

## Browser Support

This SDK works in both Node.js and modern browsers that support ES modules.

## License

Apache License 2.0

## Contributing

See the [main Wvlet repository](https://github.com/wvlet/wvlet) for contribution guidelines.