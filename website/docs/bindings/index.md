
# Language Bindings

Wvlet provides SDKs for various programming languages. The Python and TypeScript SDKs compile Wvlet queries to SQL from your language; the Java / Scala API runs queries in your application on Wvlet's embedded engine.

## Available SDKs

### [Python SDK](python.md) 
[![PyPI version](https://badge.fury.io/py/wvlet.svg)](https://pypi.org/project/wvlet/)

The Python SDK provides native performance with easy installation via pip:

```bash
pip install wvlet
```

```python
from wvlet import compile

sql = compile("from users select name, age where age > 18")
print(sql)  # SELECT name, age FROM users WHERE age > 18
```

**Features:**
- Native library for high-performance compilation
- Automatic CLI fallback
- Support for all Wvlet features
- Integration with pandas, DuckDB, SQLAlchemy

### [TypeScript/JavaScript SDK](typescript.md)
[![npm version](https://badge.fury.io/js/@wvlet%2Fwvlet.svg)](https://www.npmjs.com/package/@wvlet/wvlet)

The TypeScript SDK enables Wvlet compilation in Node.js and browsers:

```bash
npm install @wvlet/wvlet
```

```typescript
import { WvletCompiler } from '@wvlet/wvlet';

const compiler = new WvletCompiler();
const sql = compiler.compile('from users select name, age where age > 18');
console.log(sql);  // SELECT name, age FROM users WHERE age > 18
```

**Features:**
- Pure JavaScript via Scala.js compilation
- Works in browsers and Node.js
- Full TypeScript support
- No native dependencies

### [Java / Scala (embedded engine)](jvm.md)

Run queries in-process on the JVM, with no server or database:

```scala
import wvlet.lang.embedded.EmbeddedEngine

val engine = EmbeddedEngine.open()
engine.registerTable("orders", "data/orders.parquet")
val result = engine.query("from orders group by o_orderstatus agg _.count as n")
println(result.toJson)
engine.close()
```

**Features:**
- Wvlet's columnar engine in your process: Parquet, Iceberg, JSON, and CSV tables
- Works from Scala and Java (`AutoCloseable`, index-based result accessors)
- Rows as JVM values or JSON

## Planned SDKs

The following language bindings are planned for future releases:

- **Ruby** - Ruby gem with native extensions
- **Rust** - Cargo crate with native bindings
- **Go** - Go module with CGO bindings
- **C/C++** - Header-only library

## Native Library

All language SDKs use the same underlying native library (`libwvlet`) for consistent behavior and performance across languages. The native library is built using Scala Native and provides a C-compatible API.

### Supported Platforms

| Platform | x86_64 | ARM64 |
|----------|--------|-------|
| Linux    | ✅     | ✅    |
| macOS    | ❌     | ✅    |
| Windows  | ✅     | ✅    |

✅ Supported | ❌ Not Supported

## Contributing

We welcome contributions to add more language bindings! If you're interested in creating an SDK for your favorite language:

1. Check the [Python SDK implementation](https://github.com/wvlet/wvlet/tree/main/sdks/python) as a reference
2. Use the native library API defined in [WvcLib.scala](https://github.com/wvlet/wvlet/blob/main/wvc/src/main/scala/wvlet/lang/native/WvcLib.scala)
3. Follow the language's best practices for packaging and distribution
4. Submit a pull request with your implementation

For questions or discussions about language bindings, please use [GitHub Discussions](https://github.com/wvlet/wvlet/discussions).
