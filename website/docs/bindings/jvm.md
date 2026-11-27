# Java / Scala (embedded engine)

Run Wvlet queries inside a JVM application on Wvlet's own columnar [engine](../usage/wvlet-engine.md), the way you would embed DuckDB or SQLite. The engine runs in-process: no server, no database, and no SQL generation. Register tables from files, run a query, and read the rows.

## Installation

The JVM artifacts are not published to Maven Central yet. Build them from a Wvlet checkout and publish them to your local repository:

```bash
git clone https://github.com/wvlet/wvlet.git
cd wvlet
./sbt runnerJVM/publishLocal
```

Then depend on `wvlet-runner` (Scala 3) with the version that `publishLocal` printed:

```scala
libraryDependencies += "wvlet.lang" %% "wvlet-runner" % "<version>"
```

## Scala

```scala
import wvlet.lang.embedded.WvletEmbed

val engine = WvletEmbed.open("data") // relative paths resolve against data/
try
  engine.registerTable("orders", "orders.parquet")
  val result = engine.query("""
    from orders
    group by o_orderstatus
    agg _.count as n
    order by o_orderstatus
  """)
  result.columns.foreach(c => println(s"${c.name}: ${c.typeName}"))
  result.rows.foreach(println)
  println(result.toJson)
finally engine.close()
```

## Java

`WvletEmbed` is `AutoCloseable`, and the result has index-based accessors:

```java
import wvlet.lang.embedded.WvletEmbed;
import wvlet.lang.embedded.WvletEmbedResult;

try (WvletEmbed engine = WvletEmbed.open("data")) {
  engine.registerTable("region", "region.parquet");
  WvletEmbedResult result = engine.query("from region agg _.count as n");
  for (int i = 0; i < result.columnCount(); i++) {
    System.out.println(result.columnName(i) + ": " + result.columnType(i));
  }
  long n = (Long) result.get(0, 0);
}
```

## Tables

- `registerTable(name, path)` registers a file or folder by name: Parquet (a file, a glob such as `sales/*.parquet`, or a folder of files, Hive-partitioned or not), an Iceberg table (its folder or `metadata.json`), JSON Lines, or CSV / TSV, optionally gzip-compressed. Each name can be registered once per engine.
- Parquet tables are typed when a query compiles, so an unknown column fails before anything runs.
- A query can also read a file without registering it: `from 'data/orders.parquet'`.
- `save to <table>` and `create table` keep tables in the engine for later queries; `tableNames` lists every table.

## Queries and results

- `query(text)` runs every statement in order (`save to`, `create table`, and `test` assertions included) and returns the rows of the last query. Text with no query, such as a lone `save to`, returns an empty result.
- A syntax error, an unknown column, a failed statement, or a failed `test` throws `WvletLangException`, with a status code (`e.statusCode`) and the source location.
- Each column has a name and a Wvlet type name (`int`, `long`, `double`, `string`, `decimal[15,2]`, `date`, ...).
- Values are `Long` for integers, `Double`, `Boolean`, and `String`. Decimals, dates, and timestamps are strings, so no precision is lost. Arrays are `Seq`s, maps are `Map`s, and NULL is `null`.
- `toJson` returns `{"columns":[{"name":...,"type":...}],"rows":[[...],...]}` with the same values.
- One engine runs one query at a time. Open an engine per thread to run queries in parallel. `close()` releases the engine; later calls fail.
