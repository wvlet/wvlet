/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.SourceIO
import wvlet.lang.engine.catalog.CsvSource
import wvlet.lang.engine.catalog.DataFileFormat
import wvlet.lang.engine.catalog.DataFileWriter
import wvlet.lang.engine.catalog.IcebergWriter
import wvlet.lang.engine.catalog.JsonSource
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import scala.collection.mutable

/**
  * Writes query results into the in-memory tables of a [[WvletEngine]] catalog: `save to`,
  * `append to` (plain and keyed), and `delete`. Tables backed by files are read-only: writes to
  * them fail instead of silently replacing the files' rows with an in-memory copy
  */
class TableWriter(engine: WvletEngine) extends LogSupport:
  import TableWriter.*

  private def catalog = engine.catalog

  // Declared shapes (defaults and constraints) of tables made by `create table`, by lower-case name
  private val specs = mutable.HashMap.empty[String, TableSpec]

  private def specOf(name: String): Option[TableSpec] = specs.synchronized(
    specs.get(name.toLowerCase)
  )

  private def defaultsOf(name: String, schema: Schema): IndexedSeq[Any] =
    specOf(name) match
      case Some(spec) if spec.columns.size == schema.size =>
        spec.columns.map(_.default).toIndexedSeq
      case _ =>
        IndexedSeq.fill(schema.size)(null)

  /**
    * `create table <name>`: create an empty in-memory table with the declared columns. With
    * `ifNotExists`, an existing table is kept (returns None); with `replace`, it is dropped first;
    * otherwise an existing table is an error
    */
  def createTable(
      name: String,
      spec: TableSpec,
      ifNotExists: Boolean = false,
      replace: Boolean = false
  ): Option[MemTable] =
    catalog.find(name) match
      case Some(_) if ifNotExists =>
        None
      case Some(_) if !replace =>
        throw StatusCode.TABLE_ALREADY_EXISTS.newException(s"Table ${name} already exists")
      case existing =>
        existing.foreach(t => writable(t, "replace"))
        specs.synchronized(specs(name.toLowerCase) = spec)
        Some(register(MemTable(name, spec.schema, Nil)))

  /** `drop table <name>`: remove the table; returns false when it doesn't exist and `ifExists` */
  def dropTable(name: String, ifExists: Boolean = false): Boolean =
    catalog.find(name) match
      case None if ifExists =>
        false
      case None =>
        throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${name}")
      case Some(t) =>
        // Dropping a table registered from files unregisters it; the files are left as they are
        catalog.drop(name)
        specs.synchronized(specs.remove(name.toLowerCase))
        true

  /** `truncate <name>`: remove every row, keeping the table's columns and constraints */
  def truncate(name: String): MemTable =
    val table = writable(catalog.get(name), "truncate")
    register(MemTable(table.name, table.schema, Nil))

  /**
    * `save to '<path>' [with options]`: write the query result to a Parquet, CSV, TSV, or JSON
    * lines file chosen by the extension (or the `format` option), replacing the file
    */
  def saveToFile(path: String, query: Relation, options: Map[String, Any] = Map.empty)(using
      Context
  ): Unit =
    val result = engine.execute(query)
    DataFileWriter.write(path, unqualified(result.schema), result.batches, options)

  /**
    * `append to '<path>'`: add the query rows to a data file, creating it when it doesn't exist.
    * Rows map to the file's columns by position. CSV / TSV / JSON lines files get the new rows
    * appended as text, leaving the existing rows untouched; Parquet files are rewritten with the
    * new rows cast to the file's column types, through a temporary file. A folder holding an
    * Iceberg table gets the rows, cast to the table's column types, as a new snapshot
    */
  def appendToFile(path: String, query: Relation)(using Context): Unit =
    if path.toLowerCase.endsWith(".metadata.json") then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(
          s"Cannot append to the Iceberg metadata file ${path}: append to the table folder instead"
        )
    val result = engine.execute(query)
    if IcebergWriter.isTable(path) then
      val schema   = unqualified(ParquetSource(path, path).schema)
      val sourceOf = positionalSources(path, schema, result.schema, Nil)
      IcebergWriter.append(
        path,
        schema,
        result.batches.map(b => castBatch(b, result.schema, schema, sourceOf))
      )
    else if !SourceIO.existsFile(path) then
      DataFileWriter.write(path, unqualified(result.schema), result.batches)
    else
      DataFileFormat.of(path) match
        case DataFileFormat.Parquet =>
          val existing = ParquetSource(path, path)
          val schema   = unqualified(existing.schema)
          val sourceOf = positionalSources(path, schema, result.schema, Nil)
          // Read the whole file before replacing it
          val oldBatches = batchesOf(existing).toIndexedSeq
          val newBatches = result
            .batches
            .map(b =>
              castBatch(b, result.schema, schema, sourceOf, IndexedSeq.fill(schema.size)(null))
            )
          DataFileWriter.replace(path, schema, oldBatches ++ newBatches)
        case format =>
          // The file's column names (JSON keys, CSV header); values keep their query types
          val fileSchema =
            format match
              case DataFileFormat.JsonLines =>
                JsonSource(path, path).schema
              case _ =>
                CsvSource(path, path).schema
          positionalSources(path, fileSchema, result.schema, Nil)
          val names  = fileSchema.names
          val schema = Schema(
            result.schema.fields.zipWithIndex.map((f, i) => f.noQualifier.withName(names(i)))
          )
          DataFileWriter.appendText(path, schema, result.batches)
    end if

  end appendToFile

  /**
    * `save to <name>`: create or replace the table with the query result. With `ifNotExists` (`save
    * to t if not exists`), an existing table is kept as is
    */
  def saveTo(name: String, query: Relation, ifNotExists: Boolean = false)(using
      Context
  ): TableSource =
    catalog.find(name) match
      case Some(existing) if ifNotExists =>
        existing
      case _ =>
        replaceWith(name, query)

  private def replaceWith(name: String, query: Relation)(using Context): MemTable =
    val result = engine.execute(query)
    // A replaced table takes the query's shape, without the declared defaults and constraints
    specs.synchronized(specs.remove(name.toLowerCase))
    register(MemTable(name, unqualified(result.schema), result.batches))

  /**
    * `append to <name> [(columns)] [on keys]`: insert the query rows into the table, creating the
    * table when it doesn't exist. Rows map to the table columns by position, or to `columns` when
    * given (other columns get their declared default, or NULL), and are cast to the column types.
    * Rows violating the table's NOT NULL or unique constraints fail the append.
    *
    * With `keys`, the append is an insert-or-update (SQL MERGE): query columns map to table columns
    * by name, a row whose non-NULL key values match existing rows updates their mapped columns, and
    * the other rows insert
    */
  def appendTo(name: String, query: Relation, columns: Seq[String] = Nil, keys: Seq[String] = Nil)(
      using Context
  ): MemTable =
    val result = engine.execute(query)
    catalog.find(name) match
      case None =>
        register(MemTable(name, unqualified(result.schema), result.batches))
      case Some(target) =>
        val table    = writable(target, "append to")
        val schema   = table.schema
        val defaults = defaultsOf(name, schema)
        if keys.isEmpty then
          val sourceOf = positionalSources(name, schema, result.schema, columns)
          val added    = result
            .batches
            .map(b => castBatch(b, result.schema, schema, sourceOf, defaults))
          val batches = batchesOf(table) ++ added
          // Existing rows were checked when they were written: NOT NULL needs only the new ones
          checkConstraints(name, schema, batches, notNullOf = added)
          register(MemTable(table.name, schema, batches))
        else
          val batches = Seq(
            toBatch(schema, upsert(name, schema, table, result, columns, keys, defaults))
          )
          checkConstraints(name, schema, batches, notNullOf = batches)
          register(MemTable(table.name, schema, batches))

  /**
    * `delete`: remove the rows of the table that satisfy the filters of `input` (all rows when it
    * has none). `input` is the delete's input relation: filters over a single table scan
    */
  def delete(name: String, input: Relation)(using Context): MemTable =
    val table         = writable(catalog.get(name), "delete")
    val (scan, conds) = deleteInput(input)
    val kept          =
      conds match
        case Nil =>
          Nil
        case _ =>
          val cond = conds.reduce((a, b) => And(a, b, NoSpan))
          // Keep the rows whose condition is not TRUE: false and NULL rows survive, as in SQL
          val keep = Not(
            FunctionApply(
              UnquotedIdentifier("coalesce", NoSpan),
              List(
                FunctionArg(None, cond, span = NoSpan),
                FunctionArg(None, FalseLiteral(NoSpan), span = NoSpan)
              ),
              None,
              span = NoSpan
            ),
            NoSpan
          )
          engine.execute(Filter(scan, keep, NoSpan)).batches
    register(MemTable(table.name, table.schema, kept))

  private def register(t: MemTable): MemTable =
    catalog.register(t)
    t

  private def unqualified(schema: Schema): Schema = schema.mapFields(_.noQualifier)

  private def writable(source: TableSource, op: String): MemTable =
    source match
      case m: MemTable =>
        m
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"Cannot ${op} table ${other
                .name}: tables backed by files are read-only on the wvlet engine"
          )

  private def batchesOf(table: TableSource): Seq[Batch] = table.scan(ScanSpec()).toSeq

  private def indexOfColumn(names: Seq[String], name: String): Int = names.indexWhere(
    _.equalsIgnoreCase(name)
  )

  private def requireColumns(name: String, schema: Schema, columns: Seq[String]): Unit = columns
    .foreach { c =>
      if indexOfColumn(schema.names, c) < 0 then
        throw StatusCode.COLUMN_NOT_FOUND.newException(s"Column ${c} is not found in ${name}")
    }

  // For each table column, the query column that fills it (-1: NULL), as in INSERT INTO
  private def positionalSources(
      name: String,
      schema: Schema,
      from: Schema,
      columns: Seq[String]
  ): Array[Int] =
    if columns.isEmpty then
      if from.size != schema.size then
        throw StatusCode
          .SYNTAX_ERROR
          .newException(
            s"Table ${name} has ${schema.size} columns but the appended rows have ${from.size}"
          )
      Array.tabulate(schema.size)(identity)
    else
      if columns.size != from.size then
        throw StatusCode
          .SYNTAX_ERROR
          .newException(
            s"${columns.size} target columns are given but the appended rows have ${from.size}"
          )
      requireColumns(name, schema, columns)
      Array.tabulate(schema.size)(i => indexOfColumn(columns, schema(i).name))

  // Re-arrange and cast the columns of a query batch into the table schema
  private def castBatch(
      b: Batch,
      from: Schema,
      to: Schema,
      sourceOf: Array[Int],
      defaults: IndexedSeq[Any]
  ): Batch =
    val cols =
      Array.tabulate(to.size) { i =>
        val src   = sourceOf(i)
        val ptype = to(i).ptype
        if src < 0 && defaults(i) == null then
          Vec.nullVec(ptype, b.size)
        else if src < 0 then
          val out = VecBuilder(ptype, b.size)
          var r   = 0
          while r < b.size do
            out.add(defaults(i))
            r += 1
          out.result()
        else if from(src).ptype == ptype then
          b.columns(src)
        else
          val v   = b.columns(src)
          val out = VecBuilder(ptype, b.size)
          var r   = 0
          while r < b.size do
            out.add(ValueOps.cast(v.get(r), from(src).ptype, ptype))
            r += 1
          out.result()
      }
    Batch(cols, b.size)

  end castBatch

  private def upsert(
      name: String,
      schema: Schema,
      table: TableSource,
      result: EngineResult,
      columns: Seq[String],
      keys: Seq[String],
      defaults: IndexedSeq[Any]
  ): IndexedSeq[Array[Any]] =
    // Query columns are matched to table columns by name, as MERGE reads `src.<column>`
    val sourceNames =
      if columns.nonEmpty then
        columns
      else
        result.schema.names
    if sourceNames.size != result.schema.size then
      throw StatusCode
        .SYNTAX_ERROR
        .newException(
          s"${columns.size} target columns are given but the appended rows have ${result
              .schema
              .size}"
        )
    requireColumns(name, schema, columns)
    val sourceOf = Array.tabulate(schema.size)(i => indexOfColumn(sourceNames, schema(i).name))
    val keyIdx   = keys.map { k =>
      val i = indexOfColumn(schema.names, k)
      if i < 0 || sourceOf(i) < 0 then
        throw StatusCode.SYNTAX_ERROR.newException(s"Key column(s) ${k} not found in table ${name}")
      i
    }
    // NULL never equals NULL in a MERGE condition, so rows with a NULL key never match
    def keyOf(r: Array[Any]): Option[Seq[Any]] =
      val k = keyIdx.map(r(_))
      Option.when(!k.contains(null))(k)

    val rows = mutable.ArrayBuffer.empty[Array[Any]]
    batchesOf(table).foreach(b => Iterator.range(0, b.size).foreach(i => rows += b.row(i)))
    // Positions of the existing rows by key; rows inserted by this append are not matched again
    val positions = mutable.HashMap.empty[Seq[Any], mutable.ArrayBuffer[Int]]
    rows
      .indices
      .foreach(i =>
        keyOf(rows(i)).foreach(k => positions.getOrElseUpdate(k, mutable.ArrayBuffer.empty) += i)
      )
    val fromTypes = result.schema.types
    result
      .rows
      .foreach { r =>
        val incoming =
          Array.tabulate[Any](schema.size) { i =>
            val src = sourceOf(i)
            if src < 0 then
              defaults(i)
            else
              ValueOps.cast(r(src), fromTypes(src), schema(i).ptype)
          }
        keyOf(incoming).flatMap(positions.get) match
          case Some(matched) =>
            // Update the mapped columns of every matched row; unmapped columns keep their values
            matched.foreach { p =>
              val updated = rows(p).clone()
              sourceOf
                .indices
                .foreach(i =>
                  if sourceOf(i) >= 0 then
                    updated(i) = incoming(i)
                )
              rows(p) = updated
            }
          case None =>
            rows += incoming
      }
    rows.toIndexedSeq

  end upsert

  // NOT NULL and unique (primary key) constraints of a table made by `create table`
  private def checkConstraints(
      name: String,
      schema: Schema,
      batches: Seq[Batch],
      notNullOf: Seq[Batch]
  ): Unit = specOf(name)
    .filter(_.columns.size == schema.size)
    .foreach { spec =>
      spec
        .columns
        .zipWithIndex
        .filter(_._1.notNull)
        .foreach { (c, i) =>
          if notNullOf.exists(b => Iterator.range(0, b.size).exists(b.column(i).isNull)) then
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(
                s"NULL value in column ${c.name} of ${name} violates its NOT NULL constraint"
              )
        }
      spec
        .uniqueKeys
        .foreach { key =>
          val idx  = key.map(k => indexOfColumn(schema.names, k))
          val seen = mutable.HashSet.empty[Seq[Any]]
          batches.foreach { b =>
            var r = 0
            while r < b.size do
              val k = idx.map(i => b.column(i).get(r))
              // NULLs never collide in a unique constraint
              if !k.contains(null) && !seen.add(k) then
                throw StatusCode
                  .INVALID_ARGUMENT
                  .newException(
                    s"Duplicate value (${k.mkString(
                        ", "
                      )}) in ${name} violates its unique constraint on (${key.mkString(", ")})"
                  )
              r += 1
          }
        }
    }

  end checkConstraints

  private def toBatch(schema: Schema, rows: IndexedSeq[Array[Any]]): Batch =
    val cols =
      Array.tabulate(schema.size) { c =>
        val b = VecBuilder(schema(c).ptype, rows.size)
        rows.foreach(r => b.add(r(c)))
        b.result()
      }
    Batch(cols, rows.size)

  // The table scan and filter conditions of a delete input (`from t where ... delete`)
  private def deleteInput(r: Relation): (Relation, List[Expression]) =
    r match
      case q: Query =>
        deleteInput(q.child)
      case f: Filter =>
        val (scan, conds) = deleteInput(f.child)
        (scan, f.filterExpr :: conds)
      case l: LeafPlan =>
        (l, Nil)
      case other =>
        throw StatusCode.SYNTAX_ERROR.newException(s"Unsupported delete input: ${other.nodeName}")

end TableWriter

object TableWriter:
  /** A declared column of a table made by `create table` */
  case class ColumnSpec(name: String, ptype: PType, default: Any = null, notNull: Boolean = false)

  /** The declared shape of a table: its columns and unique keys (primary key included) */
  case class TableSpec(columns: Seq[ColumnSpec], uniqueKeys: Seq[Seq[String]] = Nil):
    def schema: Schema = Schema(columns.map(c => Field(c.name, c.ptype)).toIndexedSeq)
