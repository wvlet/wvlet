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
import wvlet.lang.engine.catalog.ManagedTable
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.catalog.Warehouse
import wvlet.lang.engine.plan.ScanFilters
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.*
import wvlet.uni.parquet.IcebergTableChange
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
class TableWriter(engine: WvletEngine, warehouse: Option[Warehouse] = None) extends LogSupport:
  import TableWriter.*

  private def catalog = engine.catalog

  // Declared shapes (defaults and constraints) of tables made by `create table`, by lower-case name.
  // A warehouse table keeps its shape in a table property, read when first needed
  private val specs = mutable.HashMap.empty[String, Option[TableSpec]]

  private def specOf(name: String): Option[TableSpec] = specs.synchronized(
    specs.getOrElseUpdate(
      name.toLowerCase,
      catalog.find(name) match
        case Some(m: ManagedTable) =>
          m.properties.get(SpecProperty).filter(_.nonEmpty).map(specFromJson(_, m.schema))
        case _ =>
          None
    )
  )

  private def setSpec(name: String, spec: Option[TableSpec]): Unit = specs.synchronized(
    specs(name.toLowerCase) = spec
  )

  // The property that stores a warehouse table's declared shape; empty when it has none
  private def specProperties(spec: Option[TableSpec]): Map[String, String] = Map(
    SpecProperty -> spec.map(specToJson).getOrElse("")
  )

  /**
    * Where a new table goes: the warehouse when there is one, so it outlives the session, or an
    * in-memory table
    */
  private def persistent: Boolean = warehouse.isDefined

  private def wh: Warehouse = warehouse.get

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
  ): Option[TableSource] =
    catalog.find(name) match
      case Some(_) if ifNotExists =>
        None
      case Some(_) if !replace =>
        throw StatusCode.TABLE_ALREADY_EXISTS.newException(s"Table ${name} already exists")
      case existing =>
        existing.foreach(t => writable(t, "replace"))
        // The spec is kept only once the table exists, so a failed create leaves no stale spec
        val created =
          if persistent then
            wh.create(name, spec.schema, specProperties(Some(spec)), replace = existing.isDefined)
          else
            MemTable(name, spec.schema, Nil)
        setSpec(name, Some(spec))
        Some(register(created))

  /** `drop table <name>`: remove the table; returns false when it doesn't exist and `ifExists` */
  def dropTable(name: String, ifExists: Boolean = false): Boolean =
    catalog.find(name) match
      case None if ifExists =>
        false
      case None =>
        throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${name}")
      case Some(t) =>
        // A warehouse table is deleted; dropping a table registered from files unregisters it, and
        // the files are left as they are
        t match
          case _: ManagedTable =>
            wh.drop(name)
          case _ =>
        catalog.drop(name)
        specs.synchronized(specs.remove(name.toLowerCase))
        true

  /** `truncate <name>`: remove every row, keeping the table's columns and constraints */
  def truncate(name: String): TableSource =
    writable(catalog.get(name), "truncate") match
      case m: ManagedTable =>
        register(wh.replace(name, unqualified(m.schema), Nil))
      case table =>
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
        result
          .batches
          .map(b =>
            castBatch(b, result.schema, schema, sourceOf, IndexedSeq.fill(schema.size)(null))
          )
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

  private def replaceWith(name: String, query: Relation)(using Context): TableSource =
    val existing = catalog.find(name)
    // Only the warehouse's own tables are replaced there; a table registered from files stays
    // read-only rather than gaining a warehouse table of the same name
    existing.filter(_ => persistent).foreach(t => writable(t, "save to"))
    val result = engine.execute(query)
    // A replaced table takes the query's shape, without the declared defaults and constraints
    val hadSpec = specOf(name).isDefined
    setSpec(name, None)
    if persistent then
      val props =
        if hadSpec then
          specProperties(None)
        else
          Map.empty[String, String]
      register(wh.replace(name, unqualified(result.schema), result.batches, props))
    else
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
  ): TableSource =
    val result = engine.execute(query)
    catalog.find(name) match
      case None if persistent =>
        register(wh.replace(name, unqualified(result.schema), result.batches))
      case None =>
        register(MemTable(name, unqualified(result.schema), result.batches))
      case Some(target) =>
        writable(target, "append to") match
          case m: ManagedTable =>
            appendToWarehouse(name, m, result, columns, keys)
          case table =>
            appendToMemory(name, table, result, columns, keys)

  private def appendToMemory(
      name: String,
      table: TableSource,
      result: EngineResult,
      columns: Seq[String],
      keys: Seq[String]
  ): TableSource =
    val schema   = table.schema
    val defaults = defaultsOf(name, schema)
    if keys.isEmpty then
      val sourceOf = positionalSources(name, schema, result.schema, columns)
      val added   = result.batches.map(b => castBatch(b, result.schema, schema, sourceOf, defaults))
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
  def delete(name: String, input: Relation)(using Context): TableSource =
    val table         = writable(catalog.get(name), "delete")
    val (scan, conds) = deleteInput(input)
    table match
      case m: ManagedTable if conds.isEmpty =>
        register(wh.replace(name, unqualified(m.schema), Nil))
      case m: ManagedTable =>
        deleteFromWarehouse(name, m, scan, conds.reduce((a, b) => And(a, b, NoSpan)))
      case _ =>
        val kept =
          conds match
            case Nil =>
              Nil
            case _ =>
              val cond = conds.reduce((a, b) => And(a, b, NoSpan))
              engine.execute(Filter(scan, keepRows(cond), NoSpan)).batches
        register(MemTable(table.name, table.schema, kept))

  /**
    * `alter table` / `reshape`: apply the operations in order. On a warehouse table, operations
    * Iceberg makes without rewriting data (adding a nullable column without a default, dropping,
    * renaming, widening a type, dropping NOT NULL, defaults) commit metadata only; others (a new
    * column with a default or NOT NULL, a narrowing type change) rewrite the table in one snapshot
    */
  def alterTable(name: String, ops: Seq[AlterOp]): TableSource =
    val table = writable(catalog.get(name), "alter")
    ops.collectFirst { case AlterOp.RenameTable(to) =>
      to
    } match
      case Some(to) =>
        if ops.size > 1 then
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException("Renaming a table can't be combined with other changes")
        renameTable(name, table, to)
      case None =>
        val before = unqualified(table.schema)
        val cached = specOf(name)
        // A spec that doesn't match the columns is ignored, as defaultsOf and checkConstraints do
        val spec0 = cached.filter(_.columns.size == before.size)
        // The new schema, spec, and how to compute each new column from the old row
        var columns: IndexedSeq[AlteredColumn] = before
          .fields
          .zipWithIndex
          .map((f, i) =>
            AlteredColumn(
              f.name,
              f.ptype,
              Some(i),
              spec0.map(_.columns(i).default).orNull,
              spec0.exists(_.columns(i).notNull)
            )
          )
        var uniqueKeys = spec0.map(_.uniqueKeys).getOrElse(Nil)
        // Whether the warehouse table needs its rows rewritten
        var rewrite                 = false
        def find(c: String): Int    = columns.indexWhere(_.name.equalsIgnoreCase(c))
        def require(c: String): Int =
          val i = find(c)
          if i < 0 then
            throw StatusCode.COLUMN_NOT_FOUND.newException(s"Column ${c} is not found in ${name}")
          i
        val changes = List.newBuilder[IcebergTableChange]
        ops.foreach {
          case AlterOp.AddColumn(c, ifNotExists) =>
            if find(c.name) >= 0 then
              if !ifNotExists then
                throw StatusCode
                  .INVALID_ARGUMENT
                  .newException(s"Column ${c.name} already exists in ${name}")
            else
              columns :+= AlteredColumn(c.name, c.ptype, None, c.default, c.notNull)
              if c.default != null || c.notNull then
                rewrite = true
              else
                changes += IcebergTableChange.AddColumn(Warehouse.columnOf(Field(c.name, c.ptype)))
          case AlterOp.DropColumn(c, ifExists) =>
            val i = find(c)
            if i < 0 then
              if !ifExists then
                require(c)
            else
              if columns.size == 1 then
                throw StatusCode
                  .INVALID_ARGUMENT
                  .newException(s"Cannot drop ${c}, the only column of ${name}")
              if uniqueKeys.exists(_.exists(_.equalsIgnoreCase(c))) then
                throw StatusCode
                  .INVALID_ARGUMENT
                  .newException(s"Cannot drop ${c}: it is in a primary key or unique constraint")
              changes += IcebergTableChange.DropColumn(columns(i).name)
              columns = columns.patch(i, Nil, 1)
          case AlterOp.RenameColumn(from, to, ifExists) =>
            val i = find(from)
            if i < 0 then
              if !ifExists then
                require(from)
            else
              if find(to) >= 0 && !to.equalsIgnoreCase(from) then
                throw StatusCode
                  .INVALID_ARGUMENT
                  .newException(s"Column ${to} already exists in ${name}")
              val old = columns(i).name
              columns = columns.updated(i, columns(i).copy(name = to))
              uniqueKeys = uniqueKeys.map(
                _.map(k =>
                  if k.equalsIgnoreCase(old) then
                    to
                  else
                    k
                )
              )
              changes += IcebergTableChange.RenameColumn(old, to)
          case AlterOp.SetType(c, ptype) =>
            val i    = require(c)
            val from = columns(i).ptype
            if from != ptype then
              // The default takes the new type too
              columns = columns.updated(
                i,
                columns(i).copy(
                  ptype = ptype,
                  default = ValueOps.cast(columns(i).default, from, ptype)
                )
              )
              if canWiden(from, ptype) then
                changes +=
                  IcebergTableChange.WidenColumn(
                    columns(i).name,
                    Warehouse
                      .columnOf(Field(columns(i).name, ptype))
                      .tpe
                      .asInstanceOf[wvlet.uni.parquet.ParquetType.Primitive]
                  )
              else
                rewrite = true
          case AlterOp.SetNotNull(c) =>
            val i = require(c)
            columns = columns.updated(i, columns(i).copy(notNull = true))
          case AlterOp.DropNotNull(c) =>
            val i = require(c)
            columns = columns.updated(i, columns(i).copy(notNull = false))
          case AlterOp.SetDefault(c, v, vtype) =>
            val i = require(c)
            columns = columns.updated(
              i,
              columns(i).copy(default = ValueOps.cast(v, vtype, columns(i).ptype))
            )
          case AlterOp.DropDefault(c) =>
            val i = require(c)
            columns = columns.updated(i, columns(i).copy(default = null))
          case AlterOp.RenameTable(_) =>
        }
        val schema  = Schema(columns.map(c => Field(c.name, c.ptype)))
        val hasSpec = columns.exists(c => c.notNull || c.default != null) || uniqueKeys.nonEmpty
        val spec    =
          Option.when(hasSpec)(
            TableSpec(
              columns.map(c => ColumnSpec(c.name, c.ptype, c.default, c.notNull)),
              uniqueKeys
            )
          )
        // The rows in the new shape, computed only when they are rewritten
        def rows(): Seq[Batch] = batchesOf(table).map { b =>
          val cols = columns.map { c =>
            c.from match
              case Some(i) if before(i).ptype == c.ptype =>
                b.column(i)
              case Some(i) =>
                val out = VecBuilder(c.ptype, b.size)
                var r   = 0
                while r < b.size do
                  out.add(ValueOps.cast(b.column(i).get(r), before(i).ptype, c.ptype))
                  r += 1
                out.result()
              case None if c.default == null =>
                Vec.nullVec(c.ptype, b.size)
              case None =>
                val out = VecBuilder(c.ptype, b.size)
                var r   = 0
                while r < b.size do
                  out.add(c.default)
                  r += 1
                out.result()
          }
          Batch(cols.toArray, b.size)
        }
        // checkConstraints reads the new spec; a failed check or commit restores the old one
        setSpec(name, spec)
        try
          table match
            case m: ManagedTable =>
              if rewrite then
                val batches = rows()
                checkConstraints(name, schema, batches, notNullOf = batches)
                register(wh.replace(name, schema, batches, specProperties(spec)))
              else
                if columns.exists(c =>
                    c.notNull &&
                      !spec0.exists(
                        _.columns.exists(o => o.name.equalsIgnoreCase(c.name) && o.notNull)
                      )
                  )
                then
                  val batches = rows()
                  checkConstraints(name, schema, batches, notNullOf = batches)
                register(
                  wh.alter(
                    name,
                    changes.result() :+
                      IcebergTableChange.SetProperty(
                        SpecProperty,
                        spec.map(specToJson).getOrElse("")
                      )
                  )
                )
            case _ =>
              val batches = rows()
              checkConstraints(name, schema, batches, notNullOf = batches)
              register(MemTable(table.name, schema, batches))
        catch
          case e: Throwable =>
            setSpec(name, cached)
            throw e
        end try
    end match

  end alterTable

  private def renameTable(name: String, table: TableSource, to: String): TableSource =
    table match
      case _: ManagedTable =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Renaming the warehouse table ${name} is not supported yet")
      case _ =>
        if catalog.find(to).isDefined then
          throw StatusCode.TABLE_ALREADY_EXISTS.newException(s"Table ${to} already exists")
        val spec = specOf(name)
        catalog.drop(name)
        specs.synchronized(specs.remove(name.toLowerCase))
        setSpec(to, spec)
        register(MemTable(to, table.schema, batchesOf(table)))

  // The rows whose condition is TRUE
  private def matching(cond: Expression): Expression = FunctionApply(
    UnquotedIdentifier("coalesce", NoSpan),
    List(
      FunctionArg(None, cond, span = NoSpan),
      FunctionArg(None, FalseLiteral(NoSpan), span = NoSpan)
    ),
    None,
    span = NoSpan
  )

  // Keep the rows whose condition is not TRUE: false and NULL rows survive, as in SQL
  private def keepRows(cond: Expression): Expression = Not(matching(cond), NoSpan)

  /**
    * A copy-on-write delete: only the data files holding deleted rows are rewritten (with their
    * other rows), in one snapshot that removes them
    */
  private def deleteFromWarehouse(
      name: String,
      table: ManagedTable,
      scan: Relation,
      cond: Expression
  )(using Context): TableSource =
    val schema         = unqualified(table.schema)
    val (parts, close) = wh.parts(name, schema)
    try
      val touched = parts.flatMap { part =>
        val onPart = engine.withCatalog(catalog.overlay(part))
        // A probe for one matching row: the condition goes down to the file scan, so statistics
        // skip files and row groups without a match, and only touched files are read in full
        val probe = Limit(Filter(scan, cond, NoSpan), LongLiteral(1, "1", NoSpan), NoSpan)
        if onPart.execute(probe).numRows == 0 then
          None
        else
          Some(part.file.path -> onPart.execute(Filter(scan, keepRows(cond), NoSpan)).batches)
      }
      if touched.isEmpty then
        table
      else
        register(
          wh.replaceFiles(
            name,
            schema,
            touched.map(_._1),
            touched
              .flatMap(_._2)
              .map(b => castBatch(b, schema, schema, identityOf(schema), nulls(schema)))
          )
        )
    finally
      close()

  end deleteFromWarehouse

  private def identityOf(schema: Schema): Array[Int] = Array.tabulate(schema.size)(identity)

  private def nulls(schema: Schema): IndexedSeq[Any] = IndexedSeq.fill(schema.size)(null)

  /**
    * `append to` a warehouse table: a plain append adds a snapshot; a keyed append rewrites only
    * the data files with matching keys, and adds the other rows, in one snapshot
    */
  private def appendToWarehouse(
      name: String,
      table: ManagedTable,
      result: EngineResult,
      columns: Seq[String],
      keys: Seq[String]
  ): TableSource =
    val schema   = unqualified(table.schema)
    val defaults = defaultsOf(name, schema)
    if keys.isEmpty then
      val sourceOf = positionalSources(name, schema, result.schema, columns)
      val added = result.batches.map(b => castBatch(b, result.schema, schema, sourceOf, defaults))
      // Unique keys need the existing rows; NOT NULL only the new ones
      val existing =
        if specOf(name).exists(_.uniqueKeys.nonEmpty) then
          batchesOf(table)
        else
          Nil
      checkConstraints(name, schema, existing ++ added, notNullOf = added)
      register(wh.append(name, schema, added))
    else
      val m              = mergeInput(name, schema, result, columns, keys, defaults)
      val (parts, close) = wh.parts(name, schema)
      try
        val keyNames = m.keyIdx.map(schema(_).name)
        val incoming = m.incoming.flatMap(r => m.keyOf(r).map(_ -> r))
        val keySet   = incoming.map(_._1).toSet
        val matched  = mutable.HashSet.empty[Seq[Any]]
        // Statistics skip the files whose key columns can't hold an incoming key
        val keyFilter = keyNames
          .indices
          .flatMap { k =>
            val f = schema(m.keyIdx(k))
            ScanFilters.valuesFilter(f.name, f.ptype, keySet.iterator.map(_(k)).toSeq)
          }
          .reduceOption(_ && _)
        // Files with a matching key are read in full and rewritten with the updates
        val touched = parts.flatMap { part =>
          val partKeys =
            part
              .scan(ScanSpec(columns = Some(keyNames), filter = keyFilter))
              .flatMap(b =>
                Iterator.range(0, b.size).map(i => keyNames.indices.map(k => b.column(k).get(i)))
              )
              .filter(keySet.contains)
              .toSet
          if partKeys.isEmpty then
            None
          else
            matched ++= partKeys
            val rows = mutable.ArrayBuffer.empty[Array[Any]]
            batchesOf(part).foreach(b => Iterator.range(0, b.size).foreach(i => rows += b.row(i)))
            Some(part.file.path -> applyUpdates(rows, m))
        }
        val inserted = m.incoming.filter(r => m.keyOf(r).forall(k => !matched.contains(k)))
        val written  =
          touched.map(t => toBatch(schema, t._2.toIndexedSeq)) ++
            Option.when(inserted.nonEmpty)(toBatch(schema, inserted))
        val untouched =
          if specOf(name).exists(_.uniqueKeys.nonEmpty) then
            val replaced = touched.map(_._1).toSet
            parts.filterNot(p => replaced.contains(p.file.path)).flatMap(batchesOf)
          else
            Nil
        checkConstraints(name, schema, untouched ++ written, notNullOf = written)
        if touched.isEmpty && inserted.isEmpty then
          table
        else if touched.isEmpty then
          register(wh.append(name, schema, written))
        else
          register(wh.replaceFiles(name, schema, touched.map(_._1), written))
      finally
        close()
      end try
    end if

  end appendToWarehouse

  private def register[T <: TableSource](t: T): T =
    catalog.register(t)
    t

  private def unqualified(schema: Schema): Schema = schema.mapFields(_.noQualifier)

  private def writable(source: TableSource, op: String): TableSource =
    source match
      case m: MemTable =>
        m
      case m: ManagedTable =>
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

  // The rows of a keyed append, cast to the table schema, and how they match existing rows
  private case class MergeInput(
      sourceOf: Array[Int],
      keyIdx: Seq[Int],
      incoming: IndexedSeq[Array[Any]]
  ):
    // NULL never equals NULL in a MERGE condition, so rows with a NULL key never match
    def keyOf(r: Array[Any]): Option[Seq[Any]] =
      val k = keyIdx.map(r(_))
      Option.when(!k.contains(null))(k)

  private def mergeInput(
      name: String,
      schema: Schema,
      result: EngineResult,
      columns: Seq[String],
      keys: Seq[String],
      defaults: IndexedSeq[Any]
  ): MergeInput =
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
    val fromTypes = result.schema.types
    val incoming  =
      result
        .rows
        .map { r =>
          Array.tabulate[Any](schema.size) { i =>
            val src = sourceOf(i)
            if src < 0 then
              defaults(i)
            else
              ValueOps.cast(r(src), fromTypes(src), schema(i).ptype)
          }
        }
        .toIndexedSeq
    MergeInput(sourceOf, keyIdx, incoming)

  end mergeInput

  /** Update the mapped columns of every row whose key an incoming row has */
  private def applyUpdates(
      rows: mutable.ArrayBuffer[Array[Any]],
      m: MergeInput
  ): mutable.ArrayBuffer[Array[Any]] =
    val positions = mutable.HashMap.empty[Seq[Any], mutable.ArrayBuffer[Int]]
    rows
      .indices
      .foreach(i =>
        m.keyOf(rows(i)).foreach(k => positions.getOrElseUpdate(k, mutable.ArrayBuffer.empty) += i)
      )
    m.incoming
      .foreach { incoming =>
        m.keyOf(incoming)
          .flatMap(positions.get)
          .foreach { matched =>
            // Unmapped columns keep their values
            matched.foreach { p =>
              val updated = rows(p).clone()
              m.sourceOf
                .indices
                .foreach(i =>
                  if m.sourceOf(i) >= 0 then
                    updated(i) = incoming(i)
                )
              rows(p) = updated
            }
          }
      }
    rows

  end applyUpdates

  private def upsert(
      name: String,
      schema: Schema,
      table: TableSource,
      result: EngineResult,
      columns: Seq[String],
      keys: Seq[String],
      defaults: IndexedSeq[Any]
  ): IndexedSeq[Array[Any]] =
    val m    = mergeInput(name, schema, result, columns, keys, defaults)
    val rows = mutable.ArrayBuffer.empty[Array[Any]]
    batchesOf(table).foreach(b => Iterator.range(0, b.size).foreach(i => rows += b.row(i)))
    val existingKeys = rows.flatMap(m.keyOf).toSet
    applyUpdates(rows, m)
    // Rows inserted by this append are not matched again
    rows ++= m.incoming.filter(r => m.keyOf(r).forall(k => !existingKeys.contains(k)))
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
  /** The table property that keeps a warehouse table's declared defaults and constraints */
  val SpecProperty = "wvlet.table-spec"

  /** An `alter table` operation */
  enum AlterOp:
    case AddColumn(column: ColumnSpec, ifNotExists: Boolean = false)
    case DropColumn(name: String, ifExists: Boolean = false)
    case RenameColumn(from: String, to: String, ifExists: Boolean = false)
    case SetType(name: String, ptype: PType)
    case SetNotNull(name: String)
    case DropNotNull(name: String)

    /** A literal default of type `valueType`, cast to the column's type when the alter applies */
    case SetDefault(name: String, value: Any, valueType: PType)
    case DropDefault(name: String)
    case RenameTable(to: String)

  // A column of an altered table: `from` is its index in the old schema (None for a new column)
  private case class AlteredColumn(
      name: String,
      ptype: PType,
      from: Option[Int],
      default: Any,
      notNull: Boolean
  )

  // The type changes Iceberg makes without rewriting data
  private def canWiden(from: PType, to: PType): Boolean =
    (from, to) match
      case (PType.Int32, PType.Int64) =>
        true
      case (a: PType.Decimal, b: PType.Decimal) =>
        a.scale == b.scale && b.precision > a.precision
      case _ =>
        false

  /** A table spec as the JSON of [[SpecProperty]]; defaults are kept as strings */
  def specToJson(spec: TableSpec): String =
    JSONObject(
      Seq(
        "columns" ->
          JSONArray(
            spec
              .columns
              .map { c =>
                JSONObject(
                  Seq(
                    "name"    -> JSONString(c.name),
                    "notNull" -> JSONBoolean(c.notNull),
                    "default" -> (
                      if c.default == null then
                        JSONNull()
                      else
                        JSONString(ValueOps.cast(c.default, c.ptype, PType.Str).toString)
                    )
                  )
                )
              }
              .toIndexedSeq
          ),
        "uniqueKeys" ->
          JSONArray(
            spec.uniqueKeys.map(k => JSONArray(k.map(JSONString(_)).toIndexedSeq)).toIndexedSeq
          )
      )
    ).toJSON

  /** The spec from [[specToJson]], typed by the table's columns */
  def specFromJson(json: String, schema: Schema): TableSpec =
    val o                                                = JSON.parse(json).asInstanceOf[JSONObject]
    def arr(v: Option[JSONValue]): IndexedSeq[JSONValue] =
      v match
        case Some(JSONArray(vs)) =>
          vs
        case _ =>
          IndexedSeq.empty
    val byName =
      arr(o.get("columns"))
        .collect { case c: JSONObject =>
          c.get("name")
            .collect { case JSONString(n) =>
              n.toLowerCase
            }
            .getOrElse("") -> c
        }
        .toMap
    val columns = schema
      .fields
      .map { f =>
        val c       = byName.get(f.name.toLowerCase)
        val default = c
          .flatMap(_.get("default"))
          .collect { case JSONString(v) =>
            ValueOps.cast(v, PType.Str, f.ptype)
          }
        ColumnSpec(
          f.name,
          f.ptype,
          default.orNull,
          c.flatMap(_.get("notNull")).contains(JSONBoolean(true))
        )
      }
    val keys = arr(o.get("uniqueKeys")).map(k =>
      arr(Some(k)).collect { case JSONString(n) =>
        n
      }
    )
    TableSpec(columns, keys)

  end specFromJson

  /** A declared column of a table made by `create table` */
  case class ColumnSpec(name: String, ptype: PType, default: Any = null, notNull: Boolean = false)

  /** The declared shape of a table: its columns and unique keys (primary key included) */
  case class TableSpec(columns: Seq[ColumnSpec], uniqueKeys: Seq[Seq[String]] = Nil):
    def schema: Schema = Schema(columns.map(c => Field(c.name, c.ptype)).toIndexedSeq)

end TableWriter
