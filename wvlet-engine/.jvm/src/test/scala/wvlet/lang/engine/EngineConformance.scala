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

import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.vector.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import java.math.BigDecimal as JBigDecimal
import java.sql.Connection
import java.sql.DriverManager

/** One query compared between DuckDB and the engine */
case class ConformanceResult(name: String, matched: Boolean, message: String)

/**
  * Runs the queries of .wv files on both DuckDB (via generated SQL) and the engine, and compares
  * the results as multisets of rows (row order is compared only through the values: ties in ORDER
  * BY keys may legitimately differ). `optimizer` selects the engine's planner optimizations
  */
class EngineConformance(
    specPath: String,
    setupSQL: Seq[String],
    setupEngine: WvletEngine => Unit,
    optimizer: plan.OptimizerOptions = plan.OptimizerOptions()
) extends LogSupport
    with AutoCloseable:
  Class.forName("org.duckdb.DuckDBDriver")
  private val conn: Connection = DriverManager.getConnection("jdbc:duckdb:")
  // The engine's session time zone is UTC; DuckDB's defaults to the machine's
  (Seq("SET TimeZone = 'UTC'") ++ setupSQL).foreach { sql =>
    val st = conn.createStatement()
    try st.execute(sql)
    finally st.close()
  }

  val engine = WvletEngine(baseDir = specPath, optimizer = optimizer)
  setupEngine(engine)

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List(specPath), workEnv = WorkEnv(specPath))
  )

  def units: Seq[CompilationUnit] = compiler.localCompilationUnits.sortBy(_.sourceFile.fileName)

  override def close(): Unit = conn.close()

  def run(unit: CompilationUnit): Seq[ConformanceResult] = queries(unit)((n, q) => compare(n, q))

  /**
    * Fail unless the query returns the same rows on `target` as on DuckDB, which must accept its
    * SQL (see [[compare]])
    */
  def assertMatches(
      name: String,
      q: Relation,
      target: WvletEngine = engine,
      ordered: Boolean = false
  )(using Context): Unit =
    val r = compare(name, q, target, ordered)
    debug(r.message)
    if !r.matched || r.message.startsWith("skipped") then
      throw AssertionError(s"${r.message}\n${name}")

  /** The engine's physical plan of each query in the unit, or the planning error */
  def explain(unit: CompilationUnit): Seq[String] =
    queries(unit) { (_, q) =>
      try
        engine.explain(q)
      catch
        case e: Exception =>
          s"error: ${e.getMessage}"
    }

  /** Apply f to each query of the unit, named `<file name>#<query number>` */
  private def queries[A](unit: CompilationUnit)(f: Context ?=> (String, Relation) => A): Seq[A] =
    val r                            = compiler.compileSingleUnit(unit)
    given ctx: Context               = r.context
    val results                      = Seq.newBuilder[A]
    var idx                          = 0
    def walk(e: ExecutionPlan): Unit =
      e match
        case ExecuteQuery(q: Relation) =>
          idx += 1
          results += f(s"${unit.sourceFile.fileName}#${idx}", q)
        case ExecuteTasks(ts) =>
          ts.foreach(walk)
        case ExecuteSave(_, q) =>
          walk(q)
        case _ =>
    walk(unit.executionPlan)
    results.result()

  /** How much of a nondeterministic query's result can be compared */
  private enum Determinism:
    case Full,
      RowCountOnly,
      SchemaOnly

  private def determinism(q: Relation): Determinism =
    var d                            = Determinism.Full
    def weaken(x: Determinism): Unit =
      if x.ordinal > d.ordinal then
        d = x
    q.traverse { case s: Sample =>
      s.size match
        case SamplingSize.Rows(_) =>
          weaken(Determinism.RowCountOnly)
        case _ =>
          weaken(Determinism.SchemaOnly)
    }
    q.traverseExpressions { case _: wvlet.lang.model.expr.NativeExpression =>
      // e.g., ulid_string returns a new value for every evaluation
      weaken(Determinism.RowCountOnly)
    }
    d

  private lazy val queryCompiler = Compiler(
    CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(specPath))
  )

  /**
    * Compile one Wvlet query string and compare its rows on the engine with DuckDB's, as
    * [[compare]] does. A query that does not compile is reported as unmatched, with a message
    * starting with "compile error"
    */
  def compareQuery(query: String, ordered: Boolean = false): ConformanceResult =
    val unit = CompilationUnit.fromWvletString(query)
    val r    = queryCompiler.compileSingleUnit(unit)
    if r.hasFailures then
      ConformanceResult(
        query,
        matched = false,
        s"compile error: ${r.failureReport.map(_._2).mkString.take(200)}"
      )
    else
      given Context = r.context
      EngineTPCHBench.queryOf(unit) match
        case None =>
          ConformanceResult(query, matched = false, "compile error: no query")
        case Some(rel) =>
          compare(query, GenSQL.expand(rel, summon[Context]), ordered = ordered)

  /**
    * Compare one query's rows on `target` (this conformance's engine by default) with DuckDB's, as
    * multisets, or in order with `ordered` (for queries whose ORDER BY keys are a total order). A
    * query DuckDB rejects is reported as matched, with a message starting with "skipped"
    */
  def compare(name: String, q: Relation, target: WvletEngine = engine, ordered: Boolean = false)(
      using ctx: Context
  ): ConformanceResult =
    val mode     = determinism(q)
    val expected =
      try
        Right(runDuckDB(GenSQL.generateSQLFromRelation(q, addHeader = false).sql))
      catch
        case e: Throwable =>
          Left(e)
    expected match
      case Left(e) =>
        // Queries DuckDB itself rejects are not conformance targets
        ConformanceResult(
          name,
          matched = true,
          s"skipped (DuckDB error: ${e.getMessage.take(120)})"
        )
      case Right((cols, expectedRows)) =>
        val actual =
          try
            val res = target.execute(q)
            Right(
              (
                res.schema.names,
                res
                  .rows
                  .map(r => r.indices.map(i => normalizeEngine(r(i), res.schema(i).ptype)))
                  .toSeq
              )
            )
          catch
            case e: Throwable =>
              Left(e)
        actual match
          case Left(e) =>
            ConformanceResult(name, matched = false, s"engine error: ${e}")
          case Right((actualCols, actualRows)) =>
            if actualCols.size != cols.size then
              ConformanceResult(
                name,
                false,
                s"column count: expected ${cols.mkString(",")} but got ${actualCols.mkString(",")}"
              )
            else if mode == Determinism.SchemaOnly then
              ConformanceResult(
                name,
                true,
                s"${actualRows.size} rows (nondeterministic: schema only)"
              )
            else if actualRows.size != expectedRows.size then
              ConformanceResult(
                name,
                false,
                s"row count: expected ${expectedRows.size} but got ${actualRows
                    .size}\nexpected: ${expectedRows.take(3)}\nactual:   ${actualRows.take(3)}"
              )
            else if mode == Determinism.RowCountOnly then
              ConformanceResult(
                name,
                true,
                s"${actualRows.size} rows (nondeterministic: row count only)"
              )
            else
              val e =
                if ordered then
                  expectedRows
                else
                  expectedRows.sortBy(rowKey)
              val a =
                if ordered then
                  actualRows
                else
                  actualRows.sortBy(rowKey)
              val diff = e.zip(a).find((x, y) => !rowEquals(x, y))
              diff match
                case Some((x, y)) =>
                  ConformanceResult(name, false, s"row mismatch:\nexpected: ${x}\nactual:   ${y}")
                case None =>
                  val nameNote =
                    if actualCols.map(_.toLowerCase) != cols.map(_.toLowerCase) then
                      s" (column names differ: ${cols.mkString(",")} vs ${actualCols.mkString(
                          ","
                        )})"
                    else
                      ""
                  ConformanceResult(name, true, s"${a.size} rows${nameNote}")
        end match
    end match

  end compare

  private def runDuckDB(sql: String): (Seq[String], Seq[Seq[Any]]) =
    debug(s"DuckDB SQL:\n${sql}")
    val st = conn.createStatement()
    try
      val rs   = st.executeQuery(sql)
      val md   = rs.getMetaData
      val n    = md.getColumnCount
      val cols = (1 to n).map(md.getColumnLabel)
      val rows = Seq.newBuilder[Seq[Any]]
      while rs.next() do
        rows +=
          (1 to n).map { i =>
            // java.sql.Timestamp renders in the JVM's zone, which shifts wall-clock times inside
            // its DST gaps: read plain timestamps as LocalDateTime
            if md.getColumnType(i) == java.sql.Types.TIMESTAMP then
              normalizeJdbc(rs.getObject(i, classOf[java.time.LocalDateTime]))
            else
              normalizeJdbc(rs.getObject(i))
          }
      (cols, rows.result())
    finally
      st.close()

  private def normalizeJdbc(v: Any): Any =
    v match
      case null =>
        null
      case i: java.lang.Integer =>
        i.longValue()
      case s: java.lang.Short =>
        s.longValue()
      case b: java.lang.Byte =>
        b.longValue()
      case l: java.lang.Long =>
        l.longValue()
      case b: java.math.BigInteger =>
        JBigDecimal(b)
      case f: java.lang.Float =>
        f.doubleValue()
      case d: java.lang.Double =>
        d.doubleValue()
      case b: JBigDecimal =>
        b
      case d: java.sql.Date =>
        d.toString
      case d: java.time.LocalDate =>
        d.toString
      case t: java.sql.Timestamp =>
        val s = t.toString.stripSuffix(".0")
        s
      case t: java.time.LocalDateTime =>
        ValueOps.formatTimestamp(
          Math.addExact(
            Math.multiplyExact(t.toEpochSecond(java.time.ZoneOffset.UTC), 1000000L),
            t.getNano / 1000L
          )
        )
      case t: java.time.OffsetDateTime =>
        // JDBC shows TIMESTAMPTZ in the JVM's zone: render the instant in UTC, as the engine does
        val i = t.toInstant
        ValueOps.formatTimestampTz(
          Math.addExact(Math.multiplyExact(i.getEpochSecond, 1000000L), i.getNano / 1000L)
        )
      case b: java.lang.Boolean =>
        b.booleanValue()
      case a: java.sql.Array =>
        nested(a.getArray.asInstanceOf[Array[AnyRef]].toSeq.map(normalizeJdbc))
      case s: org.duckdb.DuckDBStruct =>
        import scala.jdk.CollectionConverters.*
        nested(s.getMap.asScala.toSeq.map((k, v) => (k, normalizeJdbc(v))).toMap)
      case m: java.util.Map[?, ?] =>
        import scala.jdk.CollectionConverters.*
        nested(m.asScala.toSeq.map((k, v) => (normalizeJdbc(k), normalizeJdbc(v))).toMap)
      case other =>
        other.toString

  /** A canonical string for lists, maps, and structs (as maps of field names) */
  private def nested(v: Any): String =
    v match
      case null =>
        "NULL"
      case s: Seq[?] =>
        s.map(nested).mkString("[", ", ", "]")
      case m: scala.collection.Map[?, ?] =>
        m.toSeq
          .map((k, x) => (nested(k), nested(x)))
          .sortBy(_._1)
          .map((k, x) => s"${k}=${x}")
          .mkString("{", ", ", "}")
      case i: Int =>
        i.toLong.toString
      case d: Double =>
        ValueOps.formatDouble(d)
      case b: JBigDecimal =>
        b.stripTrailingZeros().toPlainString
      case other =>
        other.toString

  private def normalizeEngine(v: Any, t: PType): Any =
    if v == null then
      null
    else
      t match
        case PType.Int32 =>
          v.asInstanceOf[Int].toLong
        case PType.Date =>
          ValueOps.formatDate(v.asInstanceOf[Int])
        case PType.Timestamp =>
          ValueOps.formatTimestamp(v.asInstanceOf[Long])
        case PType.TimestampTz =>
          ValueOps.formatTimestampTz(v.asInstanceOf[Long])
        case PType.AnyT =>
          v match
            // Dynamically typed values (e.g., unnested list elements)
            case i: Int =>
              i.toLong
            case l: Long =>
              l
            case d: Double =>
              d
            case b: JBigDecimal =>
              b
            case b: Boolean =>
              b
            case s: Seq[?] =>
              nested(s)
            case m: scala.collection.Map[?, ?] =>
              nested(m)
            case other =>
              other.toString
        case _ =>
          v

  private def numeric(v: Any): Option[Double] =
    v match
      case l: Long =>
        Some(l.toDouble)
      case d: Double =>
        Some(d)
      case b: JBigDecimal =>
        Some(b.doubleValue())
      case _ =>
        None

  private def rowKey(r: Seq[Any]): String = r
    .map { v =>
      numeric(v) match
        case Some(d) =>
          f"${d}%.6e"
        case None =>
          String.valueOf(v)
    }
    .mkString("\u0001")

  private def valueEquals(x: Any, y: Any): Boolean =
    (x, y) match
      case (null, null) =>
        true
      case (null, _) | (_, null) =>
        false
      case _ =>
        (numeric(x), numeric(y)) match
          case (Some(a), Some(b)) =>
            a == b ||
            (a.isNaN && b.isNaN) || math.abs(a - b) <= 1e-9 * math.max(math.abs(a), math.abs(b)) ||
            math.abs(a - b) < 1e-12
          case _ =>
            x.toString == y.toString

  private def rowEquals(x: Seq[Any], y: Seq[Any]): Boolean =
    x.size == y.size && x.zip(y).forall(valueEquals)

end EngineConformance

object EngineConformance:
  /** A conformance over the TPC-H sample tables (spec/tpch/data), on DuckDB and the engine */
  def tpch(optimizer: plan.OptimizerOptions): EngineConformance =
    val tables = EngineTPCHBench.tables
    EngineConformance(
      "spec/tpch",
      tables.map(t => s"create view ${t} as select * from 'spec/tpch/data/${t}.parquet'"),
      e => tables.foreach(t => e.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet")),
      optimizer
    )
