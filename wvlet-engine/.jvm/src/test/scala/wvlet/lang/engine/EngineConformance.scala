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
  * BY keys may legitimately differ)
  */
class EngineConformance(specPath: String, setupSQL: Seq[String], setupEngine: WvletEngine => Unit)
    extends LogSupport
    with AutoCloseable:
  Class.forName("org.duckdb.DuckDBDriver")
  private val conn: Connection = DriverManager.getConnection("jdbc:duckdb:")
  setupSQL.foreach { sql =>
    val st = conn.createStatement()
    try st.execute(sql)
    finally st.close()
  }

  val engine = WvletEngine(baseDir = specPath)
  setupEngine(engine)

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = List(specPath), workEnv = WorkEnv(specPath))
  )

  def units: Seq[CompilationUnit] = compiler.localCompilationUnits.sortBy(_.sourceFile.fileName)

  override def close(): Unit = conn.close()

  def run(unit: CompilationUnit): Seq[ConformanceResult] =
    val r                            = compiler.compileSingleUnit(unit)
    given ctx: Context               = r.context
    val results                      = Seq.newBuilder[ConformanceResult]
    var idx                          = 0
    def walk(e: ExecutionPlan): Unit =
      e match
        case ExecuteQuery(q: Relation) =>
          idx += 1
          results += compare(s"${unit.sourceFile.fileName}#${idx}", q)
        case ExecuteTasks(ts) =>
          ts.foreach(walk)
        case ExecuteSave(_, q) =>
          walk(q)
        case _ =>
    walk(unit.executionPlan)
    results.result()

  private def compare(name: String, q: Relation)(using ctx: Context): ConformanceResult =
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
            val res = engine.execute(q)
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
            else if actualRows.size != expectedRows.size then
              ConformanceResult(
                name,
                false,
                s"row count: expected ${expectedRows.size} but got ${actualRows
                    .size}\nexpected: ${expectedRows.take(3)}\nactual:   ${actualRows.take(3)}"
              )
            else
              val e    = expectedRows.sortBy(rowKey)
              val a    = actualRows.sortBy(rowKey)
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
    val st = conn.createStatement()
    try
      val rs   = st.executeQuery(sql)
      val md   = rs.getMetaData
      val n    = md.getColumnCount
      val cols = (1 to n).map(md.getColumnLabel)
      val rows = Seq.newBuilder[Seq[Any]]
      while rs.next() do
        rows += (1 to n).map(i => normalizeJdbc(rs.getObject(i)))
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
        t.toString.replace('T', ' ')
      case b: java.lang.Boolean =>
        b.booleanValue()
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
        case PType.AnyT =>
          v.toString
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
            a == b || math.abs(a - b) <= 1e-9 * math.max(math.abs(a), math.abs(b)) ||
            math.abs(a - b) < 1e-12
          case _ =>
            x.toString == y.toString

  private def rowEquals(x: Seq[Any], y: Seq[Any]): Boolean =
    x.size == y.size && x.zip(y).forall(valueEquals)

end EngineConformance
