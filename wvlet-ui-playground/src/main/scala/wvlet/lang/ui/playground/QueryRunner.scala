package wvlet.lang.ui.playground

import wvlet.airframe.rx.Rx
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QueryResult
import wvlet.lang.ui.component.duckdb.Arrow.*
import wvlet.lang.ui.component.duckdb.DuckDB
import wvlet.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

class QueryRunner extends AutoCloseable with LogSupport:
  private val connectors = new ConcurrentHashMap[String, DuckDB]().asScala

  private def getConnector(name: String): DuckDB = connectors.getOrElseUpdate(name, newDuckDB())

  private def newDuckDB(): DuckDB =
    try
      val duckdb = DuckDB()
      // Preload an example TPC-H dataset
      duckdb.query("""load tpch;
          |call dbgen(sf=0.01);
          |""".stripMargin)
      duckdb
    catch
      case e: Throwable =>
        warn(e)
        throw e

  def runQuery(connector: String, sql: String, rowLimit: Int = 40): Rx[QueryResult] = getConnector(
    connector
  ).query(sql)
    .map { tbl =>
      val rows                                    = tbl.asScalaArray
      val (truncatedRows, totalRows: Option[Int]) =
        if rows.size > rowLimit then
          (rows.take(rowLimit), Some(rows.size))
        else
          (rows, None)
      QueryResult(tbl.schema.columns, truncatedRows, totalRows)
    }

  end runQuery

  override def close(): Unit = connectors.values.foreach(c => c.close())

end QueryRunner
