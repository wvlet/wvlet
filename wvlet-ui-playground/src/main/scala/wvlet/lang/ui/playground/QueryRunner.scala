package wvlet.lang.ui.playground

import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.json.JSON
import wvlet.airframe.json.JSON.{JSONArray, JSONObject}
import wvlet.airframe.rx.Rx
import wvlet.lang.api.v1.query.{Column, QueryResult}
import wvlet.lang.compiler.analyzer.JSONAnalyzer
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
          |call dbgen(sf=-0.01);
          |""".stripMargin)
      duckdb
    catch
      case e: Throwable =>
        warn(e)
        throw e

  def runQuery(connector: String, sql: String): Rx[QueryResult] = getConnector(connector)
    .query(sql)
    .map { jsonString =>
      val j            = JSON.parse(jsonString)
      val relationType = JSONAnalyzer.guessSchema(j)
      val columns = relationType
        .fields
        .map { f =>
          Column(f.name.name, f.dataType.toString)
        }
      val result: Seq[Seq[Any]] =
        j match
          case a: JSONArray =>
            a.v
              .map { row =>
                row match
                  case obj: JSONObject =>
                    obj.v.map(_._2)
                  case _ =>
                    Seq.empty
              }
          case _ =>
            Seq.empty
      QueryResult(columns, result)
    }

  override def close(): Unit = connectors.values.foreach(c => c.close())

end QueryRunner
