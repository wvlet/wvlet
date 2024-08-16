package com.treasuredata.flow.lang.runner.connector.duckdb

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.catalog.SQLFunction
import com.treasuredata.flow.lang.catalog.SQLFunction.FunctionType
import com.treasuredata.flow.lang.compiler.DBType.DuckDB
import com.treasuredata.flow.lang.compiler.Name
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.NamedType
import com.treasuredata.flow.lang.runner.ThreadUtil
import com.treasuredata.flow.lang.runner.connector.DBConnector
import org.duckdb.DuckDBConnection
import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.metrics.ElapsedTime
import wvlet.log.LogSupport

import java.sql.{Connection, DriverManager, SQLException}
import scala.util.{Try, Using}

class DuckDBConnector(prepareTPCH: Boolean = false)
    extends DBConnector(DuckDB)
    with AutoCloseable
    with LogSupport:

  // We need to reuse he same connection for preserving in-memory tables
  private var conn: DuckDBConnection = null

  // Initialize DuckDB in the background thread as it may take several seconds
  private val initThread = ThreadUtil.runBackgroundTask { () =>
    val nano = System.nanoTime()
    logger.trace("Initializing DuckDB connection")
    conn = newConnection
    logger.trace(s"Finished initializing DuckDB. ${ElapsedTime.nanosSince(nano)}")
  }

  override def newConnection: DuckDBConnection =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        if prepareTPCH then
          Using.resource(conn.createStatement()): stmt =>
            stmt.execute("install tpch")
            stmt.execute("load tpch")
            stmt.execute("call dbgen(sf = 0.01)")
        conn
      case _ =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")

  override def close(): Unit = Option(conn).foreach { c =>
    c.close()
    conn = null
  }

  private def getConnection: DuckDBConnection =
    if conn == null && initThread.isAlive then
      // Wait until the connection is available
      initThread.join()

    if conn == null then
      throw StatusCode.NON_RETRYABLE_INTERNAL_ERROR.newException("Failed to initialize DuckDB")
    conn

  override def withConnection[U](body: Connection => U): U =
    try
      body(getConnection)
    catch
      case e: SQLException if e.getMessage.contains("403") =>
        throw StatusCode.PERMISSION_DENIED.newException(e.getMessage, e)

  override def listFunctions(catalog: String): List[SQLFunction] =
    val functionList = List.newBuilder[SQLFunction]

    val jsonArrayCodec = MessageCodec.of[Seq[String]]
    runQuery("""select function_name, function_type,
        |parameters::json as parameters, parameter_types::json parameter_types,
        |return_type,
        |description
        |from duckdb_functions()""".stripMargin) { rs =>
      while rs.next() do
        val argNames = jsonArrayCodec.fromJson(rs.getString("parameters"))
        val argTypes = jsonArrayCodec.fromJson(rs.getString("parameter_types"))
        val args: Seq[DataType] = argNames
          .zipAll(argTypes, "", "")
          .map {
            case ("", argType) =>
              DataType.parse(argType)
            case (argName, argType) =>
              NamedType(Name.termName(argName.toLowerCase), DataType.parse(argType))
          }
        val props = Map.newBuilder[String, Any]
        rs.getString("description") match
          case null =>
          case desc =>
            props += "description" -> desc

        functionList +=
          SQLFunction(
            name = rs.getString("function_name"),
            functionType = Try(FunctionType.valueOf(rs.getString("function_type").toUpperCase))
              .toOption
              .getOrElse(FunctionType.UNKNOWN),
            args = args,
            returnType = DataType.parse(rs.getString("return_type")),
            properties = props.result()
          )
    }

    functionList.result()

  end listFunctions

end DuckDBConnector
