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
package wvlet.lang.runner.connector.duckdb

import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.SQLFunction
import wvlet.lang.catalog.SQLFunction.FunctionType
import wvlet.lang.compiler.DBType.DuckDB
import wvlet.lang.compiler.{Name, WorkEnv}
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.runner.ThreadUtil
import wvlet.lang.runner.connector.{DBConnection, DBConnector}
import org.duckdb.DuckDBConnection
import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.metrics.ElapsedTime
import wvlet.log.LogSupport

import java.sql.{Connection, DriverManager, SQLException}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Try, Using}

class DuckDBConnector(workEnv: WorkEnv, prepareTPCH: Boolean = false, prepareTPCDS: Boolean = false)
    extends DBConnector(DuckDB, workEnv)
    with AutoCloseable
    with LogSupport:

  // We need to reuse the same connection for preserving in-memory tables
  private var conn: DBConnection = null

  private val initialized = AtomicBoolean(false)
  private val closed      = AtomicBoolean(false)

  // Initialize DuckDB in the background thread as it may take several seconds
  private val initThread = ThreadUtil.runBackgroundTask { () =>
    val nano = System.nanoTime()
    logger.trace("Initializing DuckDB connection")
    conn = newConnection
    if prepareTPCH then
      loadTPCH()
    if prepareTPCDS then
      loadTPCDS()

    initialized.set(true)
    logger.trace(s"Finished initializing DuckDB. ${ElapsedTime.nanosSince(nano)}")
  }

  def loadTPCH(): Unit =
    Using.resource(conn.createStatement()): stmt =>
      stmt.execute("install tpch")
      stmt.execute("load tpch")
      stmt.execute("call dbgen(sf = 0.01)")

  def loadTPCDS(): Unit =
    Using.resource(conn.createStatement()): stmt =>
      stmt.execute("install tpcds")
      stmt.execute("load tpcds")
      stmt.execute("call dsdgen(sf = 0.01)")

  override private[connector] def newConnection: DBConnection =
    // For in-memory DuckDB, the connection will be created only once
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:") match
      case conn: DuckDBConnection =>
        DBConnection(conn)
      case _ =>
        throw StatusCode.NOT_IMPLEMENTED.newException("duckdb connection is unavailable")

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      // Ensure the connection is prepared
      verifyConnection
      trace("Closing DuckDB connection")
      Option(conn).foreach { c =>
        c.close()
      }
      conn = null

  private def verifyConnection: Unit =
    if !initialized.get() then
      // Wait until the connection is available
      initThread.join()

  private def getConnection: DBConnection =
    verifyConnection
    if conn == null then
      throw StatusCode.NON_RETRYABLE_INTERNAL_ERROR.newException("Failed to initialize DuckDB")
    conn

  override def withConnection[U](body: DBConnection => U): U =
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
