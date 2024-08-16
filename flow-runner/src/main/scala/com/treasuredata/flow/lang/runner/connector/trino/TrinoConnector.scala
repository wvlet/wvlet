package com.treasuredata.flow.lang.runner.connector.trino

import com.treasuredata.flow.lang.catalog.SQLFunction
import com.treasuredata.flow.lang.compiler.DBType
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.sql.*
import com.treasuredata.flow.lang.runner.connector.DBConnector
import io.trino.jdbc.{TrinoDriver, TrinoResultSet}
import wvlet.log.LogSupport

import java.sql.{Connection, ResultSet, SQLWarning}
import java.util.Properties

case class TrinoConfig(
    catalog: String,
    schema: String,
    hostAndPort: String,
    useSSL: Boolean = true,
    user: Option[String] = None,
    password: Option[String] = None
)

class TrinoConnector(val config: TrinoConfig) extends DBConnector(DBType.Trino) with LogSupport:
  private lazy val driver = new TrinoDriver()

  override protected def newConnection: Connection =
    val jdbcUrl =
      s"jdbc:trino://${config.hostAndPort}/${config.catalog}/${config.schema}${
          if config.useSSL then
            "?SSL=true"
          else
            ""
        }"
    trace(s"Connecting to Trino: ${jdbcUrl}")
    val properties = new Properties()
    config.user.foreach(x => properties.put("user", x))
    config.password.foreach(x => properties.put("password", x))

    driver.connect(jdbcUrl, properties)

  override def close(): Unit = driver.close()

  def withConfig(newConfig: TrinoConfig): TrinoConnector = new TrinoConnector(newConfig)

  override def listFunctions(catalog: String): List[SQLFunction] =
    val functionList = List.newBuilder[SQLFunction]
    runQuery("show functions") { rs =>
      while rs.next() do
        val returnType    = DataType.parse(rs.getString("Return Type"))
        val argumentTypes = rs.getString("Argument Types").split(", ").map(DataType.parse).toList
        val prop          = Map.newBuilder[String, Any]
        Option(rs.getString("description")).foreach(x => prop += "description" -> x)
        functionList +=
          SQLFunction(
            name = rs.getString("Function"),
            functionType = SQLFunction
              .FunctionType
              .valueOf(rs.getString("Function Type").toUpperCase),
            returnType = returnType,
            args = argumentTypes,
            properties = prop.result()
          )
    }
    functionList.result()

end TrinoConnector
