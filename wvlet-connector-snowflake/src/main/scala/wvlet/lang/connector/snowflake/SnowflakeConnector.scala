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
package wvlet.lang.connector.snowflake

import wvlet.lang.catalog.SQLFunction
import wvlet.uni.control.Control
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.DataType
import wvlet.lang.connector.*
import wvlet.uni.log.LogSupport

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

/**
  * Configuration for Snowflake connection
  *
  * @param account
  *   Snowflake account identifier (e.g., "myorg-myaccount")
  * @param database
  *   Database name (catalog in SQL terminology)
  * @param schema
  *   Schema name
  * @param warehouse
  *   Virtual warehouse name
  * @param role
  *   Role name
  * @param user
  *   User name
  * @param password
  *   Password. Snowflake requires MFA for human users who sign in with a password and rejects
  *   passwords for service users, so prefer privateKeyFile or token for programs
  * @param privateKeyFile
  *   Path to a PKCS#8 private key (.p8) for key-pair authentication (authenticator SNOWFLAKE_JWT)
  * @param privateKeyPassword
  *   Passphrase of an encrypted private key file
  * @param token
  *   Programmatic access token (authenticator PROGRAMMATIC_ACCESS_TOKEN)
  */
case class SnowflakeConfig(
    account: String,
    database: String,
    schema: String,
    warehouse: Option[String] = None,
    role: Option[String] = None,
    user: Option[String] = None,
    password: Option[String] = None,
    privateKeyFile: Option[String] = None,
    privateKeyPassword: Option[String] = None,
    token: Option[String] = None
):
  def withAccount(account: String): SnowflakeConfig     = copy(account = account)
  def withDatabase(database: String): SnowflakeConfig   = copy(database = database)
  def withSchema(schema: String): SnowflakeConfig       = copy(schema = schema)
  def withWarehouse(warehouse: String): SnowflakeConfig = copy(warehouse = Some(warehouse))
  def noWarehouse(): SnowflakeConfig                    = copy(warehouse = None)
  def withRole(role: String): SnowflakeConfig           = copy(role = Some(role))
  def noRole(): SnowflakeConfig                         = copy(role = None)
  def withUser(user: String): SnowflakeConfig           = copy(user = Some(user))
  def noUser(): SnowflakeConfig                         = copy(user = None)
  def withPassword(password: String): SnowflakeConfig   = copy(password = Some(password))
  def noPassword(): SnowflakeConfig                     = copy(password = None)
  def withPrivateKeyFile(path: String): SnowflakeConfig = copy(privateKeyFile = Some(path))
  def noPrivateKeyFile(): SnowflakeConfig               = copy(privateKeyFile = None)
  def withPrivateKeyPassword(passphrase: String): SnowflakeConfig = copy(privateKeyPassword =
    Some(passphrase)
  )

  def noPrivateKeyPassword(): SnowflakeConfig   = copy(privateKeyPassword = None)
  def withToken(token: String): SnowflakeConfig = copy(token = Some(token))
  def noToken(): SnowflakeConfig                = copy(token = None)

  /**
    * The JDBC connection properties. Key-pair authentication wins over a token, and a token over a
    * password
    */
  def jdbcProperties: Properties =
    val properties = Properties()
    user.foreach(x => properties.put("user", x))
    (privateKeyFile, token) match
      case (Some(keyFile), _) =>
        properties.put("authenticator", "SNOWFLAKE_JWT")
        properties.put("private_key_file", keyFile)
        privateKeyPassword.foreach(x => properties.put("private_key_pwd", x))
      case (None, Some(t)) =>
        // The driver reads a programmatic access token from the password property
        properties.put("authenticator", "PROGRAMMATIC_ACCESS_TOKEN")
        properties.put("password", t)
      case _ =>
        password.foreach(x => properties.put("password", x))
    warehouse.foreach(x => properties.put("warehouse", x))
    properties.put("db", database)
    properties.put("schema", schema)
    role.foreach(x => properties.put("role", x))
    properties

end SnowflakeConfig

/**
  * Snowflake connector using JDBC driver
  *
  * @param config
  *   Snowflake connection configuration
  * @param workEnv
  *   Work environment
  */
class SnowflakeConnector(val config: SnowflakeConfig, workEnv: WorkEnv)
    extends DBConnector(DBType.Snowflake, workEnv)
    with LogSupport:

  // Load Snowflake JDBC driver
  Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

  private[connector] override lazy val newConnection: DBConnection =
    // Snowflake JDBC URL format:
    // jdbc:snowflake://<account>.snowflakecomputing.com/?warehouse=<warehouse>&db=<database>&schema=<schema>
    val jdbcUrl = s"jdbc:snowflake://${config.account}.snowflakecomputing.com/"

    trace(s"Connecting to Snowflake: ${jdbcUrl}")

    DBConnection(DriverManager.getConnection(jdbcUrl, config.jdbcProperties))

  override def close(): Unit = Control.closeResources(newConnection)

  override protected def withConnection[U](body: DBConnection => U): U =
    val conn = newConnection
    // Do not close the connection for reusing the connection
    body(conn)

  def withConfig(newConfig: SnowflakeConfig): SnowflakeConnector =
    new SnowflakeConnector(newConfig, workEnv)

  override def listFunctions(catalog: String): List[SQLFunction] =
    val functionList = List.newBuilder[SQLFunction]
    // Snowflake uses information_schema.functions to list functions
    // Escape single quotes to prevent SQL injection
    val escapedCatalog = catalog.replace("'", "''")
    runQuery(s"""
        |SELECT
        |  function_name,
        |  data_type as return_type,
        |  argument_signature,
        |  function_definition
        |FROM information_schema.functions
        |WHERE function_catalog = '${escapedCatalog}'
        |""".stripMargin) { rs =>
      while rs.next() do
        val functionName = rs.getString("function_name")
        val returnType   = DataType.parse(rs.getString("return_type"))
        // Parse argument signature (e.g., "(VARCHAR, NUMBER)" -> List("VARCHAR", "NUMBER"))
        val argSignature = Option(rs.getString("argument_signature"))
          .map { sig =>
            val args = sig.stripPrefix("(").stripSuffix(")").trim
            if args.isEmpty then
              List.empty
            else
              args.split(",").map(_.trim).map(DataType.parse).toList
          }
          .getOrElse(List.empty)

        functionList +=
          SQLFunction(
            name = functionName,
            functionType = SQLFunction.FunctionType.SCALAR, // Default to SCALAR, can be refined
            returnType = returnType,
            args = argSignature,
            properties = Map.empty
          )
    }
    functionList.result()

  end listFunctions

end SnowflakeConnector
