package com.treasuredata.flow.lang.connector.trino

import com.treasuredata.flow.lang.connector.DBContext
import com.treasuredata.flow.lang.model.sql.*
import com.treasuredata.flow.lang.model.sql.SqlExpr.*
import io.trino.jdbc.TrinoDriver

import java.sql.Connection
import java.util.Properties

case class TrinoConfig(
    catalog: String,
    schema: String,
    hostAndPort: String,
    useSSL: Boolean = true,
    user: Option[String] = None,
    password: Option[String] = None
)

class TrinoContext(val config: TrinoConfig) extends DBContext:
  private lazy val driver = new TrinoDriver()

  override protected def newConnection: Connection =
    val jdbcUrl =
      s"jdbc:trino://${config.hostAndPort}/${config.catalog}/${config.schema}${
          if config.useSSL then
            "?SSL=true"
          else
            ""
        }"
    val properties = new Properties()
    config.user.foreach(x => properties.put("user", x))
    config.password.foreach(x => properties.put("password", x))

    driver.connect(jdbcUrl, properties)

  override def close(): Unit = driver.close()

  def withConfig(newConfig: TrinoConfig): TrinoContext = new TrinoContext(newConfig)

  override def IString: IString   = TrinoString(using this)
  override def IBoolean: IBoolean = TrinoBoolean(using this)
  override def IInt: IInt         = TrinoInt(using this)
  override def ILong: ILong       = TrinoLong(using this)
  override def IFloat: IFloat     = TrinoFloat(using this)
  override def IDouble: IDouble   = TrinoDouble(using this)

  class TrinoString(using ctx: TrinoContext) extends IString:
    override def toInt     = sql"cast(${self} as int)"
    override def toLong    = sql"cast(${self} as bigint)"
    override def toFloat   = sql"cast(${self} as real)"
    override def toDouble  = sql"cast(${self} as double)"
    override def toBoolean = sql"cast(${self} as boolean)"
    override def length    = sql"length(${self})"

    override def substring(start: SqlExpr)               = sql"substring(${self}, ${start})"
    override def substring(start: SqlExpr, end: SqlExpr) = sql"substring(${self}, ${start}, ${end})"
    override def regexpContains(pattern: SqlExpr)        = sql"regexp_like(${self}, ${pattern})"

  class TrinoBoolean(using ctx: TrinoContext) extends IBoolean
  class TrinoInt(using ctx: TrinoContext)     extends IInt
  class TrinoLong(using ctx: TrinoContext)    extends ILong
  class TrinoFloat(using ctx: TrinoContext)   extends IFloat
  class TrinoDouble(using ctx: TrinoContext)  extends IDouble
