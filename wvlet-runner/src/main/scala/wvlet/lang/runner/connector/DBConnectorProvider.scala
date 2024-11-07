package wvlet.lang.runner.connector

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.DBType.DuckDB
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
import wvlet.log.LogSupport

object DBConnectorProvider extends LogSupport:

  def getConnector(profile: Profile, properties: Map[String, Any] = Map.empty): DBConnector =
    val dbType = profile.dbType
    debug(s"Get a connector for DBType:${dbType}")
    dbType match
      case DBType.Trino =>
        TrinoConnector(
          TrinoConfig(
            catalog = profile.catalog.getOrElse("default"),
            schema = profile.schema.getOrElse("default"),
            hostAndPort = profile.host.getOrElse("localhost"),
            user = profile.user,
            password = profile.password
          )
        )
      case DBType.DuckDB =>
        DuckDBConnector(
          // TODO Use more generic way to pass profile properties
          prepareTPCH = (profile.properties ++ properties)
            .getOrElse("prepareTPCH", "false")
            .toString
            .toBoolean
        )
      case DBType.Generic =>
        GenericConnector()
      case other =>
        warn(
          s"Connector for -t ${other.toString.toLowerCase} option is not implemented. Using GenericConnector for DuckDB as a fallback"
        )
        GenericConnector()

end DBConnectorProvider
