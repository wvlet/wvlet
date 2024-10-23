package wvlet.lang.runner.connector

import wvlet.lang.catalog.Profile
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
import wvlet.log.LogSupport

object DBConnectorProvider extends LogSupport:

  def getConnector(profile: Profile, properties: Map[String, Any] = Map.empty): DBConnector =
    profile.`type` match
      case "trino" =>
        TrinoConnector(
          TrinoConfig(
            catalog = profile.catalog.getOrElse("default"),
            schema = profile.schema.getOrElse("default"),
            hostAndPort = profile.host.getOrElse("localhost"),
            user = profile.user,
            password = profile.password
          )
        )
      case _ =>
        DuckDBConnector(
          // TODO Use more generic way to pass profile properties
          prepareTPCH = (profile.properties ++ properties)
            .getOrElse("prepareTPCH", "false")
            .toString
            .toBoolean
        )

end DBConnectorProvider
