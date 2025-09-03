package wvlet.lang.runner.connector

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.{DBType, WorkEnv}
import wvlet.lang.compiler.DBType.DuckDB
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.{TrinoConfig, TrinoConnector}
import wvlet.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

class DBConnectorProvider(workEnv: WorkEnv) extends LogSupport with AutoCloseable:

  private val connectorCache = new ConcurrentHashMap[Profile, DBConnector]().asScala

  override def close(): Unit = connectorCache.values.foreach(_.close())

  def getConnector(profile: Profile, properties: Map[String, Any] = Map.empty): DBConnector =
    def createConnector: DBConnector =
      val dbType = profile.dbType
      debug(s"Get a connector for DBType:${dbType}")
      dbType match
        case DBType.Trino =>
          val props = profile.properties ++ properties
          def parseBoolean(v: Any): Option[Boolean] = v match
            case b: java.lang.Boolean => Some(b.booleanValue())
            case b: Boolean           => Some(b)
            case n: java.lang.Integer => Some(n.intValue() != 0)
            case n: Int               => Some(n != 0)
            case s: String =>
              s.trim.toLowerCase match
                case "true" | "1" | "yes" | "y" | "on"   => Some(true)
                case "false" | "0" | "no" | "n" | "off" => Some(false)
                case _                                     => None
            case _ => None

          val useSSL = props.get("useSSL").flatMap(parseBoolean).getOrElse(true)
          TrinoConnector(
            TrinoConfig(
              catalog = profile.catalog.getOrElse("default"),
              schema = profile.schema.getOrElse("default"),
              hostAndPort = profile.host.getOrElse("localhost"),
              useSSL = useSSL,
              user = profile.user,
              password = profile.password
            ),
            workEnv
          )
        case DBType.DuckDB =>
          DuckDBConnector(
            workEnv,
            // TODO Use more generic way to pass profile properties
            prepareTPCH = (profile.properties ++ properties)
              .getOrElse("prepareTPCH", "false")
              .toString
              .toBoolean,
            prepareTPCDS = (profile.properties ++ properties)
              .getOrElse("prepareTPCDS", "false")
              .toString
              .toBoolean
          )
        case DBType.Generic =>
          GenericConnector(workEnv)
        case other =>
          warn(
            s"Connector for -t ${other
                .toString
                .toLowerCase} option is not implemented. Using GenericConnector for DuckDB as a fallback"
          )
          GenericConnector(workEnv)
      end match
    end createConnector

    connectorCache.getOrElseUpdate(profile, createConnector)

  end getConnector

  def getConnector(dbType: DBType, profileName: Option[String]): DBConnector =
    val profile =
      profileName match
        case Some(name) =>
          Profile.getProfile(name).getOrElse(Profile.defaultProfileFor(dbType))
        case None =>
          // Create a minimal profile for the given DB type
          Profile.defaultProfileFor(dbType)
    getConnector(profile)

end DBConnectorProvider
