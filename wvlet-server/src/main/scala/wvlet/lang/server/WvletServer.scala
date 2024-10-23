package wvlet.lang.server

import org.jline.nativ.OSInfo
import wvlet.airframe.Design
import wvlet.airframe.control.Shell
import wvlet.airframe.http.netty.{Netty, NettyServer}
import wvlet.airframe.http.{Http, RxRouter}
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.OS
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.{QueryExecutor, WvletScriptRunnerConfig}
import wvlet.lang.runner.connector.{DBConnector, DBConnectorProvider}
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

case class WvletServerConfig(
    @option(prefix = "-p,--port", description = "Port number to listen")
    port: Int = 9090,
    @option(prefix = "-w", description = "Working directory")
    workDir: String = ".",
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None,
    @option(
      prefix = "--quit-immediately",
      description = "Quit the server immediately after starting. Only for boot testing"
    )
    quitImmediately: Boolean = false,
    @option(prefix = "--tpch", description = "Load a small demo TPC-H data (DuckDB only)")
    prepareTPCH: Boolean = false
):
  lazy val workEnv: WorkEnv = WorkEnv(path = workDir)

object WvletServer extends LogSupport:
  def router: RxRouter = RxRouter
    .of(RxRouter.of[FrontendApiImpl], RxRouter.of[FileApiImpl], RxRouter.of[StaticContentApi])

  def startServer(config: WvletServerConfig, openBrowser: Boolean = false): Unit = design(config)
    .build[NettyServer] { server =>
      info(s"Wvlet UI server started at http://localhost:${config.port}")
      info(s"Press ctrl+c to stop the server")

      if !config.quitImmediately then
        if OS.isMacOS && openBrowser then
          // Open the web browser
          Shell.exec(s"open http://localhost:${config.port}")
        server.awaitTermination()
    }

  def design(config: WvletServerConfig): Design = Netty
    .server
    .withName("wvlet-ui")
    .withPort(config.port)
    .withRouter(router)
    .design
    .bindInstance[WvletServerConfig](config)
    // TODO Switch working folder
    .bindInstance[WorkEnv](config.workEnv)
    .bindInstance[Profile](Profile.getProfile(config.profile, config.catalog, config.schema))
    // TODO Support switching DB Connector
    .bindProvider[Profile, DBConnector] { p =>
      val prop = Map("prepareTPCH" -> config.prepareTPCH)
      DBConnectorProvider.getConnector(p)
    }
    .bindInstance[WvletScriptRunnerConfig](
      WvletScriptRunnerConfig(interactive = false, catalog = Some("memory"), schema = Some("main"))
    )
    .bindSingleton[QueryExecutor]

  def testDesign: Design = design(WvletServerConfig(port = IOUtil.unusedPort)).bindProvider {
    (server: NettyServer) =>
      FrontendRPC.newRPCSyncClient(Http.client.newSyncClient(server.localAddress))
  }

end WvletServer
