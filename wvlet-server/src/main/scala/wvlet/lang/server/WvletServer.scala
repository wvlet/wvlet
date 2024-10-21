package wvlet.lang.server

import org.jline.nativ.OSInfo
import wvlet.airframe.Design
import wvlet.airframe.control.Shell
import wvlet.airframe.http.netty.{Netty, NettyServer}
import wvlet.airframe.http.{Http, RxRouter}
import wvlet.airframe.launcher.{Launcher, command, option}
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.cli.OS
import wvlet.lang.runner.{QueryExecutor, WvletScriptRunnerConfig}
import wvlet.lang.runner.connector.DBConnector
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

case class WvletServerConfig(
    @option(prefix = "-p,--port", description = "Port number to listen")
    port: Int = 9090,
    @option(prefix = "-w", description = "Working directory")
    workDir: String = "."
):
  lazy val workEnv: WorkEnv = WorkEnv(path = workDir)

object WvletServer extends LogSupport:
  def router: RxRouter = RxRouter.of(RxRouter.of[FrontendApiImpl], RxRouter.of[StaticContentApi])

  def startServer(config: WvletServerConfig): Unit = design(config).build[NettyServer] { server =>
    info(s"Wvlet UI server started at http://localhost:${config.port}")
    info(s"Press ctrl+c to stop the server")
    if OS.isMacOS then
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
    // TODO Support switching DB Connector
    .bindInstance[DBConnector](DuckDBConnector(prepareTPCH = true))
    .bindInstance[WvletScriptRunnerConfig](
      WvletScriptRunnerConfig(interactive = false, catalog = Some("memory"), schema = Some("main"))
    )
    .bindSingleton[QueryExecutor]

  def testDesign: Design = design(WvletServerConfig(port = IOUtil.unusedPort)).bindProvider {
    (server: NettyServer) =>
      FrontendRPC.newRPCSyncClient(Http.client.newSyncClient(server.localAddress))
  }

end WvletServer
