package wvlet.lang.api.server

import wvlet.airframe.Design
import wvlet.airframe.http.{Http, RxRouter}
import wvlet.airframe.http.netty.{Netty, NettyServer}
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.connector.DBConnector
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.log.io.IOUtil

case class WvletServerConfig(port: Int = 8080)

object WvletServer:

  def router: RxRouter = RxRouter.of(RxRouter.of[FrontendApiImpl], RxRouter.of[StaticContentApi])

  def design(config: WvletServerConfig): Design = Netty
    .server
    .withName("wvlet-ui")
    .withPort(config.port)
    .withRouter(router)
    .design
    .bindInstance[WvletServerConfig](config)
    // TODO Switch working folder
    .bindInstance[WorkEnv](WorkEnv())
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
