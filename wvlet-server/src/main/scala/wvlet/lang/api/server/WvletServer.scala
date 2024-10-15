package wvlet.lang.api.server

import wvlet.airframe.Design
import wvlet.airframe.http.{Http, RxRouter}
import wvlet.airframe.http.netty.{Netty, NettyServer}
import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.log.io.IOUtil

case class WvletServerConfig(port: Int = 8080)

object WvletServer:

  def router: RxRouter = RxRouter.of[FrontendApiImpl]

  def design(config: WvletServerConfig): Design = Netty
    .server
    .withName("wvlet-server")
    .withPort(config.port)
    .withRouter(router)
    .design
    .bindInstance[WvletServerConfig](config)

  def testDesign: Design = design(WvletServerConfig(port = IOUtil.unusedPort)).bindProvider {
    (server: NettyServer) =>
      FrontendRPC.newRPCSyncClient(Http.client.newSyncClient(server.localAddress))
  }
