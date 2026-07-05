package wvlet.lang.server

import org.jline.nativ.OSInfo
import wvlet.lang.api.v1.flow.FlowApi
import wvlet.lang.api.v1.frontend.{FileApi, FrontendApi, FrontendRPC}
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.OS
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.lang.runner.{QueryExecutor, WvletScriptRunnerConfig}
import wvlet.uni.cli.launcher.option
import wvlet.uni.control.Control.withResource
import wvlet.uni.io.IO
import wvlet.uni.design.{Design, Session}
import wvlet.uni.http.netty.{NettyHttpServer, NettyServerConfig}
import wvlet.uni.http.rpc.{RPCRoute, RPCRouter, RPCStatus}
import wvlet.uni.http.{
  Http,
  HttpHeader,
  HttpMethod,
  HttpStatus,
  JVMHttpChannelFactory,
  Request,
  Response,
  RxHttpHandler
}
import wvlet.uni.log.LogSupport
import wvlet.uni.rx.Rx

import scala.util.Try
import scala.util.control.NonFatal

case class WvletServerConfig(
    @option(prefix = "-p,--port", description = "Web UI server port. default:9090")
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

  /**
    * Multi-service RPC dispatcher. uni-netty's `RPCHandler` only takes a single `RPCRouter`, so
    * this wraps multiple routers into one `RxHttpHandler` by building a path-keyed lookup. Mirrors
    * uni-netty `RPCHandler`'s wire format exactly: POST request, JSON `{"request": {...}}` body,
    * Weaver-encoded JSON response with `RPCStatus` header.
    */
  private class MultiRpcHandler(routers: Seq[RPCRouter]) extends RxHttpHandler with LogSupport:
    private val routeMap: Map[String, (RPCRoute, Any)] =
      routers.flatMap(r => r.routes.map(route => route.path -> (route, r.instance))).toMap

    override def handle(request: Request): Rx[Response] =
      if request.method != HttpMethod.POST then
        Rx.single(
          RPCStatus
            .INVALID_REQUEST_U1
            .newException(s"RPC requires POST method, got: ${request.method}")
            .toResponse
        )
      else
        routeMap.get(request.path) match
          case Some((route, instance)) =>
            handleRPC(request, route, instance)
          case None =>
            Rx.single(Response.notFound)

    private def handleRPC(request: Request, route: RPCRoute, instance: Any): Rx[Response] =
      try
        val body   = request.content.asString.getOrElse("")
        val args   = route.codec.decodeParams(body)
        val result = route.codec.method.call(instance, args*)
        result match
          case rx: Rx[?] =>
            rx.map(v => successResponse(v, route))
              .recover { case e =>
                errorResponse(e)
              }
          case value =>
            Rx.single(successResponse(value, route))
      catch
        case NonFatal(e) =>
          warn(s"RPC error on ${request.path}: ${e.getMessage}", e)
          Rx.single(errorResponse(e))

    private def successResponse(value: Any, route: RPCRoute): Response = Response
      .ok
      .addHeader(HttpHeader.XRPCStatus, RPCStatus.SUCCESS_S0.code.toString)
      .withJsonContent(route.codec.encodeResult(value))

    private def errorResponse(e: Throwable): Response =
      e match
        case rpc: wvlet.uni.http.rpc.RPCException =>
          rpc.toResponse
        case _ =>
          RPCStatus.INTERNAL_ERROR_I0.newException(e.getMessage, e).toResponse

  end MultiRpcHandler

  // Wrap an RPC handler with a static-content fallback. uni's path matcher doesn't support
  // airframe's "/*splat" pattern, so we cannot register a catch-all controller endpoint.
  // Anything the RPC handler answers with 404 is delegated to StaticContentApi.
  private def withStaticFallback(
      rpc: RxHttpHandler,
      staticContentApi: StaticContentApi
  ): RxHttpHandler =
    (request) =>
      rpc
        .handle(request)
        .flatMap { resp =>
          if resp.status == HttpStatus.NotFound_404 then
            Rx.single(staticContentApi.serve(request.path))
          else
            Rx.single(resp)
        }

  def startServer(config: WvletServerConfig, openBrowser: Boolean = false): Unit = design(config)
    .withProductionMode
    .build[NettyHttpServer] { server =>
      info(s"- log file path: ${config.workEnv.logFile}")
      info(s"- error file path: ${config.workEnv.errorFile}")
      info(s"Wvlet UI server started at http://localhost:${server.localPort}")
      info(s"Press ctrl+c to stop the server")

      if !config.quitImmediately then
        warn(s"--quit-immediately is set. The server will quit immediately after starting.")
        if OS.isMacOS && openBrowser then
          IO.run(s"open http://localhost:${server.localPort}")
        server.awaitTermination()
    }

  private def unusedPortFrom(start: Int, counter: Int = 0): Int =
    def isPortAvailable(port: Int): Boolean =
      Try(
        withResource(new java.net.ServerSocket(port)) { socket =>
          socket.close()
        }
      ).isSuccess

    val p = start + counter
    if isPortAvailable(p) then
      if p > start then
        warn(s"port:${start} was already used. Trying port:${p}")
      p
    else
      unusedPortFrom(start, counter + 1)

  def design(config: WvletServerConfig): Design =
    // Bind the actual port that was acquired so downstream consumers of
    // WvletServerConfig observe the same port the Netty server is listening on.
    val port         = unusedPortFrom(config.port)
    val resolvedConf = config.copy(port = port)
    Design
      .newSilentDesign
      .bindInstance[WvletServerConfig](resolvedConf)
      .bindInstance[WorkEnv](resolvedConf.workEnv)
      .bindInstance[Profile](
        Profile.getProfile(
          resolvedConf.profile,
          resolvedConf.catalog,
          resolvedConf.schema,
          Profile.defaultDuckDBProfile
        )
      )
      .bindProvider[Profile, WvletScriptRunnerConfig] { (profile: Profile) =>
        WvletScriptRunnerConfig(
          interactive = false,
          profile = profile,
          catalog = profile.defaultEngine.catalog,
          schema = profile.defaultEngine.schema
        )
      }
      // Explicit provider: ConnectorProvider's `factories` parameter has a default the DI layer
      // shouldn't try to resolve
      .bindProvider[WorkEnv, ConnectorProvider] { (workEnv: WorkEnv) =>
        ConnectorProvider(workEnv)
      }
      .bindSingleton[QueryExecutor]
      .bindImpl[FrontendApi, FrontendApiImpl]
      .bindImpl[FileApi, FileApiImpl]
      .bindImpl[FlowApi, FlowApiImpl]
      .bindSingleton[StaticContentApi]
      .bindProvider[Session, NettyHttpServer] { (session: Session) =>
        val frontendApi = session.build[FrontendApi]
        val fileApi     = session.build[FileApi]
        val flowApi     = session.build[FlowApi]
        val staticApi   = session.build[StaticContentApi]
        val rpc         = MultiRpcHandler(
          Seq(
            RPCRouter.of[FrontendApi](frontendApi),
            RPCRouter.of[FileApi](fileApi),
            RPCRouter.of[FlowApi](flowApi)
          )
        )
        val handler = withStaticFallback(rpc, staticApi)
        NettyHttpServer(
          NettyServerConfig().withName("wvlet-ui").withPort(port).withRxHandler(handler)
        )
      }
      .onStart(_.start())
      .onShutdown(_.stop())

  end design

  // Bind port = 0 so the design's unusedPortFrom helper acquires whichever local
  // port the OS hands back — same effect the dropped airframe IOUtil.unusedPort had.
  def testDesign: Design = design(WvletServerConfig(port = 0)).bindProvider {
    (server: NettyHttpServer) =>
      // Force the JVM HTTP channel factory before constructing the client.
      // HttpCompat (which auto-registers the factory) is package-private, so we
      // can't trigger its static init from outside wvlet.uni.http. Calling
      // setDefaultChannelFactory directly is idempotent and matches what
      // HttpCompat would do.
      Http.setDefaultChannelFactory(JVMHttpChannelFactory)
      FrontendRPC.newRPCSyncClient(
        Http.client.withBaseUri(s"http://localhost:${server.localPort}").newSyncClient
      )
  }

end WvletServer
