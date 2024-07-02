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
package wvlet.dataflow.server

import wvlet.airframe.http.client.SyncClient
import wvlet.airframe.http.netty.{Netty, NettyServer, NettyServerConfig}
import wvlet.airframe.http.{Http, Router, RxRouter, ServerAddress}
import wvlet.airframe.{Design, Session, newDesign}
import wvlet.dataflow.api.ServiceInfoApi
import wvlet.dataflow.api.internal.coordinator.CoordinatorRPC
import wvlet.dataflow.api.internal.worker.WorkerRPC
import wvlet.dataflow.api.v1.WvletRPC
import wvlet.dataflow.server.coordinator.{CoordinatorApiImpl, CoordinatorConfig, TaskApiImpl, TaskManager}
import wvlet.dataflow.server.frontend.FrontendApiImpl
import wvlet.dataflow.server.worker.{WorkerApiImpl, WorkerService}
import wvlet.log.LogSupport

import java.net.ServerSocket
import javax.annotation.PostConstruct

case class WorkerConfig(
    name: String = "worker-1",
    serverAddress: ServerAddress,
    coordinatorAddress: ServerAddress
):
  def port: Int = serverAddress.port

case class FrontendServerConfig(
    name: String = "frontend-server",
    serverAddress: ServerAddress,
    coordinatorAddress: ServerAddress
):
  def port: Int = serverAddress.port

/**
  */
object ServerModule extends LogSupport:
  class CoordinatorClient(client: SyncClient) extends CoordinatorRPC.RPCSyncClient(client)
  class WorkerClient(client: SyncClient)      extends WorkerRPC.RPCSyncClient(client)
  class ApiClient(client: SyncClient)         extends WvletRPC.RPCSyncClient(client)

  class CoordinatorServer(config: CoordinatorConfig, session: Session) extends AutoCloseable:
    private var server: Option[NettyServer] = None
    def localAddress: String                = config.serverAddress.hostAndPort
    @PostConstruct
    def start: Unit =
      server = Some(coordinatorServer(config).newServer(session))

    def awaitTermination(): Unit =
      server.foreach(_.awaitTermination())

    override def close(): Unit =
      server.foreach(_.close())

  class WorkerServer(config: WorkerConfig, session: Session) extends AutoCloseable:
    private var server: Option[NettyServer] = None
    def localAddress: String                = config.serverAddress.hostAndPort

    @PostConstruct
    def start: Unit =
      server = Some(workerServer(config).newServer(session))

    def awaitTermination(): Unit =
      server.foreach(_.awaitTermination())

    override def close(): Unit =
      server.foreach(_.close())

  class FrontendServer(config: FrontendServerConfig, session: Session) extends AutoCloseable:
    private var server: Option[NettyServer] = None

    def localAddress: String = config.serverAddress.hostAndPort

    @PostConstruct
    def start: Unit =
      server = Some(frontendServer(config).newServer(session))

    def awaitTermination(): Unit =
      server.foreach(_.awaitTermination())

    override def close(): Unit =
      server.foreach(_.close())

  def coordinatorRouter = RxRouter.of(
    RxRouter.of[ServiceInfoApi],
    RxRouter.of[CoordinatorApiImpl],
    RxRouter.of[TaskApiImpl]
  )

  def workerRouter = RxRouter.of(
    RxRouter.of[ServiceInfoApi],
    RxRouter.of[WorkerApiImpl]
  )

  def frontendServerRouter = RxRouter.of(
    RxRouter.of[ServiceInfoApi],
    RxRouter.of[FrontendApiImpl]
  )

  private def coordinatorServer(config: CoordinatorConfig): NettyServerConfig =
    Netty.server
      .withName(config.name)
      .withPort(config.port)
      .withRouter(coordinatorRouter)

  private def workerServer(config: WorkerConfig): NettyServerConfig =
    Netty.server
      .withName(config.name)
      .withPort(config.port)
      .withRouter(workerRouter)

  private def frontendServer(config: FrontendServerConfig): NettyServerConfig =
    Netty.server
      .withName(config.name)
      .withPort(config.port)
      .withRouter(frontendServerRouter)

  def coordinatorDesign(config: CoordinatorConfig): Design =
    newDesign
      .bind[CoordinatorConfig].toInstance(config)
      .bind[CoordinatorServer].toSingleton
      .add(TaskManager.design)

  def workerDesign(config: WorkerConfig): Design =
    WorkerService.design
      .bind[WorkerConfig].toInstance(config)
      .bind[WorkerServer].toSingleton
      .add(PluginManager.design)

  def frontendServerDesign(config: FrontendServerConfig): Design =
    newDesign
      .bind[FrontendServerConfig].toInstance(config)
      .bind[FrontendServer].toSingleton

  private def randomPort(num: Int): Seq[Int] =
    val sockets = (0 until num).map(i => new ServerSocket(0))
    val ports   = sockets.map(_.getLocalPort).toIndexedSeq
    sockets.foreach(_.close())
    ports

  def standaloneDesign(coordinatorPort: Int, workerPort: Int, frontendServerPort: Int): Design =
    coordinatorDesign(
      CoordinatorConfig(serverAddress = ServerAddress(s"localhost:${coordinatorPort}"))
    )
      .add(
        workerDesign(
          WorkerConfig(
            serverAddress = ServerAddress(s"localhost:${workerPort}"),
            coordinatorAddress = ServerAddress(s"localhost:${coordinatorPort}")
          )
        )
      )
      .add(
        frontendServerDesign(
          FrontendServerConfig(
            serverAddress = ServerAddress(s"localhost:${frontendServerPort}"),
            coordinatorAddress = ServerAddress(s"localhost:${coordinatorPort}")
          )
        )
      )
      .bind[ApiClient].toInstance {
        ApiClient(Http.client.withName("api-client").newSyncClient(s"localhost:${coordinatorPort}"))
      }
      .bind[CoordinatorClient].toInstance {
        CoordinatorClient(
          Http.client.withName("coordinator-client").newSyncClient(s"localhost:${coordinatorPort}")
        )
      }

  /**
    * Design for launching a test server and client
    * @return
    */
  def testServerAndClient: Design =
    val Seq(coordinatorPort, workerPort) = randomPort(2)
    coordinatorDesign(
      CoordinatorConfig(serverAddress = ServerAddress(s"localhost:${coordinatorPort}"))
    )
      .add(
        workerDesign(
          WorkerConfig(
            serverAddress = ServerAddress(s"localhost:${workerPort}"),
            coordinatorAddress = ServerAddress(s"localhost:${coordinatorPort}")
          )
        )
      )
      .bind[CoordinatorClient].toProvider { (server: CoordinatorServer) =>
        CoordinatorClient(
          Http.client.withName("coordinator-client").newSyncClient(server.localAddress)
        )
      }
      .bind[ApiClient].toProvider { (server: CoordinatorServer) =>
        ApiClient(Http.client.withName("api-client").newSyncClient(server.localAddress))
      }
      // Add this design to start up worker service early
      .bind[WorkerService].toEagerSingleton
