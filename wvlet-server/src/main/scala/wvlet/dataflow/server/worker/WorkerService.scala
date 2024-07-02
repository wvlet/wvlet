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
package wvlet.dataflow.server.worker

import wvlet.airframe.{Design, newDesign}
import wvlet.dataflow.api.internal.Cluster.Node
import wvlet.dataflow.server.ServerModule.WorkerServer
import wvlet.dataflow.server.worker.WorkerService.*
import wvlet.dataflow.server.WorkerConfig
import wvlet.dataflow.server.util.RPCClientProvider
import wvlet.log.LogSupport

import java.net.InetAddress
import java.time.Instant
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
  */
class WorkerService(
    workerConfig: WorkerConfig,
    self: WorkerSelf,
    // Adding this dependency to start WorkerServer
    workerServer: WorkerServer,
    rpcClientProvider: RPCClientProvider,
    executor: WorkerBackgroundExecutor
) extends LogSupport:
  private lazy val coordinatorClient = rpcClientProvider.getCoordinatorClient(
    s"${workerConfig.name}-${self.name}",
    workerConfig.coordinatorAddress
  )

  // Polling coordinator every 5 seconds for heartbeat
  executor.scheduleAtFixedRate(
    () =>
      trace(s"register: ${self}")
      try coordinatorClient.CoordinatorApi.register(self)
      catch case e: Throwable => warn(e)
    ,
    0,
    5,
    TimeUnit.SECONDS
  )

object WorkerService:

  type WorkerBackgroundExecutor = ScheduledExecutorService
  type WorkerSelf               = Node

  def design: Design = newDesign
    .bind[WorkerSelf].toProvider { (workerConfig: WorkerConfig) =>
      val localHost = InetAddress.getLocalHost
      val localAddr = s"${localHost.getHostAddress}:${workerConfig.serverAddress.port}"
      Node(
        name = workerConfig.name,
        address = localAddr,
        isCoordinator = false,
        startedAt = Instant.now()
      )
    }
    .bind[WorkerBackgroundExecutor].toInstance(
      Executors.newSingleThreadScheduledExecutor()
    )
    .onShutdown(_.shutdownNow())
