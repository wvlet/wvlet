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
package wvlet.dataflow.server.coordinator

import wvlet.dataflow.api.internal.Cluster.{Node, NodeInfo}
import wvlet.log.LogSupport

import java.net.InetAddress
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
  */
class NodeManager(coordinatorConfig: CoordinatorConfig) extends LogSupport:
  import scala.jdk.CollectionConverters.*

  private val self: Node =
    val localHost = InetAddress.getLocalHost
    val localAddr = s"${localHost.getHostAddress}:${coordinatorConfig.serverAddress.port}"
    Node(
      name = coordinatorConfig.name,
      address = localAddr,
      isCoordinator = true,
      startedAt = Instant.now()
    )

  private val heartBeatRecord = new ConcurrentHashMap[Node, Instant]().asScala

  def heartBeat(node: Node): Unit =
    heartBeatRecord.getOrElseUpdate(
      node, {
        info(s"A new worker node is joined: ${node}")
        Instant.now()
      }
    )
    heartBeatRecord.put(node, Instant.now())

  def listNodes: Seq[NodeInfo] =
    val b = Seq.newBuilder[NodeInfo]
    b += NodeInfo(self, Instant.now())
    heartBeatRecord.foreach { case (n, hb) => b += NodeInfo(n, hb) }
    b.result()

  def listWorkerNodes: Seq[NodeInfo] = listNodes.filterNot(_.isCoordinator)
