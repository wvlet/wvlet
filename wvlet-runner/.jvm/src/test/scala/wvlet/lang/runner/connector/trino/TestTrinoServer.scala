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
package wvlet.lang.runner.connector.trino

import io.trino.plugin.memory.MemoryPlugin
import io.trino.server.testing.TestingTrinoServer
import wvlet.uni.log.LogSupport
import wvlet.uni.log.Logger

import java.util.logging.Level
import scala.jdk.CollectionConverters.*

class TestTrinoServer() extends AutoCloseable with LogSupport:
  private def setLogLevel(loggerName: String, level: Level): Unit =
    val l = java.util.logging.Logger.getLogger(loggerName)
    l.setLevel(level)

  private val trino = Logger
    .rootLogger
    .suppressLogs {
      setLogLevel("io.airlift", Level.WARNING)
      val server = TestingTrinoServer.create()
      setLogLevel("io.trino", Level.WARNING)
      setLogLevel("Bootstrap", Level.WARNING)
      server
    }

  def withCustomMemoryPlugin: TestTrinoServer =
    trino.installPlugin(CustomMemoryPlugin())
    trino.createCatalog("memory", "wvlet")
    this

  def withMemoryPlugin: TestTrinoServer =
    trino.installPlugin(MemoryPlugin())
    trino.createCatalog("memory", "memory")
    this

  def address: String = trino.getAddress.toString

  override def close(): Unit =
    for q <- trino.getQueryManager.getQueries.asScala do
      if !q.getState.isDone then
        trino.getQueryManager.cancelQuery(q.getQueryId)
    Logger
      .rootLogger
      .suppressLogs {
        trino.close()
      }

    // io.airlift redirects stdout/stderr to loggers, so we need to clear all handlers
    Logger.init

end TestTrinoServer
