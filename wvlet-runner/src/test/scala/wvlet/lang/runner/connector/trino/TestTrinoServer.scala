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

import io.trino.plugin.deltalake.DeltaLakeConnectorFactory
import io.trino.plugin.deltalake.DeltaLakePlugin
import io.trino.plugin.memory.MemoryPlugin
import io.trino.server.testing.TestingTrinoServer
import wvlet.airframe.control.Resource
import wvlet.airframe.ulid.ULID
import wvlet.log.LogSupport
import wvlet.log.Logger

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.logging.Level
import scala.jdk.CollectionConverters.*

class TestTrinoServer() extends AutoCloseable with LogSupport:
  private def setLogLevel(loggerName: String, level: Level): Unit =
    val l = java.util.logging.Logger.getLogger(loggerName)
    l.setLevel(level)

  private val tempMetastoreDir =
    val dir = new File(s"target/trino-hive-metastore/${ULID.newULIDString}")
    dir.mkdirs()
    dir

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

  def withDeltaLakePlugin: TestTrinoServer =
    trino.installPlugin(MemoryPlugin())
    trino.createCatalog("memory", "memory")

    // For supporting insert into to Delta Lake, need to provide TransactionLogSynchronizer implementation to the plugin
    trino.installPlugin(new TestingDeltaLakePlugin(tempMetastoreDir.toPath))

    info(s"Using metastore dir: ${tempMetastoreDir}")
    trino.createCatalog(
      "delta",
      DeltaLakeConnectorFactory.CONNECTOR_NAME,
      Map[String, String](
        "hive.metastore"                         -> "file",
        "hive.metastore.catalog.dir"             -> s"file://${tempMetastoreDir.getAbsolutePath}",
        "hive.metastore.disable-location-checks" -> "true",
        "fs.hadoop.enabled"                      -> "true",
        // Allow call delta system.register_table
        "delta.register-table-procedure.enabled" -> "true",
        "delta.enable-non-concurrent-writes"     -> "true"
      ).asJava
    )
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

    // clean up tempMetastoreDir files and dirs recursively

    def delete(f: File): Unit =
      if f.isFile then
        f.delete()
      else if f.isDirectory then
        f.listFiles() match
          case lst: Array[File] =>
            lst.foreach(delete)
          case null =>
        f.delete()

    delete(tempMetastoreDir)

    // io.airlift redirects stdout/stderr to loggers, so we need to clear all handlers
    Logger.init

end TestTrinoServer
