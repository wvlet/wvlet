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

import com.google.common.collect.ImmutableList
import com.google.inject.multibindings.MapBinder.newMapBinder
import com.google.inject.Binder
import com.google.inject.Inject
import com.google.inject.Module
import com.google.inject.Scopes
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystemFactory
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer
import io.trino.plugin.deltalake.DeltaLakeConnectorFactory
import io.trino.plugin.deltalake.DeltaLakePlugin
import io.trino.spi.connector.Connector
import io.trino.spi.connector.ConnectorContext
import io.trino.spi.connector.ConnectorFactory
import io.trino.spi.connector.ConnectorSession

import java.io.IOException
import java.io.UncheckedIOException
import java.nio.file.Path

class TestingDeltaLakePlugin(localFileSystemRootPath: Path) extends DeltaLakePlugin:
  override def getConnectorFactories: java.lang.Iterable[ConnectorFactory] = ImmutableList.of(
    new ConnectorFactory():
      def getName: String = DeltaLakeConnectorFactory.CONNECTOR_NAME

      def create(
          catalogName: String,
          config: java.util.Map[String, String],
          context: ConnectorContext
      ): Connector =
        localFileSystemRootPath.toFile.mkdirs
        DeltaLakeConnectorFactory.createConnector(
          catalogName,
          config,
          context,
          java.util.Optional.empty(),
          (binder) => binder.install(new TestDeltaLakeModule)
        )
  )

class TestDeltaLakeModule extends com.google.inject.Module:
  override def configure(binder: Binder): Unit = newMapBinder(
    binder,
    classOf[String],
    classOf[TransactionLogSynchronizer]
  ).addBinding("file").to(classOf[FileTestingTransactionLogSynchronizer]).in(Scopes.SINGLETON)

class FileTestingTransactionLogSynchronizer @Inject (fileSystemFactory: TrinoFileSystemFactory)
    extends TransactionLogSynchronizer:
  def isUnsafe = true

  def write(
      session: ConnectorSession,
      clusterId: String,
      newLogEntryPath: Location,
      entryContents: Array[Byte]
  ): Unit =
    try
      val fileSystem = fileSystemFactory.create(session)
      val outputFile = fileSystem.newOutputFile(newLogEntryPath)
      outputFile.createOrOverwrite(entryContents)
    catch
      case e: IOException =>
        throw new UncheckedIOException(e)
