package com.treasuredata.flow.lang.connector.trino

import com.google.common.collect.ImmutableList
import com.google.inject.multibindings.MapBinder.newMapBinder
import com.google.inject.{Binder, Inject, Module, Scopes}
import io.trino.filesystem.{Location, TrinoFileSystemFactory}
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer
import io.trino.plugin.deltalake.{DeltaLakeConnectorFactory, DeltaLakePlugin}
import io.trino.spi.connector.{Connector, ConnectorContext, ConnectorFactory, ConnectorSession}

import java.io.{IOException, UncheckedIOException}
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
  )
    .addBinding("file").to(classOf[FileTestingTransactionLogSynchronizer]).in(Scopes.SINGLETON)

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
    catch case e: IOException => throw new UncheckedIOException(e)
