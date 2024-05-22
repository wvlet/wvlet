package com.treasuredata.flow.lang.connector.trino

import io.trino.plugin.memory.MemoryPlugin
import io.trino.server.testing.TestingTrinoServer
import wvlet.log.{LogSupport, Logger}

import java.util.logging.Level
import scala.jdk.CollectionConverters.*

class TestTrinoServer extends AutoCloseable with LogSupport:
  private def setLogLevel(loggerName: String, level: Level): Unit =
    val l = java.util.logging.Logger.getLogger(loggerName)
    l.setLevel(level)

  private val server =
    Logger.rootLogger.suppressLogs {
      setLogLevel("io.airlift", Level.WARNING)
      val trino = TestingTrinoServer.create()
      setLogLevel("io.trino", Level.WARNING)
      setLogLevel("Bootstrap", Level.WARNING)

      trino.installPlugin(new MemoryPlugin())
      trino.createCatalog("memory", "memory")
      trino
    }

  def address: String = server.getAddress.toString

  override def close(): Unit =
    for q <- server.getQueryManager.getQueries.asScala do
      if !q.getState.isDone then server.getQueryManager.cancelQuery(q.getQueryId)
    Logger.rootLogger.suppressLogs {
      server.close()
    }
