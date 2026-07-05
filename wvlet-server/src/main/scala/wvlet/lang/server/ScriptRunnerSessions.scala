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
package wvlet.lang.server

import wvlet.lang.compiler.WorkEnv
import wvlet.lang.runner.QueryExecutor
import wvlet.lang.runner.ThreadManager
import wvlet.lang.runner.WvletScriptRunner
import wvlet.lang.runner.WvletScriptRunnerConfig
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.log.LogSupport

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

/**
  * One [[WvletScriptRunner]] (compiler + query executor) per client session, so `use` statements —
  * which switch the session's active engine and mutate the compiler's default catalog/schema — do
  * not leak between concurrent clients (#1867). Heavy resources (database connections through the
  * shared [[ConnectorProvider]], the background thread pool) stay shared; only the per-session
  * compilation and engine-selection state is isolated.
  *
  * Requests without a session id share one default session, preserving the previous single-runner
  * behavior. Sessions idle longer than the timeout are evicted on access; growth is bounded by the
  * timeout since every live client keeps touching its own session.
  */
class ScriptRunnerSessions(
    workEnv: WorkEnv,
    config: WvletScriptRunnerConfig,
    connectorProvider: ConnectorProvider,
    threadManager: ThreadManager,
    idleTimeout: Duration
) extends AutoCloseable
    with LogSupport:

  private class Entry(val runner: WvletScriptRunner):
    @volatile
    var lastAccessMillis: Long = System.currentTimeMillis()

    def touch(): Unit = lastAccessMillis = System.currentTimeMillis()

  private val sessions = ConcurrentHashMap[String, Entry]()

  override def close(): Unit =
    sessions.values().asScala.foreach(e => e.runner.close())
    sessions.clear()

  /**
    * The runner of the given session, creating it on first use. Requests without a session id share
    * the default session
    */
  def runnerFor(sessionId: Option[String]): WvletScriptRunner =
    evictIdleSessions()
    val key = sessionId.getOrElse(ScriptRunnerSessions.DefaultSessionKey)
    // Create-or-touch atomically: compute and the eviction's computeIfPresent serialize on the
    // same key, so an entry can never be touched after eviction closed it
    val entry = sessions.compute(
      key,
      (k, existing) =>
        if existing == null then
          debug(s"Creating a new script-runner session: ${k}")
          Entry(newRunner())
        else
          existing.touch()
          existing
    )
    entry.runner

  /** The number of live sessions (for monitoring and tests) */
  def sessionCount: Int = sessions.size()

  private def newRunner(): WvletScriptRunner = WvletScriptRunner(
    workEnv,
    // Session runners are created on demand by the first request, so a background source
    // pre-compilation would race that request's own compilation within one compiler
    config.copy(precompileSourcePaths = false),
    QueryExecutor(connectorProvider, config.profile, workEnv),
    threadManager
  )

  private def evictIdleSessions(): Unit =
    val cutoff = System.currentTimeMillis() - idleTimeout.toMillis
    sessions
      .keySet()
      .asScala
      .foreach { key =>
        // The default session stays: it is the compatibility path for id-less clients
        if key != ScriptRunnerSessions.DefaultSessionKey then
          // Re-check the idle time inside computeIfPresent: it serializes with runnerFor's
          // compute on the same key, so a session touched concurrently is not evicted
          sessions.computeIfPresent(
            key,
            (k, entry) =>
              if entry.lastAccessMillis < cutoff then
                debug(s"Evicting idle script-runner session: ${k}")
                // Closing the runner is safe for in-flight queries: the underlying database
                // connections belong to the shared ConnectorProvider and stay open
                entry.runner.close()
                null
              else
                entry
          )
      }

end ScriptRunnerSessions

object ScriptRunnerSessions:
  /** Session key of requests that carry no session id */
  val DefaultSessionKey = "__default"

  /** Sessions idle longer than this are evicted */
  val DefaultIdleTimeout: Duration = Duration(1, java.util.concurrent.TimeUnit.HOURS)
