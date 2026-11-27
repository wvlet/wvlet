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
package wvlet.lang.runner

import wvlet.lang.api.StatusCode
import wvlet.lang.api.v1.flow.StageState
import wvlet.lang.catalog.ConnectorConfig
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.pipeline.BuildModel
import wvlet.lang.compiler.pipeline.ModelBuildPlanner
import wvlet.lang.compiler.query.QueryProgressMonitor
import wvlet.lang.runner.compat.Sleep
import wvlet.lang.runner.connector.SqlConnectorProvider
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.log.LogSupport
import wvlet.uni.util.ULID

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * `wvlet build` of the npm and Native CLIs: builds the selected models of a [[ModelBuildPlan]] one
  * at a time, in build order, on the caller. Models run on the in-process wvlet engine (tables
  * persist in its `warehouse`), DuckDB, or Trino, through the same engine sessions and
  * [[SqlConnectorProvider]] as `wvlet run`.
  *
  * Compared with the JVM build on the flow executor: models run one at a time, `timeout` is not
  * enforced (a query on Node.js can't be interrupted), no run record is written, and a model can't
  * read a model built on another engine. `retries`, `retry_delay`, `backoff`, and `max_retry_delay`
  * apply as on the JVM, and a failed model skips its downstream models.
  */
class ModelBuilder(profile: Profile, workEnv: WorkEnv) extends AutoCloseable with LogSupport:

  private val connectorProvider = SqlConnectorProvider(profile)

  // One in-process engine session per wvlet connector, so a model reads the tables built before it
  private val engineSessions = mutable.Map.empty[ConnectorConfig, WvletEngineSession]

  private def engineSessionOf(config: ConnectorConfig): Option[WvletEngineSession] =
    if WvletEngineSession.isInProcess(config) then
      Some(engineSessions.getOrElseUpdate(config, WvletEngineSession(config, workEnv.path)))
    else
      None

  /** The profile connector a model is built on: its `engine:`, or the profile's default engine */
  private def engineOf(m: BuildModel): ConnectorConfig = m
    .spec
    .engine
    .map { name =>
      profile
        .connectors
        .find(_.name == name)
        .getOrElse(
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(s"Connector '${name}' of model ${m.name} is not defined in the profile")
        )
    }
    .getOrElse(profile.defaultEngine)

  /** Whether the target relation of a model is readable on its engine */
  private def isBuilt(m: BuildModel)(using ctx: Context): Boolean =
    val config = engineOf(m)
    engineSessionOf(config) match
      case Some(session) =>
        session.hasTable(m.targetName)
      case None =>
        try
          given QueryProgressMonitor = ctx.queryProgressMonitor
          connectorProvider.getConnector(config).execute(s"select 1 from ${m.targetName} limit 0")
          true
        catch
          case NonFatal(e) =>
            debug(s"${m.targetName} is not readable: ${e.getMessage}")
            false

  /**
    * Fail when a selected model reads a model built on another engine: the JVM flow executor stages
    * such tables, this builder doesn't
    */
  private def checkSameEngineReads(plan: ModelBuildPlan): Unit = plan
    .selected
    .foreach { m =>
      val engine = engineOf(m).name
      plan
        .graph
        .upstreams(m.name)
        .flatMap(plan.graph.get)
        .find(u => engineOf(u).name != engine)
        .foreach { u =>
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(
              s"Model ${m.name} runs on '${engine}' and reads ${u.name} built on '${engineOf(u)
                  .name}'. Reading models across engines needs the JVM wvlet CLI"
            )
        }
    }

  /** Run the save statement of a model on its engine */
  private def buildModel(m: BuildModel, plan: ModelBuildPlan)(using ctx: Context): Unit =
    val save   = ModelBuildPlanner.saveStatement(m, plan.graph)
    val config = engineOf(m)
    engineSessionOf(config) match
      case Some(session) =>
        workEnv.info(s"Building ${m.name} on the wvlet engine")
        session.save(save)
      case None =>
        given QueryProgressMonitor = ctx.queryProgressMonitor
        GenSQL
          .generateSaveSQL(save, ctx)
          .foreach { sql =>
            workEnv.info(s"Building ${m.name}:\n${sql}")
            connectorProvider.getConnector(config).execute(sql)
          }

  /** Build a model, retrying failed attempts as its `retries` config allows */
  private def buildWithRetries(m: BuildModel, plan: ModelBuildPlan)(using Context): StageResult =
    val config = StageExecutionConfig.fromConfigItems(m.spec.stageConfig)
    if config.timeoutMillis.isDefined then
      warn(s"timeout of model ${m.name} is not enforced by this wvlet CLI; use the JVM CLI for it")
    var attempt                     = 0
    var result: Option[StageResult] = None
    while result.isEmpty do
      attempt += 1
      try
        buildModel(m, plan)
        result = Some(StageResult(m.name, StageState.Success, attempt, table = Some(m.targetName)))
      catch
        case NonFatal(e) =>
          if attempt <= config.retries then
            val delay = config.retryDelayFor(attempt)
            warn(
              s"Building ${m.name} failed (attempt ${attempt}), retrying in ${delay} ms: ${e
                  .getMessage}"
            )
            Sleep.sleepMillis(delay)
          else
            result = Some(StageResult(m.name, StageState.Failed, attempt, error = Some(e)))
    result.get

  /**
    * Build the selected models of the plan in build order. Fails before building anything when an
    * unselected upstream model is not built yet. A failed model skips the models that read it
    */
  def build(plan: ModelBuildPlan): FlowExecutionResult =
    given ctx: Context = plan.context
    checkSameEngineReads(plan)
    plan.checkUpstreamsBuilt(isBuilt)
    val runId = ULID.newULIDString
    workEnv.info(s"Building ${plan.selected.size} models (run: ${runId})")
    val notBuilt = mutable.Set.empty[String]
    val results  = plan
      .selected
      .map { m =>
        if plan.graph.upstreams(m.name).exists(notBuilt.contains) then
          notBuilt += m.name
          StageResult(m.name, StageState.Skipped, attempts = 0)
        else
          val r = buildWithRetries(m, plan)
          if r.state != StageState.Success then
            notBuilt += m.name
          r
      }
    FlowExecutionResult(ModelBuildPlanner.BuildFlowName, runId, results)

  override def close(): Unit =
    engineSessions.clear()
    connectorProvider.close()

end ModelBuilder
