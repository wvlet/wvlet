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
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.CompileResult
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.pipeline.BuildModel
import wvlet.lang.compiler.pipeline.ModelBuildPlanner
import wvlet.lang.compiler.pipeline.ModelGraph
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.connector.Connector
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.model.plan.ModelDef
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.control.Control
import wvlet.uni.log.LogSupport

/**
  * `wvlet build`: compiles the models of a work folder and builds the materialized ones in
  * dependency order on the flow executor
  */
object ModelBuildLauncher extends LogSupport:

  /**
    * The models of a work folder, planned for a build
    *
    * @param unit
    *   The compilation unit the build flow runs in
    * @param graph
    *   Dependency graph of the materialized models
    * @param selected
    *   Selected models in build order
    * @param flow
    *   The build flow: one stage per selected model
    */
  case class BuildPlan(
      unit: CompilationUnit,
      compileResult: CompileResult,
      graph: ModelGraph,
      selected: List[BuildModel],
      flow: FlowDef
  ):
    def context: Context = compileResult
      .context
      .withCompilationUnit(unit)
      .newContext(Symbol.NoSymbol)

    /** Materialized models the selection reads but does not build */
    def unselectedUpstreams: List[BuildModel] =
      val names = selected.map(_.name).toSet
      selected
        .flatMap(m => graph.upstreams(m.name))
        .distinct
        .filterNot(names.contains)
        .flatMap(graph.get)

    /** The SQL statements of each selected model, in build order */
    def sqlStatements: List[(BuildModel, List[String])] =
      given ctx: Context = context
      selected.map { m =>
        m -> GenSQL.generateSaveSQL(ModelBuildPlanner.saveStatement(m, graph), ctx)
      }

  end BuildPlan

  /** Compile the work folder and plan a build of the selected models */
  def plan(
      workFolder: String,
      workEnv: WorkEnv,
      selectors: List[String],
      dbType: DBType = DBType.Wvlet
  ): BuildPlan =
    // Compile for the profile's engine, so the generated DDL (e.g. create or replace) fits it
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List(workFolder), workEnv = workEnv, dbType = dbType)
    )
    val compileResult = compiler.compile()
    // Surface compile errors (e.g. an invalid model config) with their own status code
    compileResult.failureReport.toList match
      case Nil           =>
      case (_, e) :: Nil =>
        throw e
      case _ =>
        throw compileResult.failureException
    val models = List.newBuilder[(CompilationUnit, ModelDef)]
    compileResult
      .units
      .foreach { unit =>
        unit
          .resolvedPlan
          .traverse { case m: ModelDef =>
            models += unit -> m
          }
      }
    val allModels = models.result()
    if allModels.isEmpty then
      throw StatusCode.MODEL_NOT_FOUND.newException(s"No model is defined in ${workFolder}")
    val unit           = allModels.head._1
    given ctx: Context = compileResult.context.withCompilationUnit(unit).newContext(Symbol.NoSymbol)
    val graph          = ModelBuildPlanner.graph(allModels.map(_._2))
    if graph.models.isEmpty then
      throw StatusCode
        .MODEL_NOT_FOUND
        .newException(
          "No materialized model is found. Add a config like `model m with { materialized: 'table' } = { ... }`"
        )
    val selected = graph.buildOrder(graph.select(selectors))
    BuildPlan(unit, compileResult, graph, selected, ModelBuildPlanner.toFlow(graph, selected))

  end plan

  /**
    * Fail when the selection reads a materialized model that is neither selected nor readable on
    * its engine (i.e., not built yet)
    */
  def checkUpstreamsBuilt(plan: BuildPlan, connectorFor: BuildModel => Connector): Unit =
    val missing = plan
      .unselectedUpstreams
      .filterNot(m => FlowStageEngine.of(connectorFor(m)).tableExists(m.targetName))
    if missing.nonEmpty then
      throw StatusCode
        .MODEL_NOT_FOUND
        .newException(
          s"Upstream models are not built yet: ${missing
              .map(_.name)
              .mkString(", ")}. Build them first, or add their upstream with +<model>"
        )

  /** Execute a build plan, after checking that unselected upstream models are built */
  def execute(
      plan: BuildPlan,
      profile: Profile,
      workEnv: WorkEnv,
      store: FlowRunStore
  ): FlowExecutionResult =
    Control.withResource(ConnectorProvider(workEnv)) { provider =>
      // Probe each upstream on the engine it is built on
      checkUpstreamsBuilt(
        plan,
        m =>
          m.spec
            .engine
            .flatMap(e => profile.connectors.find(_.name == e))
            .map(provider.getConnector)
            .filter(FlowStageEngine.canRunStages)
            .getOrElse(provider.getConnector(profile.defaultEngine))
      )
    }
    FlowRunLauncher.executeFlowDef(
      plan.flow,
      plan.unit,
      plan.compileResult,
      profile,
      workEnv,
      store
    )

end ModelBuildLauncher
