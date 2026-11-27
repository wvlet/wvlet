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

import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.pipeline.BuildModel
import wvlet.lang.connector.Connector
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.control.Control
import wvlet.uni.log.LogSupport

/**
  * `wvlet build`: compiles the models of a work folder and builds the materialized ones in
  * dependency order on the flow executor
  */
object ModelBuildLauncher extends LogSupport:

  /** The models of a work folder, planned for a build */
  type BuildPlan = ModelBuildPlan

  /** Compile the work folder and plan a build of the selected models */
  def plan(
      workFolder: String,
      workEnv: WorkEnv,
      selectors: List[String],
      dbType: DBType
  ): BuildPlan = ModelBuildPlan.plan(workFolder, workEnv, selectors, dbType)

  /**
    * Fail when the selection reads a materialized model that is neither selected nor readable on
    * its engine (i.e., not built yet)
    */
  def checkUpstreamsBuilt(plan: BuildPlan, connectorFor: BuildModel => Connector): Unit = plan
    .checkUpstreamsBuilt(m => FlowStageEngine.of(connectorFor(m)).tableExists(m.targetName))

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
