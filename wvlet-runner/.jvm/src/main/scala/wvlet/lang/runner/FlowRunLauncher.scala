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
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.expr.FunctionArg
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.runner.connector.ConnectorProvider
import wvlet.uni.control.Control
import wvlet.uni.log.LogSupport

/**
  * Compiles the flows of a work folder and executes (or resumes) them with profile-selected
  * connectors. Shared by the `wvlet flow` CLI and the server's flow mutation API so both launch
  * runs through the same path
  */
object FlowRunLauncher extends LogSupport:

  /** Flow definitions compiled from the .wv sources of a work folder, with their defining units */
  case class LoadedFlows(flows: List[(CompilationUnit, FlowDef)], compileResult: CompileResult):
    /** The flow with the given name, or a FLOW_NOT_FOUND error listing the available flows */
    def find(name: String): (CompilationUnit, FlowDef) = flows
      .find(_._2.name.name == name)
      .getOrElse(
        throw StatusCode
          .FLOW_NOT_FOUND
          .newException(
            s"Flow '${name}' is not found. Available flows: ${flows
                .map(_._2.name.name)
                .mkString(", ")}"
          )
      )

  /** Compile all .wv files in the work folder and collect flow definitions */
  def loadFlows(workFolder: String, workEnv: WorkEnv): LoadedFlows =
    val compiler = Compiler(CompilerOptions(sourceFolders = List(workFolder), workEnv = workEnv))
    val compileResult = compiler.compile()
    val flows         = List.newBuilder[(CompilationUnit, FlowDef)]
    compileResult
      .units
      .foreach { unit =>
        unit
          .resolvedPlan
          .traverse { case f: FlowDef =>
            flows += unit -> f
          }
      }
    LoadedFlows(flows.result(), compileResult)

  /**
    * Execute (or resume) a compiled flow. Connectors are resolved through the profile, and run
    * records land in the given store (owned by the caller)
    */
  def execute(
      loaded: LoadedFlows,
      flowName: String,
      profile: Profile,
      workEnv: WorkEnv,
      store: FlowRunStore,
      resumeFrom: Option[FlowRunRecord] = None,
      args: List[FunctionArg] = Nil
  ): FlowExecutionResult =
    val (unit, flow) = loaded.find(flowName)
    Control.withResource(ConnectorProvider(workEnv)) { dbConnectorProvider =>
      val connector = dbConnectorProvider.getConnector(profile)

      given ctx: Context = loaded
        .compileResult
        .context
        .withCompilationUnit(unit)
        .newContext(Symbol.NoSymbol)

      FlowExecutor(
        connector,
        workEnv,
        registry = Some(store),
        engineResolver = Some(name =>
          profile
            .connectors
            .find(_.name == name)
            .map(dbConnectorProvider.getConnector)
            .getOrElse(
              throw StatusCode
                .INVALID_ARGUMENT
                .newException(s"Connector '${name}' is not defined in the profile")
            )
        ),
        defaultEngineName = profile.defaultEngine.name,
        activationSinks =
          FlowExecutor.defaultActivationSinks ++
            profile
              .connectors
              .map(c => ConnectorActivationSink(c.name, () => dbConnectorProvider.getConnector(c)))
      ).execute(flow, resumeFrom, args)
    }

  end execute

end FlowRunLauncher
