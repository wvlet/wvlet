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
package wvlet.lang.cli

import wvlet.uni.cli.launcher.argument
import wvlet.uni.cli.launcher.command
import wvlet.uni.cli.launcher.option
import wvlet.uni.control.Control
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Profile
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.CompileResult
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.DependsOnFlow
import wvlet.lang.model.plan.FlowDef
import wvlet.lang.model.plan.FlowStatePredicate
import wvlet.lang.runner.FlowExecutor
import wvlet.lang.runner.connector.DBConnectorProvider
import wvlet.uni.log.LogSupport

case class WvletFlowOption(
    @option(prefix = "-w", description = "Working folder containing .wv files")
    workFolder: String = ".",
    @option(prefix = "--profile", description = "Profile to use")
    profile: Option[String] = None,
    @option(prefix = "--catalog", description = "Context database catalog to use")
    catalog: Option[String] = None,
    @option(prefix = "--schema", description = "Context database schema to use")
    schema: Option[String] = None
)

/**
  * `wvlet flow` subcommands for managing and running flows from the CLI
  */
class WvletFlowCommand(opts: WvletGlobalOption) extends LogSupport:

  @command(description = "Run a flow and print its stage results", isDefault = true)
  def help: Unit = info("Usage: wvlet flow <run|list|show> ...")

  @command(description = "Run a flow defined in the working folder")
  def run(
      flowOption: WvletFlowOption,
      @argument(description = "Name of the flow to run")
      name: String
  ): Unit =
    withFlows(flowOption) { (flows, compileResult, workEnv) =>
      val (unit, flow) = findFlow(flows, name)
      val profile = Profile.getProfile(flowOption.profile, flowOption.catalog, flowOption.schema)
      Control.withResource(DBConnectorProvider(workEnv)) { dbConnectorProvider =>
        val connector = dbConnectorProvider.getConnector(profile)

        given ctx: Context = compileResult
          .context
          .withCompilationUnit(unit)
          .newContext(Symbol.NoSymbol)

        val result = FlowExecutor(connector, workEnv).execute(flow)
        println(result.toPrettyBox())
        if result.hasError && !WvletMain.isInSbt then
          System.exit(1)
      }
    }

  @command(description = "List flows defined in the working folder")
  def list(flowOption: WvletFlowOption): Unit =
    withFlows(flowOption) { (flows, _, _) =>
      flows.foreach { (unit, f) =>
        val params =
          if f.params.isEmpty then
            ""
          else
            s"(${f.params.map(p => s"${p.name.name}").mkString(", ")})"
        val dependency =
          f.dependency match
            case Some(DependsOnFlow(flowName, _)) =>
              s" depends on ${flowName.fullName}"
            case Some(FlowStatePredicate(flowName, stateName, _)) =>
              s" if ${flowName.fullName}.${stateName}"
            case None =>
              ""
        val schedule = f
          .config
          .find(_.key.unquotedValue == "schedule")
          .map(c => s" [schedule: ${c.value}]")
          .getOrElse("")
        println(
          f"${f.name.name}%-30s ${f.stages.size}%2d stages${params}${dependency}${schedule} - ${unit
              .sourceFile
              .relativeFilePath}"
        )
      }
    }

  @command(description = "Show the plan of a flow")
  def show(
      flowOption: WvletFlowOption,
      @argument(description = "Name of the flow to show")
      name: String
  ): Unit =
    withFlows(flowOption) { (flows, compileResult, _) =>
      val (unit, flow) = findFlow(flows, name)

      given Context = compileResult.context.withCompilationUnit(unit)

      println(flow.pp)
    }

  /**
    * Compile all .wv files in the working folder and collect flow definitions with their defining
    * units
    */
  private def withFlows[A](flowOption: WvletFlowOption)(
      body: (List[(CompilationUnit, FlowDef)], CompileResult, WorkEnv) => A
  ): A =
    val workEnv  = WorkEnv(flowOption.workFolder, opts.logLevel)
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List(flowOption.workFolder), workEnv = workEnv)
    )
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
    body(flows.result(), compileResult, workEnv)

  private def findFlow(
      flows: List[(CompilationUnit, FlowDef)],
      name: String
  ): (CompilationUnit, FlowDef) = flows
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

end WvletFlowCommand
