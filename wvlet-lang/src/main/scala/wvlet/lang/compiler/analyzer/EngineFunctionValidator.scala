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
package wvlet.lang.compiler.analyzer

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.catalog.StaticCatalogExporter
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Phase
import wvlet.lang.compiler.WvletEngineFunctionNames
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Reject function calls that the target engine does not bind, at compile time. Only targets that
  * know their full function list check names ([[wvlet.lang.compiler.DBType.checksFunctionNames]],
  * today the wvlet engine); other engines receive unknown names as SQL.
  *
  * A call is accepted when it resolves to a definition for the target dialect (a stdlib or user
  * def, see [[FunctionInliner.findFunctionDef]]) or names a function the engine binds
  * ([[WvletEngineFunctionNames]]). The check runs on the query being compiled (the context unit),
  * after the Typer has inlined stdlib calls; bodies of defs are checked where they are inlined. Raw
  * SQL (`sql"..."`) is not parsed by the compiler and keeps the engine's run-time check.
  */
object EngineFunctionValidator extends Phase("engine-function-validator"):

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    val isTarget = context.global.getContextUnit.forall(_ eq unit)
    if context.dbType.checksFunctionNames && !unit.isPreset && isTarget then
      validate(unit.resolvedPlan)(using context)
    unit

  private def validate(plan: LogicalPlan)(using context: Context): Unit = plan.traverse {
    case _: TypeDef | _: TopLevelFunctionDef =>
    // Definitions are checked where they are inlined
    case p: LogicalPlan =>
      p.childExpressions
        .foreach { e =>
          e.traverseExpressions { case f: FunctionApply =>
            checkCall(f)
          }
        }
  }

  private def checkCall(f: FunctionApply)(using context: Context): Unit =
    val names: List[String] =
      f.base match
        case id: Identifier if id.nonEmpty =>
          List(id.fullName)
        case d: DotRef =>
          // A method-style call x.fn(...), or a function with a dotted name
          d.qualifier match
            case q: NameExpr if q.nonEmpty =>
              List(d.name.leafName, s"${q.fullName}.${d.name.leafName}")
            case _ =>
              List(d.name.leafName)
        case _ =>
          Nil
    if names.nonEmpty && !names.exists(isEngineFunction) && !resolvesToDef(f) then
      val name = names.head
      val hint =
        stdlibAlternative(name) match
          case Some(alt) =>
            s"Use the standard library function ${alt} instead"
          case None =>
            "Use a standard library function, or run the query on the DuckDB connector " +
              "(a profile with type: duckdb)"
      throw StatusCode
        .FUNCTION_NOT_FOUND
        .newException(
          s"Function ${name} is not supported by the wvlet engine. ${hint}",
          context.sourceLocationAt(f.span)
        )

  private def isEngineFunction(name: String): Boolean = WvletEngineFunctionNames
    .names
    .contains(name.toLowerCase)

  private def resolvesToDef(f: FunctionApply)(using context: Context): Boolean =
    try
      FunctionInliner.findFunctionDef(f).isDefined
    catch
      case _: WvletLangException =>
        // An ambiguous definition is reported by the inliner, not here
        true

  /** The stdlib function that expands to the given DuckDB function name, if any */
  private[analyzer] def stdlibAlternative(name: String): Option[String] = stdlibAlternatives.get(
    name.toLowerCase
  )

  /**
    * DuckDB function name -> the stdlib function that expands to it on DuckDB, e.g. regexp_matches
    * -> string.regexp_like. Read from the hand-written stdlib sources: a def whose `in duckdb` (or
    * dialect-neutral) body is `sql"name(..."`
    */
  private lazy val stdlibAlternatives: Map[String, String] =
    val m = List.newBuilder[(String, String)]
    def visit(owner: Option[String], ownerContexts: List[DefContext], fd: FunctionDef): Unit =
      val contexts = (ownerContexts ++ fd.defContexts).map(_.contextType.leafName.toLowerCase)
      if contexts.isEmpty || contexts.exists(c => c == "duckdb" || c == "wvlet") then
        fd.expr match
          case Some(s: InterpolatedString) if s.prefix.fullName == "sql" =>
            s.parts.headOption match
              case Some(p: StringPart) =>
                callee
                  .findPrefixMatchOf(p.value)
                  .foreach { mt =>
                    val stdName = fd.name.name
                    m += mt.group(1).toLowerCase ->
                      owner.map(o => s"${o}.${stdName}").getOrElse(stdName)
                  }
              case _ =>
            end match
          case _ =>
    CompilationUnit
      .stdLib
      .filterNot(
        _.sourceFile.getContentAsString.contains(StaticCatalogExporter.generatedFileHeader)
      )
      .foreach { unit =>
        ParserPhase.parseOnly(unit) match
          case p: PackageDef =>
            p.statements
              .foreach {
                case t: TypeDef =>
                  t.elems
                    .foreach {
                      case fd: FunctionDef =>
                        visit(Some(t.name.name), t.defContexts, fd)
                      case _ =>
                    }
                case t: TopLevelFunctionDef =>
                  visit(None, Nil, t.functionDef)
                case _ =>
              }
          case _ =>
      }
    // When several stdlib functions expand to the same name, name the alphabetically first one so
    // that the message is stable
    m.result().groupBy(_._1).map((k, vs) => k -> vs.map(_._2).min)

  end stdlibAlternatives

  private val callee = """\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(""".r

end EngineFunctionValidator
