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
package wvlet.lang.compiler.typer

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.PackageDef
import wvlet.lang.model.plan.Relation
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.log.LogSupport

/**
  * Validation tests that compare TypeResolver and Typer outputs. These tests help identify gaps
  * between the old TypeResolver and the new Typer implementation.
  */
class TyperValidationTest extends AirSpec with LogSupport:

  private def compileWithTypeResolver(wvletSource: String): CompilationUnit =
    val options  = CompilerOptions(workEnv = WorkEnv("."))
    val compiler = Compiler(options)
    val unit     = CompilationUnit.fromWvletString(wvletSource)
    compiler.compileSingleUnit(unit)
    unit

  private def compileWithTyper(wvletSource: String): CompilationUnit =
    val options  = CompilerOptions(workEnv = WorkEnv("."))
    val compiler = Compiler.withNewTyper(options)
    val unit     = CompilationUnit.fromWvletString(wvletSource)
    compiler.compileSingleUnit(unit)
    unit

  /**
    * Compare resolved plans from TypeResolver and Typer. Returns (matches, differences) tuple.
    */
  private def compareTypers(wvletSource: String): TyperComparisonResult =
    val unitOld = compileWithTypeResolver(wvletSource)
    val unitNew = compileWithTyper(wvletSource)

    val resolvedOld = unitOld.resolvedPlan
    val resolvedNew = unitNew.resolvedPlan

    // Collect types from both plans
    val typesOld = collectTypes(resolvedOld, "")
    val typesNew = collectTypes(resolvedNew, "")

    // Compare using partitionMap for a more functional approach
    val allPaths               = (typesOld.keySet ++ typesNew.keySet).toList
    val (differences, matches) = allPaths.partitionMap { path =>
      (typesOld.get(path), typesNew.get(path)) match
        case (Some(tOld), Some(tNew)) if tOld == tNew =>
          Right(path)
        case (tOld, tNew) =>
          Left(TypeDifference(path, tOld.getOrElse("missing"), tNew.getOrElse("missing")))
    }

    TyperComparisonResult(matches.size, differences)

  /**
    * Collect path -> type string mappings from a plan
    */
  private def collectTypes(plan: LogicalPlan, prefix: String): Map[String, String] =
    val builder = Map.newBuilder[String, String]
    val path    =
      if prefix.isEmpty then
        plan.getClass.getSimpleName
      else
        s"${prefix}/${plan.getClass.getSimpleName}"

    // Record this node's type
    val tpeStr =
      plan.tpe match
        case NoType =>
          "NoType"
        case t =>
          t.toString.take(50) // Truncate long type strings
    builder += (path -> tpeStr)

    // Recursively collect from children using zipWithIndex
    plan
      .children
      .zipWithIndex
      .foreach { case (child, childIndex) =>
        val childPrefix = s"${path}[${childIndex}]"
        builder ++= collectTypes(child, childPrefix)
      }

    // Collect from expressions in relations
    plan match
      case r: Relation =>
        r.childExpressions
          .zipWithIndex
          .foreach { case (expr, exprIndex) =>
            val exprPrefix = s"${path}/expr[${exprIndex}]"
            builder ++= collectExprTypes(expr, exprPrefix)
          }
      case _ =>

    builder.result()

  end collectTypes

  private def collectExprTypes(expr: Expression, prefix: String): Map[String, String] =
    val builder = Map.newBuilder[String, String]
    val path    = s"${prefix}/${expr.getClass.getSimpleName}"

    val tpeStr =
      expr.tpe match
        case NoType =>
          "NoType"
        case t =>
          t.toString.take(50)
    builder += (path -> tpeStr)

    // Recursively collect from child expressions using zipWithIndex
    expr
      .children
      .zipWithIndex
      .foreach { case (child, childIndex) =>
        val childPrefix = s"${path}[${childIndex}]"
        builder ++= collectExprTypes(child, childPrefix)
      }

    builder.result()

  // ============================================
  // Validation Tests - Document current gaps
  // ============================================

  test("should produce resolvedPlan for simple VALUES query"):
    val source  = "from values(1, 2, 3) as t(x)"
    val unitOld = compileWithTypeResolver(source)
    val unitNew = compileWithTyper(source)

    // Both should have non-empty resolved plans
    unitOld.resolvedPlan shouldNotBe LogicalPlan.empty
    unitNew.resolvedPlan shouldNotBe LogicalPlan.empty

  test("should type literal expressions similarly"):
    val result = compareTypers("from values(42, 3.14, 'hello', true) as t(a, b, c, d)")

    debug(s"Matches: ${result.matches}, Differences: ${result.differences.size}")
    result
      .differences
      .foreach { diff =>
        debug(s"  ${diff.path}: TypeResolver=${diff.oldType}, Typer=${diff.newType}")
      }

    // Report for documentation - we expect some differences as Typer is incomplete
    (result.matches >= 1) shouldBe true

  test("should type arithmetic expressions"):
    val result = compareTypers("""
      from values(1, 2) as t(x, y)
      select x + y as sum, x * y as product
    """)

    debug(s"Arithmetic: Matches=${result.matches}, Differences=${result.differences.size}")
    result
      .differences
      .take(5)
      .foreach { diff =>
        debug(s"  ${diff.path}: ${diff.oldType} vs ${diff.newType}")
      }

    // Ensure there's at least one match to confirm the comparison is working
    (result.matches >= 1) shouldBe true

  test("should type comparison expressions"):
    val result = compareTypers("""
      from values(1, 2) as t(x, y)
      where x > y
    """)

    debug(s"Comparison: Matches=${result.matches}, Differences=${result.differences.size}")

    // Ensure there's at least one match to confirm the comparison is working
    (result.matches >= 1) shouldBe true

  test("should type CASE expressions"):
    // Note: Wvlet doesn't use 'end' keyword for case expressions
    val result = compareTypers("""
      from values(1) as t(x)
      select if x > 0 then 'positive' else 'negative' as sign
    """)

    debug(s"CASE: Matches=${result.matches}, Differences=${result.differences.size}")

    // Ensure there's at least one match to confirm the comparison is working
    (result.matches >= 1) shouldBe true

  // Known gaps - these tests document what TypeResolver handles but Typer doesn't yet

  test("gap: table reference resolution (TypeResolver only)"):
    // This test documents a known gap - table references aren't resolved by Typer yet
    val source   = "from sample_data"
    val unitNew  = compileWithTyper(source)
    val resolved = unitNew.resolvedPlan

    // Typer stores the plan but table refs aren't fully resolved
    resolved shouldNotBe LogicalPlan.empty

end TyperValidationTest

case class TyperComparisonResult(matches: Int, differences: List[TypeDifference])
case class TypeDifference(path: String, oldType: String, newType: String)
