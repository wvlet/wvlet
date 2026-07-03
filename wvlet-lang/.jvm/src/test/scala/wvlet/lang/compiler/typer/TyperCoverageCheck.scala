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

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.{LogicalPlan, Relation}
import wvlet.lang.model.expr.{Expression, Identifier}

import java.io.File
import scala.collection.mutable

/**
  * Coverage gate for the Typer (issue #392): compile every spec under spec/basic and measure how
  * many plan nodes and expressions end up with a resolved type.
  *
  * The bounds below pin the current typing coverage. Improvements are accepted automatically;
  * regressions fail this test. Ratchet the bounds upward as typing coverage improves toward 100%.
  */
class TyperCoverageCheck extends UniTest:

  private case class Coverage(
      relationTotal: Int,
      relationResolved: Int,
      exprTotal: Int,
      exprTyped: Int,
      exprTpeSet: Int,
      unresolvedRelations: Map[String, Int],
      untypedExprs: Map[String, Int]
  ):
    def relationCoverage: Double = relationResolved.toDouble / relationTotal.max(1)
    def exprCoverage: Double     = exprTyped.toDouble / exprTotal.max(1)

  // An expression counts as typed when it carries any resolved type: a DataType for value
  // expressions, or e.g. a FunctionType for references to functions
  private def isTypedExpr(e: Expression): Boolean = e.dataType.isResolved || e.tpe.isResolved

  private def measure(plans: Seq[LogicalPlan]): Coverage =
    var relationTotal    = 0
    var relationResolved = 0
    var exprTotal        = 0
    var exprTyped        = 0
    var exprTpeSet       = 0
    val unresolvedRel    = mutable.Map.empty[String, Int].withDefaultValue(0)
    val untypedExpr      = mutable.Map.empty[String, Int].withDefaultValue(0)

    plans.foreach { plan =>
      plan.traverse { case r: Relation =>
        relationTotal += 1
        if r.relationType.isResolved then
          relationResolved += 1
        else
          unresolvedRel(r.nodeName) += 1
      }
      plan.traverseExpressions {
        case i: Identifier if i.isEmpty =>
        // An empty name is a structural placeholder (e.g. an unnamed column slot), not an
        // expression that could carry a type
        case e: Expression =>
          exprTotal += 1
          if e.isTyped then
            exprTpeSet += 1
          if isTypedExpr(e) then
            exprTyped += 1
          else
            untypedExpr(e.nodeName) += 1
      }
    }
    Coverage(
      relationTotal,
      relationResolved,
      exprTotal,
      exprTyped,
      exprTpeSet,
      unresolvedRel.toMap,
      untypedExpr.toMap
    )

  end measure

  test("typing coverage over spec/basic must not regress") {
    val specDir = File("spec/basic")
    val wvFiles = Option(specDir.listFiles())
      .getOrElse(Array.empty[File])
      .filter(f => f.isFile && f.getName.endsWith(".wv"))
      .sortBy(_.getName)

    val resolvedPlans = List.newBuilder[LogicalPlan]
    var compiled      = 0
    var needsCatalog  = 0
    wvFiles.foreach { f =>
      try
        // A fresh Compiler per file is deliberate: spec files share one global namespace, so
        // reusing a compiler would make symbol resolution depend on the compilation order of
        // unrelated files
        val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(specDir.getPath)))
        val unit     = CompilationUnit.fromFile(f.getPath)
        compiler.compileSingleUnit(unit)
        resolvedPlans += unit.resolvedPlan
        compiled += 1
      catch
        case e: Throwable =>
          // Specs requiring a catalog (DB connection) cannot be compiled standalone; they are
          // covered by the runner-side spec tests
          needsCatalog += 1
    }

    val c = measure(resolvedPlans.result())
    info(f"compiled ${compiled}/${wvFiles.length} specs (${needsCatalog} need a catalog)")
    info(
      f"relations resolved: ${c.relationResolved}/${c.relationTotal} (${c.relationCoverage *
          100}%.1f%%)"
    )
    info(f"expressions typed:  ${c.exprTyped}/${c.exprTotal} (${c.exprCoverage * 100}%.1f%%)")
    info(f"expressions with tpe set: ${c.exprTpeSet}/${c.exprTotal}")

    def top(m: Map[String, Int], n: Int): String = m
      .toSeq
      .sortBy(-_._2)
      .take(n)
      .map { case (k, v) =>
        s"${k}=${v}"
      }
      .mkString(", ")
    info(s"top unresolved relations: ${top(c.unresolvedRelations, 10)}")
    info(s"top untyped expressions:  ${top(c.untypedExprs, 10)}")

    // Ratchet (2026-07-03: 90.7% / 87.0%). Raise these as coverage improves toward 1.0
    val minRelationCoverage = 0.89
    val minExprCoverage     = 0.86
    if c.relationCoverage < minRelationCoverage then
      fail(
        f"Relation type coverage regressed: ${c.relationCoverage *
            100}%.1f%% (expected >= ${minRelationCoverage * 100}%.0f%%)"
      )
    if c.exprCoverage < minExprCoverage then
      fail(
        f"Expression type coverage regressed: ${c.exprCoverage *
            100}%.1f%% (expected >= ${minExprCoverage * 100}%.0f%%)"
      )
  }

end TyperCoverageCheck
