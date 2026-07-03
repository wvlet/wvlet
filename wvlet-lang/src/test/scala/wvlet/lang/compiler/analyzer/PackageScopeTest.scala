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

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.{CompilationUnit, Compiler, CompilerOptions, WorkEnv}
import wvlet.lang.model.plan.Relation

/**
  * Tests for package-level visibility of top-level definitions (issue #93): units in a named
  * package are visible only within that package or through an import, while units without a package
  * declaration stay globally visible
  */
class PackageScopeTest extends UniTest:

  private def compileRef(defs: List[String], ref: String): CompilationUnit =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val refUnit  = CompilationUnit.fromWvletString(ref)
    compiler.compileMultipleUnits(defs.map(CompilationUnit.fromWvletString), contextUnit = refUnit)
    refUnit

  private def resolvedRelation(unit: CompilationUnit): Boolean =
    var resolved = false
    unit
      .resolvedPlan
      .traverse {
        case r: Relation if r.relationType.isResolved =>
          resolved = true
      }
    resolved

  test("share definitions between units of the same package") {
    val unit = compileRef(
      List("package scope_a\nmodel pkg_m1 = { from [[1]] as t(id) }"),
      "package scope_a\nfrom pkg_m1"
    )
    resolvedRelation(unit) shouldBe true
  }

  test("hide a named package's definitions from other packages") {
    val unit = compileRef(
      List("package scope_b\nmodel pkg_m2 = { from [[1]] as t(id) }"),
      "package scope_other\nfrom pkg_m2"
    )
    resolvedRelation(unit) shouldBe false
  }

  test("make a named package's definitions visible through an import") {
    val unit = compileRef(
      List("package scope_c\nmodel pkg_m3 = { from [[1]] as t(id) }"),
      // the val between the import and the query avoids the import's optional
      // `from "source"` clause consuming the query's from keyword
      "import scope_c.*\nval one = 1\nfrom pkg_m3"
    )
    resolvedRelation(unit) shouldBe true
  }

  test("keep units without a package declaration globally visible") {
    val unit = compileRef(
      List("model pkg_m4 = { from [[1]] as t(id) }"),
      "package scope_d\nfrom pkg_m4"
    )
    resolvedRelation(unit) shouldBe true
  }

end PackageScopeTest
