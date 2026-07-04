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
package wvlet.lang.compiler

import wvlet.lang.api.Span.NoSpan
import wvlet.uni.test.UniTest

class GlobalSymbolIndexTest extends UniTest:

  private def newUnit(fileName: String): CompilationUnit = CompilationUnit(
    SourceFile.fromString(fileName, "")
  )

  private def newSymbol(id: Int, name: String): Symbol =
    val sym = Symbol(id, NoSpan)
    // Install a completer so that the name is recorded eagerly without computing a SymbolInfo
    sym.setCompleter(Name.termName(name), _ => NoSymbolInfo)
    sym

  test("order definitions of a name by the defining file name") {
    val index = GlobalSymbolIndex()
    val unitB = newUnit("b.wv")
    val unitA = newUnit("a.wv")
    val symB  = newSymbol(1, "m1")
    val symA  = newSymbol(2, "m1")
    index.add(unitB, symB)
    index.add(unitA, symA)

    val entries = index.visibleEntries(Name.termName("m1"), currentPackage = "", importRefs = Nil)
    entries.map(_.fileName) shouldBe List("a.wv", "b.wv")
    entries.head.symbol shouldBe symA
  }

  test("prefer the most recent definition within the same unit") {
    val index = GlobalSymbolIndex()
    val unit  = newUnit("a.wv")
    val sym1  = newSymbol(1, "m1")
    val sym2  = newSymbol(2, "m1")
    index.add(unit, sym1)
    index.add(unit, sym2)

    val entries = index.visibleEntries(Name.termName("m1"), currentPackage = "", importRefs = Nil)
    entries.map(_.symbol) shouldBe List(sym2, sym1)
  }

  test("skip symbols without a name") {
    val index   = GlobalSymbolIndex()
    val unit    = newUnit("a.wv")
    val unnamed = Symbol(1, NoSpan)
    index.add(unit, unnamed)
    index.findFirstAnywhere(Name.NoName) shouldBe None
  }

  test("keep preset symbols visible from every package") {
    val index      = GlobalSymbolIndex()
    val presetUnit = CompilationUnit(SourceFile.fromString("stdlib.wv", ""), isPreset = true)
    val sym        = newSymbol(1, "count_all")
    index.add(presetUnit, sym)

    index
      .visibleEntries(Name.termName("count_all"), currentPackage = "mypkg", importRefs = Nil)
      .map(_.symbol) shouldBe List(sym)
  }

  test("remove all symbols of a reloaded unit") {
    val index = GlobalSymbolIndex()
    val unitA = newUnit("a.wv")
    val unitB = newUnit("b.wv")
    index.add(unitA, newSymbol(1, "m1"))
    index.add(unitA, newSymbol(2, "m2"))
    index.add(unitB, newSymbol(3, "m1"))

    index.remove(unitA)

    index
      .visibleEntries(Name.termName("m1"), currentPackage = "", importRefs = Nil)
      .map(_.fileName) shouldBe List("b.wv")
    index.visibleEntries(Name.termName("m2"), currentPackage = "", importRefs = Nil) shouldBe Nil
  }

  test("replay the known symbols of an already-labeled unit in their entry order") {
    val index = GlobalSymbolIndex()
    val unit  = newUnit("a.wv")
    val sym1  = newSymbol(1, "m1")
    val sym2  = newSymbol(2, "m1")
    unit.enter(sym1)
    unit.enter(sym2)

    index.addAll(unit)

    val entries = index.visibleEntries(Name.termName("m1"), currentPackage = "", importRefs = Nil)
    // knownSymbols is newest-first, and the replay must preserve that precedence
    entries.map(_.symbol) shouldBe List(sym2, sym1)
  }

  test("resolve names across packages only through the package or a member import") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))

    val pkgUnit = CompilationUnit(
      SourceFile.fromString(
        "pkg_models.wv",
        """package mypkg1
          |model pm1 = {
          |  from [[1]] as t(id) select id
          |}
          |""".stripMargin
      )
    )
    val importingUnit = CompilationUnit(
      SourceFile.fromString(
        "importing.wv",
        """import mypkg1.*
          |
          |model imported_ref = {
          |  from pm1
          |}
          |""".stripMargin
      )
    )
    val queryUnit = CompilationUnit.fromWvletString("from imported_ref\n")

    val result = compiler.compileMultipleUnits(List(pkgUnit, importingUnit), queryUnit)
    result.hasFailures shouldBe false

    // The importing unit compiles because pm1 is visible through the import of mypkg1
    val importedRef = result.context.findSymbolByName(Name.termName("imported_ref"))
    importedRef.isDefined shouldBe true

    // The root context has no imports and belongs to the default package, so the packaged
    // model must stay invisible from it
    result.context.findSymbolByName(Name.termName("pm1")) shouldBe None

    // Package visibility rules of the index itself
    val index = result.context.global.symbolIndex
    val pm1   = Name.termName("pm1")
    index.visibleEntries(pm1, currentPackage = "", importRefs = Nil) shouldBe Nil
    index.visibleEntries(pm1, currentPackage = "mypkg1", importRefs = Nil).size shouldBe 1
    // An import of the package or of one of its members makes the package visible
    index.visibleEntries(pm1, currentPackage = "", importRefs = List("mypkg1")).size shouldBe 1
    index.visibleEntries(pm1, currentPackage = "", importRefs = List("mypkg1.pm1")).size shouldBe 1
    index.visibleEntries(pm1, currentPackage = "", importRefs = List("mypkg2")) shouldBe Nil
    index.findFirstAnywhere(pm1).map(_.fileName) shouldBe Some("pkg_models.wv")
  }

  test("warn once for a top-level name defined in multiple files") {
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))

    val unitA = CompilationUnit(
      SourceFile.fromString("dup_a.wv", "model dup_m = {\n  from [[1]] as t(id) select id\n}\n")
    )
    val unitB = CompilationUnit(
      SourceFile.fromString("dup_b.wv", "model dup_m = {\n  from [[2]] as t(id) select id\n}\n")
    )
    val queryUnit = CompilationUnit.fromWvletString("from dup_m\n")

    val result = compiler.compileMultipleUnits(List(unitB, unitA), queryUnit)
    result.hasFailures shouldBe false

    // The definition in the first file (by file name) wins regardless of the compilation order
    val sym = result.context.findSymbolByName(Name.termName("dup_m"))
    sym.map(_.symbolInfo.compilationUnit.sourceFile.fileName) shouldBe Some("dup_a.wv")

    result
      .context
      .global
      .duplicateDefinitions
      .find(_._1 == Name.termName("dup_m"))
      .map(_._2) shouldBe Some(List("dup_a.wv", "dup_b.wv"))
  }

end GlobalSymbolIndexTest
