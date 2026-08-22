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
package wvlet.lang.compiler.codegen

import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.WorkEnv

import java.nio.file.Files
import java.nio.file.Paths

/**
  * Regenerates the Wvlet TPC-DS spec queries in `spec/tpcds` from the SQL queries in
  * `spec/sql/tpc-ds`, using the SQL-to-Wvlet converter (the same path as `wvlet to_wvlet`).
  *
  * Run from the repository root with:
  * {{{
  *   ./sbt "langJVM/Test/runMain wvlet.lang.compiler.codegen.TPCDSWvletSpecGenerator"
  * }}}
  */
object TPCDSWvletSpecGenerator:
  def main(args: Array[String]): Unit =
    val srcDir = "spec/sql/tpc-ds"
    val outDir = Paths.get("spec/tpcds")
    Files.createDirectories(outDir)

    val options  = CompilerOptions(sourceFolders = List(srcDir), workEnv = WorkEnv(srcDir))
    val compiler = Compiler.parseOnly(options)
    for unit <- compiler.localCompilationUnits do
      val name          = unit.sourceFile.fileName.stripSuffix(".sql")
      val compileResult = compiler.compileSingleUnit(unit)
      val ctx       = compileResult.context.withCompilationUnit(unit).newContext(Symbol.NoSymbol)
      val generator = WvletGenerator(CodeFormatterConfig(sqlDBType = ctx.dbType))(using ctx)
      val wvlet     = generator.print(unit.resolvedPlan)
      val outFile   = outDir.resolve(s"q${name}.wv")
      Files.writeString(outFile, s"${wvlet}\n")
      println(s"Generated ${outFile}")

end TPCDSWvletSpecGenerator
