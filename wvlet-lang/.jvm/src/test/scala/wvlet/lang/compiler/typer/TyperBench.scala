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

import java.io.File

/**
  * A lightweight timing probe for compiler performance work (issue #392): compiles the spec/basic
  * corpus repeatedly after warmup and logs the median wall time. No assertion — the numbers are for
  * comparing before/after within a single machine and JVM session.
  */
class TyperBench extends UniTest:

  test("time spec/basic corpus compilation") {
    val specDir = File("spec/basic")
    val wvFiles = Option(specDir.listFiles())
      .getOrElse(Array.empty[File])
      .filter(f => f.isFile && f.getName.endsWith(".wv"))
      .sortBy(_.getName)

    def compileAll(): Unit = wvFiles.foreach { f =>
      try
        val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(specDir.getPath)))
        val unit     = CompilationUnit.fromFile(f.getPath)
        compiler.compileSingleUnit(unit)
      catch
        case _: Throwable =>
          ()
    }

    // Warm up the JIT before measuring
    (1 to 5).foreach(_ => compileAll())
    val times = (1 to 10).map { _ =>
      val t0 = System.nanoTime()
      compileAll()
      (System.nanoTime() - t0) / 1000000.0
    }
    info(
      f"corpus compile: median=${times.sorted.apply(times.size / 2)}%.1f ms min=${times
          .min}%.1f ms max=${times.max}%.1f ms"
    )
  }

end TyperBench
