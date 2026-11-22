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
package wvlet.lang.engine

import wvlet.uni.test.UniTest

/**
  * The window and ordering specs in spec/basic with sorts and windows forced onto their parallel
  * paths must return the same rows (in the same order for ORDER BY) as DuckDB
  */
class EngineParallelSortWindowSpecTest extends UniTest:
  private val minRowsKeys = Seq(
    "wvlet.engine.parallelSortMinRows",
    "wvlet.engine.parallelWindowMinRows"
  )

  minRowsKeys.foreach(k => sys.props(k) = "1")

  private val conformance = EngineConformance("spec/basic", Nil, _ => ())

  override def afterAll: Unit =
    minRowsKeys.foreach(sys.props.remove)
    conformance.close()

  private def covers(fileName: String): Boolean =
    fileName.contains("window") || fileName.contains("order")

  for
    unit <- conformance.units
    if covers(unit.sourceFile.fileName)
  do
    test(unit.sourceFile.fileName) {
      conformance
        .run(unit)
        .foreach { r =>
          debug(s"${r.name}: ${r.message}")
          if !r.matched then
            fail(s"${r.name}: ${r.message}")
        }
    }

end EngineParallelSortWindowSpecTest
