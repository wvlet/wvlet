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

/** Reading flow-style EXPLAIN output (see `Operator.explain`) in tests */
object PlanText:
  private def indentOf(line: String): Int = line.takeWhile(_ == ' ').length

  /** The operator name and details of a line, without the flow marker and the output columns */
  private def opOf(line: String): String =
    line.trim.stripPrefix("↓ ").stripPrefix("↙ ").split(" -> \\[").head

  private def hasMarker(line: String): Boolean =
    val t = line.trim
    t.startsWith("↓ ") || t.startsWith("↙ ")

  /**
    * The operators the output of the first operator starting with `op` flows into, nearest first,
    * up to the root: the following marked lines at the same indentation (skipping the inputs nested
    * between them), then the reader one level up when the flow is itself a nested input
    */
  def downstream(plan: String, op: String): Seq[String] =
    val lines = plan.linesIterator.toIndexedSeq
    lines.indexWhere(l => opOf(l).startsWith(op)) match
      case -1 =>
        Nil
      case i =>
        val out      = Seq.newBuilder[String]
        var indent   = indentOf(lines(i))
        var skipping = false
        lines
          .drop(i + 1)
          .foreach { l =>
            val d = indentOf(l)
            if d < indent then
              // The reader of this nested input
              out += opOf(l)
              indent = d
              skipping = false
            else if d == indent && !skipping then
              if hasMarker(l) then
                out += opOf(l)
              else
                // Another input of the same reader starts: skip it up to the reader
                skipping = true
          }
        out.result()

end PlanText
