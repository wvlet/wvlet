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

  /**
    * The operators the output of the first operator starting with `op` flows into, nearest first:
    * the following lines at the same indentation, skipping the nested inputs between them
    */
  def downstream(plan: String, op: String): Seq[String] =
    val lines = plan.linesIterator.toIndexedSeq
    lines.indexWhere(l => opOf(l).startsWith(op)) match
      case -1 =>
        Nil
      case i =>
        val indent = indentOf(lines(i))
        lines
          .drop(i + 1)
          .filterNot(l => indentOf(l) > indent)
          .takeWhile(indentOf(_) == indent)
          .map(opOf)

end PlanText
