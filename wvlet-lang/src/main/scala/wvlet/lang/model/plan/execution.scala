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
package wvlet.lang.model.plan

import wvlet.lang.model.NodeLocation
import wvlet.lang.model.expr.{Attribute, NameExpr}
import wvlet.airframe.ulid.ULID

/**
  * Additional nodes that for organizing tasks for executing LogicalPlan nodes
  */
sealed trait ExecutionPlan extends Product:
  def isEmpty: Boolean = false
  def planName: String = this.getClass.getSimpleName.stripSuffix("$")

  def pp: String =
    def iter(p: ExecutionPlan, level: Int): String =
      def indent(s: String): String =
        val lines = s.split("\n")
        lines.map(l => s"${("  " * level)}${l}").mkString("\n")

      val header = p.planName
      val body =
        p match
          case t: ExecuteTasks =>
            val tasks = t.tasks.map(iter(_, level + 1)).mkString("\n")
            s"- ${header}\n${tasks}"
          case q: ExecuteQuery =>
            s"- ${header}\n${q.plan.pp}"
          case s: ExecuteSave =>
            s"- ${header} ${s.save.targetName}"
          case t: ExecuteTest =>
            s"- ${header} ${t.test.testExpr.pp}"
          case _ =>
            ""

      indent(body)

    iter(this, 0)

object ExecutionPlan:
  def empty: ExecutionPlan = ExecuteNothing
  def apply(tasks: List[ExecutionPlan]): ExecutionPlan =
    tasks match
      case Nil =>
        ExecuteNothing
      case task :: Nil =>
        task
      case lst =>
        ExecuteTasks(lst)

case object ExecuteNothing extends ExecutionPlan:
  override def isEmpty: Boolean = true

case class ExecuteTasks(tasks: List[ExecutionPlan]) extends ExecutionPlan

case class ExecuteQuery(plan: LogicalPlan)                            extends ExecutionPlan
case class ExecuteSave(save: Save, queryPlan: ExecutionPlan)          extends ExecutionPlan
case class ExecuteDelete(delete: DeleteOps, queryPlan: ExecutionPlan) extends ExecutionPlan
case class ExecuteCommand(execute: Execute)                           extends ExecutionPlan
case class ExecuteTest(test: TestRelation)                            extends ExecutionPlan

case class ExecuteDebug(debug: Debug, debugExecutionPlan: ExecutionPlan) extends ExecutionPlan
