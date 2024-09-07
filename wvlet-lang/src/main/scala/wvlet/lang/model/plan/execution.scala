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
sealed trait ExecutionPlan:
  def isEmpty: Boolean = false

object ExecutionPlan:
  def empty: ExecutionPlan = ExecuteNothing
  def apply(tasks:List[ExecutionPlan]): ExecutionPlan =
    tasks match
      case Nil => ExecuteNothing
      case task :: Nil => task
      case lst => ExecuteTasks(lst)

case object ExecuteNothing extends ExecutionPlan:
  override def isEmpty: Boolean = true

case class ExecuteTasks(tasks: List[ExecutionPlan]) extends ExecutionPlan

case class ExecuteQuery(plan: LogicalPlan) extends ExecutionPlan
case class ExecuteTest(test: TestRelation) extends ExecutionPlan

