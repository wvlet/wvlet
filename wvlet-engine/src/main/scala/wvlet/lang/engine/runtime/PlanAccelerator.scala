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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.exec.Operator

/**
  * Runs whole [[RuntimePlan]] subtrees on another device, such as a GPU. [[Lowering]] offers each
  * subtree to the accelerator top-down, before lowering it to CPU operators: the first subtree it
  * accepts runs there as one operator, and the rest of the plan stays on the CPU. An accelerator
  * thus runs the largest subtrees it fully supports.
  *
  * The returned operator must produce the rows of `p` with `p.schema`, as the CPU operators would.
  * It is planned with the `gpu_offload` optimization ([[wvlet.lang.engine.plan.Optimization]]).
  */
trait PlanAccelerator:
  /** A short name for EXPLAIN and logs, e.g. `gpu` */
  def name: String

  /**
    * An operator computing all of `p`, or None to lower `p` (and offer its inputs) on the CPU.
    * `cpu` lowers a plan to CPU operators only, without the accelerator: the operator may run it
    * instead when the device fails at run time (e.g. out of memory)
    */
  def accelerate(p: RuntimePlan, cpu: RuntimePlan => Operator): Option[Operator]
