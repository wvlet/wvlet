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

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv

/**
  * An inlined model body is named after the model, so that its columns can be qualified by the
  * model name like the columns of a table, unless the reference has its own alias
  */
class ModelNameQualifierTest extends UniTest:

  private val models = CompilationUnit.fromWvletString("""model nq_customers = {
      |  from [[1, 'alice']] as t(cid, customer_name)
      |  select cid as customer_id, customer_name
      |}
      |model nq_orders = {
      |  from [[10, 1]] as t(oid, cust)
      |  select oid as order_id, cust as customer_id
      |}
      |""".stripMargin)

  private def sqlOf(query: String): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString(query)
    val result   = compiler.compileMultipleUnits(List(models), unit)
    GenSQL.generateSQL(unit)(using result.context)

  test("should qualify the columns of joined models by the model names") {
    val sql = sqlOf("""from nq_orders
        |join nq_customers on nq_orders.customer_id = nq_customers.customer_id
        |select order_id, nq_orders.customer_id, customer_name
        |""".stripMargin)
    sql shouldContain ") as nq_orders"
    sql shouldContain ") as nq_customers"
    sql shouldContain "on nq_orders.customer_id = nq_customers.customer_id"
  }

  test("should keep only the user's alias of an aliased model reference") {
    val sql = sqlOf("""from nq_orders as o
        |join nq_customers as c on o.customer_id = c.customer_id
        |select o.customer_id
        |""".stripMargin)
    sql shouldContain ") as o"
    sql shouldContain ") as c"
    sql shouldNotContain "as nq_orders"
    sql shouldNotContain "as nq_customers"
  }

end ModelNameQualifierTest
