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

import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/**
  * With `implied_join_equalities`, the join search may join two inputs by an equality that follows
  * from the written ones (`a.k = b.k and b.k = c.k` gives `a.k = c.k`), and a join keys on one
  * column pair per equivalence class. Each query must return DuckDB's rows with the toggle on and
  * off, NULL and duplicate keys included.
  */
class ImpliedJoinEqualitiesTest extends UniTest:

  private val dir = s"target/implied-join-equalities/${ProcessHandle.current().pid()}"
  java.io.File(dir).mkdirs()

  // Small tables with NULL keys, duplicates, and keys missing from the other tables
  private val tables = Seq(
    "a" -> "(values (1, 10), (1, 11), (2, 20), (3, 30), (null, 40)) as v(k, x)",
    "b" ->
      "(values (1, 1, 100), (2, 1, 200), (2, 2, 201), (4, 2, 400), (null, 3, 500)) as v(k, g, y)",
    "c" -> "(values (1, 1000), (2, 2000), (2, 2001), (5, 5000), (null, 6000)) as v(k, z)",
    "d" -> "(values (1, 'one'), (2, 'two'), (3, 'three')) as v(g, name)"
  )

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance(
    "spec/tpch",
    tables.flatMap((t, values) =>
      Seq(
        s"create table ${t} as select * from ${values}",
        s"copy ${t} to '${dir}/${t}.parquet' (format parquet)"
      )
    ),
    e => tables.foreach((t, _) => e.catalog.registerParquet(t, s"${dir}/${t}.parquet")),
    options
  )

  private val engine = conformance.engine
  private val off = engine.withOptimizer(options.withDisabled(Optimization.ImpliedJoinEqualities))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def compiled[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(query)
    val result    = compiler.compileSingleUnit(unit)
    given Context = result.context
    f(EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}")))

  Seq(
    "a chain of three tables" -> "select a.x, b.y, c.z from a, b, c where a.k = b.k and b.k = c.k",
    "a chain written from the other end" ->
      "select a.x, b.y, c.z from a, b, c where c.k = b.k and a.k = b.k",
    "a chain with another join key" ->
      "select a.x, b.y, c.z, d.name from a, b, c, d where a.k = b.k and b.k = c.k and b.g = d.g",
    "a chain that is also written whole" ->
      "select a.x, c.z from a, b, c where a.k = b.k and b.k = c.k and a.k = c.k",
    "a chain under an aggregation" ->
      "select d.name, sum(a.x + c.z) as s from a, b, c, d where a.k = b.k and b.k = c.k and b.g = d.g group by d.name",
    "a chain with a filter" ->
      "select a.x, c.z from a, b, c where a.k = b.k and b.k = c.k and b.y > 150",
    // A clique of ten inputs is more pairs than the exact search costs: it plans the written chain
    "a chain of ten inputs" ->
      "select count(*) as n, sum(t1.x + t10.x) as s from a as t1, a as t2, a as t3, a as t4, a as t5, a as t6, a as t7, a as t8, a as t9, a as t10 where t1.k = t2.k and t2.k = t3.k and t3.k = t4.k and t4.k = t5.k and t5.k = t6.k and t6.k = t7.k and t7.k = t8.k and t8.k = t9.k and t9.k = t10.k",
    "a class with two columns of one table" ->
      "select a.x, b.y, c.z from a, b, c where a.k = b.k and b.k = c.k and a.k = b.g and b.g = c.k"
  ).foreach { (label, query) =>
    test(s"return DuckDB's rows for ${label}") {
      compiled(query)(conformance.assertMatches(query, _, engine))
      compiled(query)(conformance.assertMatches(query, _, off))
    }
  }

end ImpliedJoinEqualitiesTest
