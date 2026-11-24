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
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** EXISTS / IN / NOT IN filter conjuncts as semi and anti joins on the runtime plan */
class EngineDecorrelateTest extends UniTest:
  import RuntimePlan.*

  private val options = OptimizerOptions().withValidatePlans(true)
  // Rows are checked against DuckDB through generated SQL, over the same tables
  private val conformance = EngineConformance.tpch(options)

  private val engine = conformance.engine

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /** The compiled query, as the engine receives it (before model expansion) */
  private def compiled[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    f(EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    compiled(query)(q => f(GenSQL.expand(q, summon[Context])))

  private def runtimePlan(query: String): RuntimePlan =
    val p = withQuery(query)(engine.runtimePlan(_))
    debug(p.explain)
    p

  private def nodes(p: RuntimePlan): Seq[RuntimePlan] = p +: p.children.flatMap(nodes)

  private def joinKinds(p: RuntimePlan): Seq[JoinKind] = nodes(p).collect { case j: Join =>
    j.kind
  }

  private def rows(e: WvletEngine, query: String): Seq[String] =
    withQuery(query) { q =>
      e.execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
        .sorted
    }

  /** The query must return the same rows on `e` as on DuckDB, which must accept its SQL */
  private def matchesDuckDB(query: String, e: WvletEngine): Unit =
    compiled(query)(conformance.assertMatches(query, _, e))

  /** The rows of the query, the same as DuckDB's */
  private def sameRows(query: String): Seq[String] =
    matchesDuckDB(query, engine)
    rows(engine, query)

  test("bind IN over an uncorrelated subquery as a semi join") {
    val query =
      """select n_name from nation
        |where n_regionkey in (select r_regionkey from region where r_name = 'ASIA')""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldBe Seq(JoinKind.Semi)
    sameRows(query).size shouldBe 5
  }

  test("bind NOT IN as an anti join over the subquery's values") {
    val query =
      """select n_name from nation
        |where n_regionkey not in (select r_regionkey from region where r_name = 'ASIA')"""
        .stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldBe Seq(JoinKind.Anti)
    // The values, decided at plan time, are a Values leaf
    nodes(p).exists(_.isInstanceOf[Values]) shouldBe true
    sameRows(query).size shouldBe 20
  }

  test("bind IN with a scalar subquery in its target as a semi join") {
    val query =
      """select n_name from nation
        |where n_regionkey + (select min(r_regionkey) from region) in (
        |  select r_regionkey from region where r_name = 'ASIA'
        |)""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldBe Seq(JoinKind.Semi)
    sameRows(query).size shouldBe 5
  }

  test("return no rows for NOT IN when the subquery has a NULL") {
    val query =
      """select n_name from nation
        |where n_regionkey not in (
        |  select case when r_name = 'ASIA' then null else r_regionkey end from region
        |)""".stripMargin
    joinKinds(runtimePlan(query)) shouldBe Nil
    sameRows(query) shouldBe Nil
  }

  test("decide uncorrelated EXISTS at plan time") {
    val none =
      """select n_name from nation
        |where exists (select 1 from region where r_name = 'MARS')""".stripMargin
    sameRows(none) shouldBe Nil
    val all =
      """select n_name from nation
        |where not exists (select 1 from region where r_name = 'MARS')""".stripMargin
    sameRows(all).size shouldBe 25
  }

  test("filter by the plain conjuncts before the semi join") {
    val query =
      """select n_name from nation
        |where n_regionkey in (select r_regionkey from region where r_name <> 'ASIA')
        |  and n_name like 'A%'""".stripMargin
    val semi =
      nodes(runtimePlan(query))
        .collectFirst { case j: Join =>
          j
        }
        .get
    semi.kind shouldBe JoinKind.Semi
    semi.left.isInstanceOf[Filter] shouldBe true
    sameRows(query) shouldBe Seq("ALGERIA", "ARGENTINA")
  }

  /** The inner join directly above the first semi or anti join, if any */
  private def joinAboveSemi(p: RuntimePlan): Option[Join] = nodes(p).collectFirst {
    case j @ Join(l, r, JoinKind.Inner, _, _, _, _)
        if Seq(l, r).exists(c =>
          joinKinds(c).headOption.exists(k => k == JoinKind.Semi || k == JoinKind.Anti)
        ) =>
      j
  }

  test("join IN over one input of a join tree onto that input") {
    val query =
      """select s_name, n_name from supplier, nation
        |where s_nationkey = n_nationkey
        |  and n_regionkey in (select r_regionkey from region where r_name = 'ASIA')""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldContain JoinKind.Semi
    // The semi join is below the inner join
    joinAboveSemi(p).isDefined shouldBe true
    sameRows(query).nonEmpty shouldBe true
  }

  test("keep NOT IN over one input of a join tree above the ordered tree") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey not in (select s_nationkey from supplier where s_acctbal > 9000)"""
        .stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldContain JoinKind.Anti
    joinAboveSemi(p) shouldBe None
    sameRows(query).nonEmpty shouldBe true
  }

  test("return no rows for NOT IN over a join tree input when the subquery has a NULL") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_regionkey not in (
        |    select case when r_name = 'ASIA' then null else r_regionkey end from region
        |  )""".stripMargin
    joinKinds(runtimePlan(query)) shouldNotContain JoinKind.Anti
    sameRows(query) shouldBe Nil
  }

  test("keep IN over several inputs of a join tree above the ordered tree") {
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey + r_regionkey in (select s_nationkey from supplier)""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p).headOption shouldBe Some(JoinKind.Semi)
    joinAboveSemi(p) shouldBe None
    sameRows(query).nonEmpty shouldBe true
  }

  test("keep IN over a much larger subquery above the ordered tree") {
    // The semi join would build on the input; over the tree, the joins reduce it first
    val query =
      """select n_name, r_name from nation, region
        |where n_regionkey = r_regionkey
        |  and r_regionkey in (select l_suppkey from lineitem)""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p).headOption shouldBe Some(JoinKind.Semi)
    joinAboveSemi(p) shouldBe None
    sameRows(query).nonEmpty shouldBe true
  }

  test("keep IN above the ordered tree with place_semi_joins disabled") {
    val query =
      """select s_name, n_name from supplier, nation
        |where s_nationkey = n_nationkey
        |  and n_regionkey in (select r_regionkey from region where r_name = 'ASIA')""".stripMargin
    val off = engine.withOptimizer(options.withDisabled(Optimization.PlaceSemiJoins))
    val p   = withQuery(query)(off.runtimePlan(_))
    joinKinds(p).headOption shouldBe Some(JoinKind.Semi)
    joinAboveSemi(p) shouldBe None
  }

  private val noDecorrelate = engine.withOptimizer(options.withDisabled(Optimization.Decorrelate))

  /** The plan and rows with decorrelate disabled, checked against DuckDB's rows */
  private def withoutDecorrelate(query: String): (RuntimePlan, Seq[String]) =
    val p = withQuery(query)(noDecorrelate.runtimePlan(_))
    debug(p.explain)
    matchesDuckDB(query, noDecorrelate)
    (p, rows(noDecorrelate, query))

  test("bind uncorrelated subquery conjuncts as plan-time constants with decorrelate disabled") {
    def constant(query: String): Seq[String] =
      val (p, r) = withoutDecorrelate(query)
      joinKinds(p) shouldBe Nil
      nodes(p).exists(_.isInstanceOf[Filter]) shouldBe true
      r

    constant("""select n_name from nation
        |where n_regionkey in (select r_regionkey from region where r_name = 'ASIA')""".stripMargin)
      .size shouldBe 5
    constant("""select n_name from nation
        |where n_regionkey not in (select r_regionkey from region where r_name = 'ASIA')
        |  and n_name like 'A%'""".stripMargin) shouldBe Seq("ALGERIA", "ARGENTINA")
    // NULL in the values: no row passes
    constant("""select n_name from nation
        |where n_regionkey not in (
        |  select case when r_name = 'ASIA' then null else r_regionkey end from region
        |)""".stripMargin) shouldBe Nil
    // NULL targets never pass NOT IN
    constant("""select n_name from nation
        |where (case when n_nationkey < 10 then null else n_regionkey end) not in (
        |  select r_regionkey from region where r_name = 'ASIA'
        |)""".stripMargin).size shouldBe 12
    constant("""select n_name from nation
        |where exists (select 1 from region where r_name = 'MARS')""".stripMargin) shouldBe Nil
  }

  test("bind subquery conjuncts over joins as plan-time constants with decorrelate disabled") {
    // Over an inner join tree
    val (tree, treeRows) = withoutDecorrelate(
      """select n_name from nation, region
        |where n_regionkey = r_regionkey
        |  and n_nationkey in (select s_nationkey from supplier where s_acctbal > 9900)"""
        .stripMargin
    )
    joinKinds(tree) shouldBe Seq(JoinKind.Inner)
    treeRows.size shouldBe 1
    // Over a left join
    val (left, leftRows) = withoutDecorrelate(
      """select n_name, r_name from nation
        |left join region on n_regionkey = r_regionkey and r_name = 'ASIA'
        |where n_nationkey not in (select s_nationkey from supplier where s_acctbal > 9000)"""
        .stripMargin
    )
    joinKinds(left) shouldBe Seq(JoinKind.Left)
    leftRows.nonEmpty shouldBe true
  }

  test("bind correlated subquery conjuncts as joins with decorrelate disabled") {
    val (p, r) = withoutDecorrelate(
      """select n_name from nation
        |where exists (select 1 from region where r_regionkey = n_regionkey and r_name = 'ASIA')"""
        .stripMargin
    )
    joinKinds(p) shouldBe Seq(JoinKind.Semi)
    r.size shouldBe 5
  }

  test("bind correlated EXISTS as a semi join over the decorrelated body") {
    val query =
      """select n_name from nation
        |where exists (select 1 from region where r_regionkey = n_regionkey and r_name = 'ASIA')"""
        .stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldBe Seq(JoinKind.Semi)
    // The body's local filter stays on its side of the join
    nodes(p).collectFirst { case j: Join =>
      j.right.isInstanceOf[Filter]
    } shouldBe Some(true)
    sameRows(query).size shouldBe 5
  }

  test("bind correlated NOT EXISTS with a residual condition as an anti join") {
    val query =
      """select n_name from nation n
        |where not exists (
        |  select 1 from nation n2
        |  where n2.n_regionkey = n.n_regionkey and n2.n_nationkey <> n.n_nationkey
        |    and n2.n_nationkey < 5
        |)""".stripMargin
    val p    = runtimePlan(query)
    val anti =
      nodes(p)
        .collectFirst { case j: Join =>
          j
        }
        .get
    anti.kind shouldBe JoinKind.Anti
    anti.residual.isDefined shouldBe true
    sameRows(query).size shouldBe 12
  }

  test("bind correlated IN as a semi join on the target and the correlation keys") {
    val query =
      """select n_name from nation
        |where n_nationkey in (
        |  select s_nationkey from supplier where s_nationkey = n_nationkey and s_acctbal > 9900
        |)""".stripMargin
    val p    = runtimePlan(query)
    val semi =
      nodes(p)
        .collectFirst { case j: Join =>
          j
        }
        .get
    semi.kind shouldBe JoinKind.Semi
    semi.leftKeys.size shouldBe 2
    sameRows(query).size shouldBe 1
  }

  // Expected counts are DuckDB's answers for the same queries over the same data
  test("filter out rows whose correlated NOT IN group has a NULL value") {
    val query =
      """select r_name from region
        |where r_regionkey + 100 not in (
        |  select case when n_nationkey = 3 then null else n_nationkey end
        |  from nation where n_regionkey = r_regionkey
        |)""".stripMargin
    val p = runtimePlan(query)
    joinKinds(p) shouldBe Seq(JoinKind.Anti)
    sameRows(query).size shouldBe 4
  }

  test("filter out NULL targets of correlated NOT IN only when the group is not empty") {
    val nonEmpty =
      """select r_name from region
        |where (case when r_regionkey = 2 then null else r_regionkey + 100 end) not in (
        |  select n_nationkey from nation where n_regionkey = r_regionkey
        |)""".stripMargin
    sameRows(nonEmpty).size shouldBe 4
    val empty =
      """select r_name from region
        |where (case when r_regionkey = 2 then null else r_regionkey + 100 end) not in (
        |  select n_nationkey from nation where n_regionkey = r_regionkey and n_nationkey > 100
        |)""".stripMargin
    sameRows(empty).size shouldBe 5
  }

  test("read a correlated NOT IN target from the outer query when an inner column has its name") {
    val query =
      """select n_name from nation n1
        |where n_nationkey not in (
        |  select n_nationkey from nation n2
        |  where n2.n_regionkey = n1.n_regionkey and n2.n_nationkey < 3
        |)""".stripMargin
    sameRows(query).size shouldBe 22
  }

  test("bind correlated NOT IN without correlation equalities as a nested loop anti join") {
    val query =
      """select r_name from region
        |where r_regionkey not in (select n_regionkey from nation where n_nationkey > r_regionkey * 5)"""
        .stripMargin
    val p    = runtimePlan(query)
    val anti =
      nodes(p)
        .collectFirst { case j: Join =>
          j
        }
        .get
    anti.kind shouldBe JoinKind.Anti
    anti.leftKeys.isEmpty shouldBe true
    sameRows(query).size shouldBe 1
  }

  private def leftJoins(p: RuntimePlan): Int = joinKinds(p).count(_ == JoinKind.Left)

  test("bind a correlated scalar subquery as a left join with the grouped body") {
    val query =
      """select s_name from supplier s1
        |where s_acctbal > (
        |  select avg(s_acctbal) from supplier s2 where s2.s_nationkey = s1.s_nationkey
        |)""".stripMargin
    val p = runtimePlan(query)
    leftJoins(p) shouldBe 1
    nodes(p).exists(_.isInstanceOf[Aggregate]) shouldBe true
    sameRows(query).size shouldBe 48
  }

  test("give count of a correlated group without rows as 0") {
    val query =
      """select n_name from nation
        |where (
        |  select count(*) from supplier where s_nationkey = n_nationkey and s_acctbal > 9900
        |) = 0""".stripMargin
    sameRows(query).size shouldBe 24
    val filtered =
      """select n_name from nation
        |where n_regionkey = 1 and (
        |  select count(*) from supplier where s_nationkey = n_nationkey and s_acctbal > 9000
        |) = 0""".stripMargin
    sameRows(filtered).size shouldBe 2
  }

  test("keep a NULL value of a matched correlated group when its empty value is not NULL") {
    val query =
      """select n_name from nation
        |where (
        |  select case when count(*) > 0 then null else 1 end
        |  from supplier where s_nationkey = n_nationkey and s_acctbal > 9900
        |) is null""".stripMargin
    sameRows(query).size shouldBe 1
  }

  test("bind correlated EXISTS under OR as a left join with the distinct keys") {
    val query =
      """select n_name from nation
        |where n_nationkey < 3 or exists (
        |  select 1 from supplier where s_nationkey = n_nationkey and s_acctbal > 9900
        |)""".stripMargin
    val p = runtimePlan(query)
    leftJoins(p) shouldBe 1
    sameRows(query).size shouldBe 4
  }

  test("bind correlated EXISTS under NOT and CASE") {
    val negated =
      """select n_name from nation
        |where not (n_nationkey < 3 or exists (
        |  select 1 from supplier where s_nationkey = n_nationkey and s_acctbal > 9900
        |))""".stripMargin
    sameRows(negated).size shouldBe 21
    val cased =
      """select n_name from nation
        |where (case when exists (
        |  select 1 from supplier where s_nationkey = n_nationkey and s_acctbal > 9900
        |) then 1 else 0 end) = 0""".stripMargin
    sameRows(cased).size shouldBe 24
  }

  test("check an outer column for NULL in a correlated body, not as a correlation key") {
    // Wvlet's `l.a is null` is `l.a = null`, a null check: as a key, it would never match
    for (exists, want) <- Seq("exists" -> Seq("2|null"), "not exists" -> Seq("1|1", "3|null")) do
      val query =
        s"""from [[1, 1], [2, null], [3, null]] as l(i, a)
           |where ${exists} { from [[1], [2]] as r(i) where r.i = l.i and l.a is null }"""
          .stripMargin
      val unit      = CompilationUnit.fromWvletString(query)
      val r         = compiler.compileSingleUnit(unit)
      given Context = r.context
      val q = GenSQL.expand(EngineTPCHBench.queryOf(unit).getOrElse(fail(query)), summon[Context])
      conformance.assertMatches(query, q)
      engine
        .execute(q)
        .batches
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).mkString("|")))
        .sorted shouldBe want
  }

end EngineDecorrelateTest
