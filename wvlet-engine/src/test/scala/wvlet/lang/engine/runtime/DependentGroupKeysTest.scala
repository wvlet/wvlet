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

import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.expr.ArithOp
import wvlet.lang.engine.expr.Arith
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Grouping by the keys that determine the others (`dependent_group_keys`) */
class DependentGroupKeysTest extends UniTest:
  import RuntimePlan.*

  private val options = OptimizerOptions()

  /** An empty table of Int64 columns `cols`, declaring `keys` unique */
  private def table(name: String, cols: Seq[String], keys: Seq[Seq[String]]): MemTable = MemTable(
    name,
    Schema(cols.map(Field(_, PType.Int64)).toIndexedSeq),
    Nil,
    keys
  )

  // customer(c_custkey, c_name, c_nationkey) and nation(n_nationkey, n_name), as ids 0-2 and 3-4
  private val customer = table(
    "customer",
    Seq("c_custkey", "c_name", "c_nationkey"),
    Seq(Seq("c_custkey"))
  )

  private val nation = table("nation", Seq("n_nationkey", "n_name"), Seq(Seq("n_nationkey")))

  private def columns(t: MemTable, first: Int): IndexedSeq[Column] =
    IndexedSeq.tabulate(t.schema.size)(i =>
      Column(ColumnId(first + i), t.schema(i).name, PType.Int64)
    )

  private val c = Scan(customer, columns(customer, 0))
  private val n = Scan(nation, columns(nation, 3))

  private def ref(id: Int): BExpr = BExpr(ColRef(id, PType.Int64, s"c${id}"))

  private def join(kind: JoinKind): RuntimePlan = Join(
    c,
    n,
    kind,
    IndexedSeq(ref(2)),
    IndexedSeq(ref(3)),
    None,
    buildLeft = false
  )

  /** count(*) grouped by `keys` over `child`, outputting ids 10.. */
  private def aggregate(child: RuntimePlan, keys: IndexedSeq[BExpr]): Aggregate = Aggregate(
    child,
    keys,
    IndexedSeq(AggregateCall("count_star", Nil, distinct = false, filter = None, PType.Int64)),
    IndexedSeq.tabulate(keys.size + 1)(i => Column(ColumnId(10 + i), s"k${i}", PType.Int64))
  )

  private def run(p: RuntimePlan, o: OptimizerOptions = options): RuntimePlan = DependentGroupKeys
    .run(p, OptimizerContext(o))

  /** The group keys and call names of the aggregation in p */
  private def grouping(p: RuntimePlan): (Seq[BExpr], Seq[String]) =
    p match
      case Aggregate(_, keys, calls, _) =>
        (keys, calls.map(_.name))
      case other =>
        grouping(other.children.head)

  test("should group by a declared key alone, with the columns it determines through a join") {
    val p      = aggregate(join(JoinKind.Inner), IndexedSeq(ref(0), ref(1), ref(4)))
    val result = run(p)
    grouping(result) shouldBe (Seq(ref(0)), Seq("count_star", "any_value", "any_value"))
    // The aggregation's columns keep their ids and order
    result.output shouldBe p.output
  }

  test("should drop a computed key over the columns a key determines") {
    val doubled = BExpr(
      Arith(
        ArithOp.Mul,
        ColRef(1, PType.Int64, "c_name"),
        ColRef(1, PType.Int64, "c_name"),
        PType.Int64
      )
    )
    grouping(run(aggregate(c, IndexedSeq(doubled, ref(0))))) shouldBe
      (Seq(ref(0)), Seq("count_star", "any_value"))
  }

  test("should keep a key from the NULL-padded side of an outer join") {
    // n_name is NULL for customers without a nation, as for a nation row whose name is NULL
    grouping(run(aggregate(join(JoinKind.Left), IndexedSeq(ref(0), ref(1), ref(4))))) shouldBe
      (Seq(ref(0), ref(4)), Seq("count_star", "any_value"))
  }

  test("should drop a column of a table whose key a join equality makes known") {
    // c_nationkey = n_nationkey: grouping by c_nationkey determines n_name
    grouping(run(aggregate(join(JoinKind.Inner), IndexedSeq(ref(1), ref(2), ref(4))))) shouldBe
      (Seq(ref(1), ref(2)), Seq("count_star", "any_value"))
  }

  test("should keep keys that no declared key determines") {
    val p = aggregate(join(JoinKind.Inner), IndexedSeq(ref(1), ref(2)))
    run(p) shouldBe p
  }

  private def squared(id: Int): BExpr = BExpr(
    Arith(
      ArithOp.Mul,
      ColRef(id, PType.Int64, s"c${id}"),
      ColRef(id, PType.Int64, s"c${id}"),
      PType.Int64
    )
  )

  test("should not take a computed column of a key as knowing the key") {
    // y = c_custkey * c_custkey, projected below the aggregation, is grouped with c_name
    val projected = Project(
      c,
      IndexedSeq(squared(0), ref(1)),
      IndexedSeq(
        Column(ColumnId(20), "y", PType.Int64),
        Column(ColumnId(21), "c_name", PType.Int64)
      )
    )
    val p = aggregate(projected, IndexedSeq(ref(20), ref(21)))
    run(p) shouldBe p
  }

  test("should not take a join on a computed column as equal columns") {
    // c_nationkey = n_nationkey * n_nationkey makes no nation key known
    val projected = Project(
      n,
      IndexedSeq(squared(3), ref(4)),
      IndexedSeq(
        Column(ColumnId(20), "nk2", PType.Int64),
        Column(ColumnId(21), "n_name", PType.Int64)
      )
    )
    val j = Join(
      c,
      projected,
      JoinKind.Inner,
      IndexedSeq(ref(2)),
      IndexedSeq(ref(20)),
      None,
      buildLeft = false
    )
    val p = aggregate(j, IndexedSeq(ref(2), ref(21)))
    run(p) shouldBe p
  }

  test("should leave the keys with dependent_group_keys off") {
    val p = aggregate(join(JoinKind.Inner), IndexedSeq(ref(0), ref(1), ref(4)))
    run(p, options.withDisabled(Optimization.DependentGroupKeys)) shouldBe p
  }

  test("should prune an any_value nobody reads") {
    val reduced = run(aggregate(join(JoinKind.Inner), IndexedSeq(ref(0), ref(1), ref(4))))
    // Only the customer key and the count are read
    val pruned = PruneColumns.prune(
      reduced,
      Set(ColumnId(10), ColumnId(13)),
      OptimizerContext(options)
    )
    grouping(pruned) shouldBe (Seq(ref(0)), Seq("count_star"))
  }

end DependentGroupKeysTest
