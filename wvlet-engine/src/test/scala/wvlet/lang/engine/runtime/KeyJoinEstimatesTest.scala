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
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.CmpOp
import wvlet.lang.engine.expr.Compare
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.KeyCoverage
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Composite-key join estimates from declared unique keys (`key_join_estimates`) */
class KeyJoinEstimatesTest extends UniTest:
  private val on  = OptimizerOptions()
  private val off = on.withDisabled(Optimization.KeyJoinEstimates)

  /** A table of `rows` rows (no batches; estimates only) with columns a, b, c */
  private def table(name: String, rows: Int, keys: Seq[Seq[String]]): MemTable =
    val schema = Schema(
      IndexedSeq(Field("a", PType.Int64), Field("b", PType.Int64), Field("c", PType.Int64))
    )
    val cols =
      Array.tabulate[Vec](3)(_ => LongVec(PType.Int64, Array.tabulate(rows)(_.toLong), null))
    MemTable(name, schema, Seq(Batch(cols, rows)), keys)

  // Scans whose columns have ids `first`, `first + 1`, `first + 2`
  private def scan(t: MemTable, first: Int): RuntimePlan.Scan = RuntimePlan.Scan(
    t,
    IndexedSeq.tabulate(3)(i => Column(ColumnId(first + i), t.schema(i).name, PType.Int64))
  )

  private def col(id: Int): ColRef = ColRef(id, PType.Int64, s"c${id}")

  test("divide a join on a declared composite key by the key's base rows") {
    // lineitem (60M) ⋈ partsupp filtered to 400K of 8M rows on its primary key
    val d = CardinalityEstimator.compositeDivisor(
      6e7,
      IndexedSeq(Some(2e6), Some(1e5)),
      4e5,
      IndexedSeq(Some(2e6), Some(1e5)),
      KeyCoverage.NotUnique,
      KeyCoverage.Unique(8e6)
    )
    d shouldBe Some(8e6)
    CardinalityEstimator.joinOutputRows(
      6e7,
      IndexedSeq.empty,
      4e5,
      IndexedSeq.empty,
      d.toSeq
    ) shouldBe 3e6
  }

  test("divide a join on columns that are no declared key by every pair, without the cap") {
    // catalog_sales (1.2M) ⋈ store_returns (240K) on item (102K) and customer (500K)
    val ld = IndexedSeq(Some(1.02e5), Some(5e5))
    val d  = CardinalityEstimator.compositeDivisor(
      1.2e6,
      ld,
      2.4e5,
      ld,
      KeyCoverage.NotUnique,
      KeyCoverage.NotUnique
    )
    // Each pair by its larger distinct count: 1.2M * 240K / 5.1e10 = 5.6 rows
    d shouldBe Some(1.02e5 * 5e5)
    // Today's estimate caps the divisor at the larger side
    CardinalityEstimator.joinOutputRows(1.2e6, ld, 2.4e5, ld) shouldBe 2.4e5
    CardinalityEstimator.compositeDivisor(
      1.2e6,
      ld,
      2.4e5,
      ld,
      KeyCoverage.NotUnique,
      KeyCoverage.Unknown
    ) shouldBe None
  }

  test("classify join keys by the unique keys their scan's table declares") {
    val t    = table("t", 100, Seq(Seq("a", "b")))
    val s    = scan(t, 0)
    val cond = Compare(CmpOp.Gt, col(2), Const(0L, PType.Int64))
    Stats.keyCoverage(s, Seq(col(0), col(1))) shouldBe KeyCoverage.Unique(100.0)
    Stats.keyCoverage(s, Seq(col(1), col(0), col(2))) shouldBe KeyCoverage.Unique(100.0)
    Stats.keyCoverage(RuntimePlan.Filter(s, BExpr(cond)), Seq(col(0), col(1))) shouldBe
      KeyCoverage.Unique(100.0)
    Stats.keyCoverage(s, Seq(col(0), col(2))) shouldBe KeyCoverage.NotUnique
    // No declared keys, or columns of two scans
    Stats.keyCoverage(scan(table("u", 100, Nil), 0), Seq(col(0), col(1))) shouldBe
      KeyCoverage.Unknown
    val join = RuntimePlan.Join(
      s,
      scan(t, 3),
      JoinKind.Inner,
      IndexedSeq(BExpr(col(2))),
      IndexedSeq(BExpr(col(5))),
      None,
      buildLeft = false
    )
    Stats.keyCoverage(join, Seq(col(0), col(4))) shouldBe KeyCoverage.Unknown
    Stats.keyCoverage(join, Seq(col(0), col(1))) shouldBe KeyCoverage.Unique(100.0)
  }

  test("estimate a composite-key join from declared keys only with key_join_estimates") {
    val fact  = scan(table("f", 1000, Seq(Seq("c"))), 0)
    val other = scan(table("g", 1000, Seq(Seq("c"))), 3)
    val lk    = IndexedSeq(col(0), col(1))
    val rk    = IndexedSeq(col(3), col(4))
    // a and b are no key of either table: the product of the pairs' divisors, at least one row
    Stats.joinRows(fact, 1000, lk, other, 1000, rk, on) shouldBe 1.0
    // The cap of correlated keys: 1000 * 1000 / max(1000, 1000)
    Stats.joinRows(fact, 1000, lk, other, 1000, rk, off) shouldBe 1000.0
  }

end KeyJoinEstimatesTest
