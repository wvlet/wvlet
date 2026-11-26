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
package wvlet.lang.engine.exec

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import scala.util.Random

/**
  * [[RoutedAggFilters]] folds aggregates with FILTERs over one shared dictionary into the same
  * results as folding each with its own FILTER mask
  */
class RoutedAggFiltersTest extends UniTest:
  private val dict = Array("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday")
  private val day  = ColRef(1, PType.Str, "day")
  private val x    = ColRef(0, PType.Int64, "x")
  private val rnd  = Random(11)

  /** n rows of (x, day) with day over `dict`, NULL (code -1) when `withNulls` and drawn so */
  private def batch(n: Int, withNulls: Boolean, d: Array[String] = dict): Batch =
    val codes =
      Array.fill(n)(
        if withNulls && rnd.nextInt(5) == 0 then
          -1
        else
          rnd.nextInt(d.length)
      )
    val values = codes.map(c =>
      if c >= 0 then
        d(c)
      else
        null
    )
    val nulls =
      if withNulls then
        codes.map(_ < 0)
      else
        null
    val xs = LongVec(PType.Int64, Array.fill(n)(rnd.nextInt(100).toLong), null)
    Batch(Array(xs, StrVec(values, nulls, codes, d.length, d)), n)

  private def eqDay(s: String): VExpr = Compare(CmpOp.Eq, day, Const(s, PType.Str))

  private def call(name: String, filter: VExpr, routed: Boolean = true): AggCall = AggCall(
    name,
    Seq(x),
    false,
    Some(filter),
    PType.Int64,
    dictionaryFilter = true,
    routedFilter = routed
  )

  // New calls per use: each FILTER keeps the results of the first dictionary it reads
  private def calls = IndexedSeq(
    call("sum", eqDay("Sunday")),
    call("count", eqDay("Monday")),
    call("max", IsNullExpr(day, negate = false)),
    call("sum", eqDay("Friday")),
    // The same FILTER as the first: shares its rows (an avg's partial sum and count)
    call("count", eqDay("Sunday")),
    // Not routed: folds on its own
    call("sum", eqDay("Tuesday"), routed = false),
    AggCall("count_star", Nil, false, None, PType.Int64)
  )

  /** Each aggregate's result per group, folding each with its own FILTER mask */
  private def expected(aggs: IndexedSeq[AggCall], bs: Seq[Batch], groups: Seq[Array[Int]]) = aggs
    .map { a =>
      val acc = a.newAccumulator()
      bs.zip(groups)
        .foreach { (b, g) =>
          acc.ensureGroups(3)
          a.filter match
            case Some(f) =>
              val mask = Kernels.trueMask(f.eval(b))
              val idx  = (0 until b.size).filter(mask(_)).toArray
              acc.update(
                idx.map(g(_)),
                idx.length,
                a.args.map(_.eval(b).gather(idx, idx.length)).toArray
              )
            case None =>
              acc.update(g, b.size, a.args.map(_.eval(b)).toArray)
        }
      (0 until 3).map(acc.result(3).get)
    }

  private def routedResults(aggs: IndexedSeq[AggCall], bs: Seq[Batch], groups: Seq[Array[Int]]) =
    val router = RoutedAggFilters.of(aggs)
    (router != null) shouldBe true
    val accs = aggs.map(_.newAccumulator())
    bs.zip(groups)
      .foreach { (b, g) =>
        val handled = router.update(accs, g, 3, b)
        (handled != null) shouldBe true
        aggs
          .indices
          .filterNot(handled(_))
          .foreach { a =>
            accs(a).ensureGroups(3)
            aggs(a).filter match
              case Some(f) =>
                val mask = Kernels.trueMask(f.eval(b))
                val idx  = (0 until b.size).filter(mask(_)).toArray
                accs(a).update(
                  idx.map(g(_)),
                  idx.length,
                  aggs(a).args.map(_.eval(b).gather(idx, idx.length)).toArray
                )
              case None =>
                accs(a).update(g, b.size, aggs(a).args.map(_.eval(b)).toArray)
          }
      }
    accs.map(acc => (0 until 3).map(acc.result(3).get))

  test("route only the marked aggregates with a FILTER over one shared dictionary") {
    val router = RoutedAggFilters.of(calls)
    val bs     = Seq(batch(100, withNulls = true))
    val gs     = bs.map(b => Array.fill(b.size)(rnd.nextInt(3)))
    val accs   = calls.map(_.newAccumulator())
    router.update(accs, gs.head, 3, bs.head).toSeq shouldBe
      Seq(true, true, true, true, true, false, false)
    RoutedAggFilters.of(calls.map(_.copy(routedFilter = false))) shouldBe null
    RoutedAggFilters.of(calls.take(1)) shouldBe null
  }

  test("give the same results as folding each FILTER on its own") {
    for withNulls <- Seq(false, true) do
      val bs = Seq(batch(200, withNulls), batch(64, withNulls), batch(300, withNulls))
      val gs = bs.map(b => Array.fill(b.size)(rnd.nextInt(3)))
      routedResults(calls, bs, gs) shouldBe expected(calls, bs, gs)
  }

  test("fold every row when one FILTER passes all of them") {
    val one = batch(50, withNulls = false, Array("Sunday"))
    val gs  = Seq(Array.fill(one.size)(rnd.nextInt(3)))
    routedResults(calls, Seq(one), gs) shouldBe expected(calls, Seq(one), gs)
  }

  test("not route FILTERs that one code passes twice") {
    val overlapping = IndexedSeq(
      call("sum", eqDay("Sunday")),
      call("sum", Compare(CmpOp.Lt, day, Const("T", PType.Str)))
    )
    val b      = batch(100, withNulls = false)
    val router = RoutedAggFilters.of(overlapping)
    router.update(overlapping.map(_.newAccumulator()), new Array[Int](b.size), 1, b) shouldBe null
  }

  test("not route batches without a shared dictionary") {
    val b     = batch(100, withNulls = false)
    val plain = Batch(
      Array(
        b.column(0),
        StrVec(Array.tabulate(b.size)(b.column(1).asInstanceOf[StrVec].str), null)
      ),
      b.size
    )
    RoutedAggFilters
      .of(calls)
      .update(calls.map(_.newAccumulator()), new Array[Int](b.size), 1, plain) shouldBe null
  }

end RoutedAggFiltersTest
