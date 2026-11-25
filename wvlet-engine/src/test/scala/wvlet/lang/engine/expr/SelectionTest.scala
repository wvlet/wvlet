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
package wvlet.lang.engine.expr

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import scala.util.Random

class SelectionTest extends UniTest:
  private val n   = 64
  private val rnd = Random(42)

  // Columns 0, 1: ints in 0..9 with some NULLs; column 2: strings
  private def randomBatch(): Batch =
    def ints(): Vec = IntVec(
      PType.Int32,
      Array.fill(n)(rnd.nextInt(10)),
      Array.fill(n)(rnd.nextInt(8) == 0)
    )
    val strs = StrVec(Array.fill(n)(s"s${rnd.nextInt(5)}"), Array.fill(n)(rnd.nextInt(10) == 0))
    Batch(Array(ints(), ints(), strs), n)

  private val a = ColRef(0, PType.Int32, "a")
  private val b = ColRef(1, PType.Int32, "b")
  private val s = ColRef(2, PType.Str, "s")

  private def int(v: Int) = Const(v, PType.Int32)

  private def randomCond(depth: Int): VExpr =
    if depth == 0 || rnd.nextInt(4) == 0 then
      rnd.nextInt(5) match
        case 0 =>
          Compare(CmpOp.Lt, a, int(rnd.nextInt(10)))
        case 1 =>
          Compare(CmpOp.Ge, b, int(rnd.nextInt(10)))
        case 2 =>
          Compare(CmpOp.Eq, a, b)
        case 3 =>
          LikeExpr(s, s"s${rnd.nextInt(5)}%", negate = rnd.nextBoolean())
        case _ =>
          IsNullExpr(b, negate = rnd.nextBoolean())
    else
      rnd.nextInt(3) match
        case 0 =>
          AndExpr(randomCond(depth - 1), randomCond(depth - 1))
        case 1 =>
          OrExpr(randomCond(depth - 1), randomCond(depth - 1))
        case _ =>
          NotExpr(randomCond(depth - 1))

  // The TRUE positions by evaluating the whole condition over the batch
  private def expected(cond: VExpr, batch: Batch): Seq[Int] =
    val mask = Kernels.trueMask(cond.eval(batch))
    (0 until batch.size).filter(mask(_))

  private def selected(cond: VExpr, batch: Batch, reorder: Boolean): Seq[Int] =
    val idx = Selection.select(Selection.compile(cond, reorder), batch)
    if idx == null then
      0 until batch.size
    else
      idx.toSeq

  test("should select the rows where random AND / OR / NOT conditions are TRUE") {
    for _ <- 0 until 300 do
      val cond  = randomCond(4)
      val batch = randomBatch()
      val want  = expected(cond, batch)
      selected(cond, batch, reorder = false) shouldBe want
      selected(cond, batch, reorder = true) shouldBe want
  }

  test("should select over a batch with deferred columns") {
    for _ <- 0 until 100 do
      val cond     = randomCond(3)
      val base     = randomBatch()
      val idx      = (0 until n).filter(_ => rnd.nextBoolean()).toArray
      val deferred = base.take(idx, idx.length)
      selected(cond, deferred, reorder = true) shouldBe expected(cond, base.gather(idx, idx.length))
  }

  test("should return null when every row is selected and empty when none is") {
    val batch = randomBatch()
    val all   = OrExpr(IsNullExpr(a, negate = false), IsNullExpr(a, negate = true))
    Selection.select(Selection.compile(all, reorder = true), batch) shouldBe null
    val none = AndExpr(IsNullExpr(a, negate = false), IsNullExpr(a, negate = true))
    Selection.select(Selection.compile(none, reorder = true), batch).length shouldBe 0
  }

  // A row function failing on the rows where a is 0
  private val failsOnZero = RowFn(
    "fails_on_zero",
    Seq(a),
    PType.Bool,
    row =>
      if row(0) == 0 then
        throw StatusCode.INVALID_ARGUMENT.newException("zero")
      else
        true
  )

  test("should not evaluate a conjunct on rows an earlier conjunct rejected") {
    val batch = Batch(Array(IntVec(PType.Int32, Array(0, 1, 2, 0), null)), 4)
    val cond  = AndExpr(Compare(CmpOp.Gt, a, int(0)), failsOnZero)
    selected(cond, batch, reorder = true) shouldBe Seq(1, 2)
    // Over the whole batch, the row function sees the zeros
    intercept[Exception] {
      cond.eval(batch)
    }
  }

  test("should not evaluate a disjunct on rows an earlier disjunct accepted") {
    val batch = Batch(Array(IntVec(PType.Int32, Array(0, 1, 2, 0), null)), 4)
    val cond  = OrExpr(Compare(CmpOp.Eq, a, int(0)), failsOnZero)
    selected(cond, batch, reorder = true) shouldBe Seq(0, 1, 2, 3)
  }

  test("should run cheap conjuncts first and keep throwing ones in place") {
    val like = LikeExpr(s, "x%", negate = false)
    val cmp  = Compare(CmpOp.Lt, a, int(3))
    Selection.compile(AndExpr(like, cmp), reorder = true) shouldBe
      Selection.Conj(IndexedSeq(Selection.Leaf(cmp, Selection.cost(cmp)), Selection.Leaf(like, 8)))
    Selection.compile(AndExpr(like, cmp), reorder = false) shouldBe
      Selection.Conj(IndexedSeq(Selection.Leaf(like, 8), Selection.Leaf(cmp, Selection.cost(cmp))))

    // like, fails, cmp: neither part moves across the row function
    val cond  = AndExpr(AndExpr(like, failsOnZero), cmp)
    val order =
      Selection.compile(cond, reorder = true) match
        case Selection.Conj(parts) =>
          parts.collect { case Selection.Leaf(e, _) =>
            e
          }
        case other =>
          fail(s"Unexpected ${other}")
    order shouldBe Seq(like, failsOnZero, cmp)
  }

  test("should treat casts and arithmetic that can fail as unsafe") {
    Selection.isSafe(Compare(CmpOp.Lt, a, int(3))) shouldBe true
    Selection.isSafe(CastExpr(s, PType.Int32)) shouldBe false
    Selection.isSafe(CastExpr(s, PType.Int32, tryCast = true)) shouldBe true
    Selection.isSafe(CastExpr(a, PType.Int64)) shouldBe true
    Selection.isSafe(Arith(ArithOp.Add, a, b, PType.Int32)) shouldBe false
  }

  test("should select only when some part is costly") {
    val cmp  = Compare(CmpOp.Lt, a, int(3))
    val like = LikeExpr(s, "x%", negate = false)
    Selection.worthSelecting(Selection.compile(AndExpr(cmp, cmp), reorder = true)) shouldBe false
    Selection.worthSelecting(Selection.compile(AndExpr(cmp, like), reorder = true)) shouldBe true
    Selection.worthSelecting(
      Selection.compile(OrExpr(cmp, AndExpr(cmp, like)), reorder = true)
    ) shouldBe true
    Selection.worthSelecting(Selection.compile(like, reorder = true)) shouldBe false
  }

  // Columns 0, 1: dictionary-coded strings over 6 and 4 entries with some NULLs, and some rows of
  // column 1 without codes when `uncoded`
  private def dictBatch(uncoded: Boolean = false): Batch =
    def strs(entries: Int, withUncoded: Boolean): StrVec =
      val codes  = Array.fill(n)(rnd.nextInt(entries))
      val nulls  = Array.fill(n)(rnd.nextInt(9) == 0)
      val values =
        Array.tabulate(n)(i =>
          if nulls(i) then
            null
          else
            s"v${codes(i)}"
        )
      (0 until n).foreach { i =>
        if nulls(i) || (withUncoded && rnd.nextInt(5) == 0) then
          codes(i) = -1
      }
      StrVec(values, nulls, codes, entries)
    Batch(Array(strs(6, false), strs(4, uncoded)), n)

  private val d0 = ColRef(0, PType.Str, "d0")
  private val d1 = ColRef(1, PType.Str, "d1")

  private def randomDictCond(depth: Int): VExpr =
    if depth == 0 || rnd.nextInt(4) == 0 then
      val c =
        if rnd.nextBoolean() then
          d0
        else
          d1
      rnd.nextInt(4) match
        case 0 =>
          Compare(CmpOp.Eq, c, Const(s"v${rnd.nextInt(6)}", PType.Str))
        case 1 =>
          Compare(CmpOp.Ne, c, Const(s"v${rnd.nextInt(6)}", PType.Str))
        case 2 =>
          IsNullExpr(c, negate = rnd.nextBoolean())
        case _ =>
          OrExpr(
            Compare(CmpOp.Eq, c, Const(s"v${rnd.nextInt(6)}", PType.Str)),
            Compare(CmpOp.Eq, c, Const(s"v${rnd.nextInt(6)}", PType.Str))
          )
    else
      rnd.nextInt(3) match
        case 0 | 1 =>
          AndExpr(randomDictCond(depth - 1), randomDictCond(depth - 1))
        case _ =>
          OrExpr(randomDictCond(depth - 1), randomDictCond(depth - 1))

  test("should select dictionary-coded rows per entry as the whole-batch evaluation does") {
    for _ <- 0 until 300 do
      val cond  = DictionaryPredicates.wrap(randomDictCond(4), whole = rnd.nextBoolean())
      val batch = dictBatch(uncoded = rnd.nextInt(4) == 0)
      val node  = Selection.compile(cond, reorder = true, dictionaryPositions = true)
      val idx   = Selection.select(node, batch)
      val got   =
        if idx == null then
          0 until batch.size
        else
          idx.toSeq
      got shouldBe expected(cond, batch)
  }

  test("should select per entry over a subset of the rows") {
    val batch = dictBatch()
    val cond  = DictionaryPredicates.wrap(Compare(CmpOp.Ne, d0, Const("v1", PType.Str)))
    val sel   = (0 until n by 3).toArray
    val got   =
      cond match
        case p: PerDictEntry =>
          p.selectPositions(batch, sel).toSeq
        case other =>
          fail(s"Unexpected ${other}")
    got shouldBe sel.toSeq.filter(expected(cond, batch).toSet)
  }

  test("should select when an AND has two per-entry parts") {
    val eq0  = DictionaryPredicates.wrap(Compare(CmpOp.Eq, d0, Const("v1", PType.Str)))
    val eq1  = DictionaryPredicates.wrap(Compare(CmpOp.Eq, d1, Const("v2", PType.Str)))
    val cond = OrExpr(AndExpr(eq0, eq1), AndExpr(eq1, eq0))
    Selection.worthSelecting(
      Selection.compile(cond, reorder = true, dictionaryPositions = true)
    ) shouldBe true
    Selection.worthSelecting(Selection.compile(cond, reorder = true)) shouldBe false
    Selection.worthSelecting(
      Selection.compile(OrExpr(eq0, eq1), reorder = true, dictionaryPositions = true)
    ) shouldBe false
  }

  test("should keep a per-entry part's cost for ordering and selecting") {
    val like = DictionaryPredicates.wrap(LikeExpr(d0, "v%", negate = false))
    val cmp  = Compare(CmpOp.Lt, a, int(3))
    val node = Selection.compile(AndExpr(like, cmp), reorder = true, dictionaryPositions = true)
    node match
      case Selection.Conj(IndexedSeq(Selection.Leaf(`cmp`, _), Selection.DictLeaf(_, c))) =>
        c shouldBe Selection.cost(like)
      case other =>
        fail(s"Unexpected ${other}")
    Selection.worthSelecting(node) shouldBe true
  }

  test("should wrap an OR over one string column as one per-entry part") {
    val either = OrExpr(
      Compare(CmpOp.Eq, d0, Const("v1", PType.Str)),
      Compare(CmpOp.Eq, d0, Const("v2", PType.Str))
    )
    DictionaryPredicates.wrap(either, whole = true) match
      case p: PerDictEntry =>
        p.inner shouldBe either
      case other =>
        fail(s"Unexpected ${other}")
    DictionaryPredicates.wrap(either) match
      case OrExpr(_: PerDictEntry, _: PerDictEntry) =>
      case other                                    =>
        fail(s"Unexpected ${other}")
  }

end SelectionTest
