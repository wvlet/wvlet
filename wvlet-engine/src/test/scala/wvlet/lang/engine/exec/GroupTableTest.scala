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

import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

class GroupTableTest extends UniTest:
  private def longs(xs: Long*): Vec  = LongVec(PType.Int64, xs.toArray, null)
  private def strs(xs: String*): Vec = StrVec(
    xs.toArray,
    if xs.contains(null) then
      xs.map(_ == null).toArray
    else
      null
  )

  test("should assign dense ids to composite keys") {
    val t   = GroupTable(IndexedSeq(PType.Int64, PType.Str))
    val ids = t.groupIds(Array(longs(1, 2, 1, 1), strs("a", "a", "a", "b")), 4)
    ids.toSeq shouldBe Seq(0, 1, 0, 2)
    t.numGroups shouldBe 3
    val keys = t.keyVectors()
    (0 until 3).map(i => (keys(0).get(i), keys(1).get(i))) shouldBe
      Seq((1L, "a"), (2L, "a"), (1L, "b"))
  }

  test("should give the same ids with a pre-sized table, beyond or below its expected groups") {
    val n    = 5000
    val keys = Array(
      longs((0 until n).map(i => (i % 1000).toLong)*),
      strs((0 until n).map(i => s"k${i % 3}")*)
    )
    val plain = GroupTable(IndexedSeq(PType.Int64, PType.Str)).groupIds(keys, n).toSeq
    Seq(10, 1000, 3000, 100000).foreach { expected =>
      val t = GroupTable(IndexedSeq(PType.Int64, PType.Str), expected)
      t.groupIds(keys, n).toSeq shouldBe plain
      t.numGroups shouldBe 3000
      t.lookup(keys, n).toSeq shouldBe plain
    }
  }

  test("should hash every batch afresh when reusing its hash buffer") {
    val t = GroupTable(IndexedSeq(PType.Int64))
    t.groupIds(Array(longs(1, 2, 3, 4)), 4).toSeq shouldBe Seq(0, 1, 2, 3)
    // A shorter batch, then a longer one: stale hashes of the previous batch must not leak
    t.groupIds(Array(longs(4, 1)), 2).toSeq shouldBe Seq(3, 0)
    t.groupIds(Array(longs(5, 3, 2, 1, 6)), 5).toSeq shouldBe Seq(4, 2, 1, 0, 5)
    t.numGroups shouldBe 6
  }

  test("should group NULL keys together but never match them in lookups") {
    val t = GroupTable(IndexedSeq(PType.Str))
    t.groupIds(Array(strs("x", null, null, "x")), 4).toSeq shouldBe Seq(0, 1, 1, 0)
    t.keyVectors()(0).isNull(1) shouldBe true
    t.lookup(Array(strs(null, "x", "y")), 3).toSeq shouldBe Seq(-1, 0, -1)
  }

  test("should keep ids stable while the table grows") {
    val t   = GroupTable(IndexedSeq(PType.Int64))
    val n   = 50000
    val v   = LongVec(PType.Int64, Array.tabulate(n)(i => (i % 20000).toLong * 7919L), null)
    val ids = t.groupIds(Array(v), n)
    t.numGroups shouldBe 20000
    (0 until n).forall(i => ids(i) == ids(i % 20000)) shouldBe true
    t.lookup(Array(longs(7919L * 3, 5L)), 2).toSeq shouldBe Seq(3, -1)
  }

  test("should match keys given in another vector representation") {
    val t = GroupTable(IndexedSeq(PType.Int64))
    t.groupIds(Array(longs(10, 20)), 2)
    // int32 values for an int64 key are converted, not compared by representation
    t.lookup(Array(IntVec(PType.Int32, Array(20, 30), null)), 2).toSeq shouldBe Seq(1, -1)
  }

  test("should compare doubles by value bits") {
    val t = GroupTable(IndexedSeq(PType.Float64))
    val v = DoubleVec(Array(Double.NaN, Double.NaN, 0.0, -0.0, 1.5), null)
    t.groupIds(Array(v), 5).toSeq shouldBe Seq(0, 0, 1, 2, 3)
  }

  test("should put NULL keys in one group whatever their slots hold") {
    // Kernels compute values for NULL rows too, e.g. NULL + 5 and NULL + 7
    val nulls = Array(true, true, false)
    for v <- Seq[Vec](
        LongVec(PType.Int64, Array(5L, 7L, 1L), nulls),
        IntVec(PType.Int32, Array(5, 7, 1), nulls),
        DoubleVec(Array(5.0, 7.0, 1.0), nulls),
        StrVec(Array("a", "b", "c"), nulls),
        BoolVec(Array(true, false, true), nulls)
      )
    do
      GroupTable(IndexedSeq(v.ptype)).groupIds(Array(v), 3).toSeq shouldBe Seq(0, 0, 1)
  }

  test("should partition rows by key hash without reusing a table's slot bits") {
    val n    = 100000
    val keys = LongVec(PType.Int64, Array.tabulate(n)(i => (i % 40000).toLong), null)
    val h    = GroupTable(IndexedSeq(PType.Int64)).keyHashes(Array(keys), n)
    val rows = GroupTable.partitionRows(h, n, 16)
    // Every row once, in input order within a partition
    rows.flatten.sorted.toSeq shouldBe (0 until n)
    rows.forall(r => r.toSeq == r.sorted.toSeq) shouldBe true
    // Rows with the same key share a partition
    val partOf = new Array[Int](n)
    rows.indices.foreach(p => rows(p).foreach(i => partOf(i) = p))
    (0 until n).forall(i => partOf(i) == partOf(i % 40000)) shouldBe true
    // Bits 16 and up, which large partition tables use as slot bits, still vary inside a partition
    rows.foreach(r => r.map(i => h(i) & 0xffff0000).distinct.size > 1 shouldBe true)
  }

  /** Strings with dictionary codes: code -1 for NULLs and for rows marked "?" (PLAIN rows) */
  private def coded(dict: IndexedSeq[String], xs: String*): StrVec =
    val nulls =
      if xs.contains(null) then
        xs.map(_ == null).toArray
      else
        null
    val codes =
      xs.map(x =>
          if x == null || x.endsWith("?") then
            -1
          else
            dict.indexOf(x)
        )
        .toArray
    StrVec(
      xs.map(x =>
          if x == null then
            null
          else
            x.stripSuffix("?")
        )
        .toArray,
      nulls,
      codes,
      dict.size
    )

  private def plain(v: StrVec): StrVec = StrVec(v.values, v.nulls)

  private def sameGroups(keys: Array[Vec], n: Int): Seq[Int] =
    val types = IndexedSeq.fill(keys.length)(PType.Str)
    val t     = GroupTable(types)
    val ids   = t.groupIds(keys, n)
    val p     = GroupTable(types)
    ids.toSeq shouldBe p.groupIds(keys.map(k => plain(k.asInstanceOf[StrVec])), n).toSeq
    t.keyVectors().map(_.asInstanceOf[StrVec].values.toSeq).toSeq shouldBe
      p.keyVectors().map(_.asInstanceOf[StrVec].values.toSeq).toSeq
    ids.toSeq

  test("should group dictionary-coded strings as their values, with NULLs and rows without codes") {
    val flags  = IndexedSeq("A", "N", "R")
    val status = IndexedSeq("F", "O")
    val n      = 40
    val f      = (0 until n).map(i =>
      if i % 13 == 0 then
        null
      else if i % 7 == 0 then
        flags(i % 3) + "?"
      else
        flags(i % 3)
    )
    val s   = (0 until n).map(i => status((i / 3) % 2))
    val ids = sameGroups(Array(coded(flags, f*), coded(status, s*)), n)
    ids.distinct.size shouldBe 8
  }

  test("should keep group ids across batches whose dictionaries differ") {
    val types = IndexedSeq(PType.Str)
    val t     = GroupTable(types)
    val p     = GroupTable(types)
    val b1    = coded(IndexedSeq("x", "y"), "x", "y", "x", "x", "y", "y")
    // The same strings under other codes, and a new one
    val b2 = coded(IndexedSeq("z", "y", "x"), "y", "z", "x", "z", "y", "x")
    t.groupIds(Array(b1), 6).toSeq shouldBe p.groupIds(Array(plain(b1)), 6).toSeq
    t.groupIds(Array(b2), 6).toSeq shouldBe p.groupIds(Array(plain(b2)), 6).toSeq
    t.groupIds(Array(b2), 6).toSeq shouldBe Seq(1, 2, 0, 2, 1, 0)
    // Pre-computed hashes take the same path
    t.groupIds(Array(b1), 6, t.keyHashes(Array(b1), 6)).toSeq shouldBe Seq(0, 1, 0, 0, 1, 1)
  }

  test("should group by values when the dictionary has as many entries as the rows") {
    val dict = IndexedSeq("a", "b", "c", "d")
    sameGroups(Array(coded(dict, "a", "b", "c", "a", "d")), 5) shouldBe Seq(0, 1, 2, 0, 3)
  }

  test("should carry dictionary codes through gather, with -1 for padding") {
    val v = coded(IndexedSeq("p", "q"), "q", "p", null).gather(Array(1, -1, 0, 2), 4)
    v.asInstanceOf[StrVec].codes.toSeq shouldBe Seq(0, -1, 1, -1)
  }

  /** Strings encoded over a shared dict, as a join build's columns ([[StrVec.encode]]) */
  private def shared(dict: Array[String], xs: String*): StrVec =
    val codes =
      xs.map(x =>
          if x == null then
            -1
          else
            dict.indexOf(x)
        )
        .toArray
    StrVec(xs.toArray, strs(xs*).nulls, codes, dict.length, dict)

  private def values(t: GroupIndex): Seq[Seq[Any]] =
    t.keyVectors().map(v => (0 until v.size).map(v.get)).toSeq

  test("should group strings with a shared dict by their codes across batches") {
    val dict  = Array("x", "y", "z")
    val types = IndexedSeq(PType.Str, PType.Int64)
    val t     = GroupTable(types, stableCodes = true)
    val p     = GroupTable(types)
    val b1    = Array[Vec](shared(dict, "x", "y", null, "x", "z"), longs(1, 1, 2, 1, 3))
    val b2    = Array[Vec](shared(dict, "z", "x", null, "y"), longs(3, 2, 2, 1))
    t.groupIds(b1, 5).toSeq shouldBe p.groupIds(b1.map(k => plainOf(k)), 5).toSeq
    t.usesStableCodes shouldBe true
    t.groupIds(b2, 4, t.keyHashes(b2, 4)).toSeq shouldBe p.groupIds(b2.map(plainOf), 4).toSeq
    t.numGroups shouldBe 5
    values(t) shouldBe values(p)
    // The key strings keep the codes, so a final aggregation groups them by codes too
    val k = t.keyVectors()(0).asInstanceOf[StrVec]
    k.dict shouldBeTheSameInstanceAs dict
    k.codes.toSeq shouldBe Seq(0, 1, -1, 2, 0)
  }

  test("should group string codes through a long group table with longTables") {
    val dict  = Array("x", "y", "z")
    val types = IndexedSeq(PType.Str, PType.Int64)
    val t     = GroupTable(types, stableCodes = true, longTables = true)
    val p     = GroupTable(types)
    val b1    = Array[Vec](shared(dict, "x", "y", null, "x", "z"), longs(1, 1, 2, 1, 3))
    val b2    = Array[Vec](shared(dict, "z", "x", null, "y"), longs(3, 2, 2, 1))
    t.groupIds(b1, 5).toSeq shouldBe p.groupIds(b1.map(k => plainOf(k)), 5).toSeq
    t.groupIds(b2, 4).toSeq shouldBe p.groupIds(b2.map(plainOf), 4).toSeq
    t.usesStableCodes shouldBe true
    values(t) shouldBe values(p)
  }

  test("should move code-keyed groups to their strings for a batch without the dict") {
    val dict  = Array("x", "y")
    val types = IndexedSeq(PType.Str)
    val t     = GroupTable(types, stableCodes = true)
    val p     = GroupTable(types)
    val b1    = Array[Vec](shared(dict, "y", "x", "y"))
    val b2    = Array[Vec](strs("x", "w", null))
    val b3    = Array[Vec](shared(Array("w", "y"), "w", "y"))
    t.groupIds(b1, 3).toSeq shouldBe p.groupIds(b1.map(plainOf), 3).toSeq
    t.groupIds(b2, 3).toSeq shouldBe p.groupIds(b2, 3).toSeq
    t.usesStableCodes shouldBe false
    t.groupIds(b3, 2).toSeq shouldBe Seq(2, 0)
    t.usesStableCodes shouldBe false
    t.groupIds(b1, 3).toSeq shouldBe Seq(0, 1, 0)
    values(t) shouldBe Seq(Seq("y", "x", "w", null))
  }

  test("should key groups by codes only from the first batch, and decode them for lookups") {
    val dict = Array("a", "b")
    val t    = GroupTable(IndexedSeq(PType.Str), stableCodes = true)
    t.groupIds(Array(strs("b")), 1).toSeq shouldBe Seq(0)
    t.groupIds(Array(shared(dict, "a", "b")), 2).toSeq shouldBe Seq(1, 0)
    t.usesStableCodes shouldBe false
    val u = GroupTable(IndexedSeq(PType.Str), stableCodes = true)
    u.groupIds(Array(shared(dict, "a", "b", "a")), 3).toSeq shouldBe Seq(0, 1, 0)
    u.lookup(Array(strs("b", "c")), 2).toSeq shouldBe Seq(1, -1)
    u.usesStableCodes shouldBe false
    // Without stableCodes, a shared dict takes the per-batch dictionary path
    val v = GroupTable(IndexedSeq(PType.Str))
    v.groupIds(Array(shared(dict, "a", "b", "a")), 3).toSeq shouldBe Seq(0, 1, 0)
    v.usesStableCodes shouldBe false
  }

  test("should encode Parquet-coded parts over their distinct values, up to a limit") {
    // Two parts with different dictionaries, a NULL, and a PLAIN "q" row (code -1)
    val p1  = coded(IndexedSeq("q", "p"), "q", null, "p", "q")
    val p2  = coded(IndexedSeq("p", "r"), "r", "p", "q?")
    val all = StrVec(p1.values ++ p2.values, p1.nulls ++ Array(false, false, false))
    val e   = StrVec.encodeParts(all, Seq(p1, p2), 7, hashPlain = false)
    e.codes.toSeq shouldBe Seq(0, -1, 1, 0, 2, 1, 0)
    e.dict.toSeq shouldBe Seq("q", "p", "r")
    e.dictSize shouldBe 3
    e.gather(Array(2, -1), 2).asInstanceOf[StrVec].dict shouldBeTheSameInstanceAs e.dict
    // Too many distinct values, a part without codes, or parts not covering the rows
    StrVec.encodeParts(all, Seq(p1, p2), 2, hashPlain = false) shouldBeTheSameInstanceAs all
    StrVec.encodeParts(all, Seq(p1, plain(p2)), 7, hashPlain = false) shouldBeTheSameInstanceAs all
    StrVec.encodeParts(all, Seq(p1), 7, hashPlain = false) shouldBeTheSameInstanceAs all
    // Small builds hash the rows of parts without codes
    StrVec.encodeParts(all, Seq(p1, plain(p2)), 7, hashPlain = true).codes.toSeq shouldBe
      e.codes.toSeq
    // Gathered codes are read on first use, also through a second gather
    val g = e.gather(Array(4, 0, -1), 3).asInstanceOf[StrVec]
    g.hasCodes shouldBe true
    g.gather(Array(1, 2, 0), 3).asInstanceOf[StrVec].codes.toSeq shouldBe Seq(0, -1, 2)
    g.codes.toSeq shouldBe Seq(2, 0, -1)
  }

  test("should keep join build codes only when every string column encodes") {
    val rep  = StrVec(Array.tabulate(100)(i => s"r${i % 4}"), null)
    val uniq = StrVec(Array.tabulate(100)(i => s"u${i}"), null)
    val ids  = LongVec(PType.Int64, Array.tabulate(100)(_.toLong), null)
    val ok   = Batch(Array[Vec](ids, rep), 100)
    val e    = JoinBuild.encodeStrings(ok, Seq(ok))
    e.column(1).asInstanceOf[StrVec].dictSize shouldBe 4
    val mixed = Batch(Array[Vec](ids, rep, uniq), 100)
    JoinBuild.encodeStrings(mixed, Seq(mixed)) shouldBeTheSameInstanceAs mixed
  }

  private def plainOf(v: Vec): Vec =
    v match
      case s: StrVec =>
        plain(s)
      case other =>
        other

end GroupTableTest
