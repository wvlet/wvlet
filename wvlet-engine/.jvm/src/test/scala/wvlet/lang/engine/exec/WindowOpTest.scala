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

class WindowOpTest extends UniTest:
  private val runner = ThreadPoolRunner(4)

  override def afterAll: Unit = runner.shutdown()

  private val schema = Schema(
    IndexedSeq(Field("g", PType.Int64), Field("s", PType.Str), Field("v", PType.Int64))
  )

  /** 10 batches of 1000 rows: about 100 partitions by (g, s), NULL keys included */
  private val batches: Seq[Batch] = (0 until 10).map { b =>
    val n = 1000
    val r = Array.tabulate(n)(i => b * n + i)
    Batch(
      Array(
        LongVec(PType.Int64, r.map(i => (i % 50).toLong), r.map(_ % 97 == 0)),
        StrVec(r.map(i => s"s${i % 2}"), r.map(_ % 89 == 0)),
        LongVec(PType.Int64, r.map(i => ((i * 7919) % 101).toLong), null)
      ),
      n
    )
  }

  private val g     = ColRef(0, PType.Int64, "g")
  private val s     = ColRef(1, PType.Str, "s")
  private val v     = ColRef(2, PType.Int64, "v")
  private val order = Seq(SortKey(v, ascending = true, nullsFirst = false))

  private def call(
      name: String,
      args: Seq[VExpr],
      partitionBy: Seq[VExpr],
      orderBy: Seq[SortKey],
      frame: Option[FrameSpec],
      ptype: PType
  ) = WindowCall(name, args, partitionBy, orderBy, frame, false, ptype)

  private val calls = IndexedSeq(
    call("row_number", Nil, Seq(g, s), order, None, PType.Int64),
    call("rank", Nil, Seq(g), order, None, PType.Int64),
    call("sum", Seq(v), Seq(g, s), order, Some(FrameSpec(true, Some(-2), Some(0))), PType.Int64),
    call("lag", Seq(v), Seq(s), order, None, PType.Int64),
    call("count", Seq(v), Seq(g), Nil, None, PType.Int64),
    // No PARTITION BY: stays sequential
    call("row_number", Nil, Nil, order, None, PType.Int64)
  )

  private val outSchema = Schema(
    schema.fields ++ calls.indices.map(i => Field(s"w${i}", calls(i).ptype))
  )

  private def rows(parallel: Boolean): Seq[String] =
    val op = WindowOp(
      BatchesOp(schema, batches),
      calls,
      outSchema,
      if parallel then
        runner
      else
        TaskRunner.Sequential
      ,
      minParallelRows = 0
    )
    try op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.row(i).map(String.valueOf).mkString(",")))
    finally op.close()

  test("should evaluate partitioned windows in parallel with the sequential results") {
    val expected = rows(parallel = false)
    val actual   = rows(parallel = true)
    actual.size shouldBe 10000
    actual shouldBe expected
  }

end WindowOpTest
