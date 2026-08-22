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
package wvlet.lang.tablestore.schema

import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONDouble
import wvlet.uni.json.JSON.JSONLong
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.test.UniTest
import wvlet.uni.test.empty

class PromotionLatticeTest extends UniTest:

  import ColumnType.*

  test("lub follows the promotion lattice") {
    lub(NullType, LongType) shouldBe LongType
    lub(LongType, NullType) shouldBe LongType
    lub(LongType, DoubleType) shouldBe DoubleType
    lub(DoubleType, StringType) shouldBe StringType
    lub(StringType, LongType) shouldBe StringType
  }

  test("boolean and numerics are incomparable; both widen to string") {
    lub(BooleanType, LongType) shouldBe StringType
    lub(LongType, BooleanType) shouldBe StringType
    lub(BooleanType, DoubleType) shouldBe StringType
    lub(BooleanType, BooleanType) shouldBe BooleanType
  }

  test("folding observed schemas is order-independent") {
    val types  = Seq[ColumnType](LongType, DoubleType, NullType, LongType, StringType)
    val folded = types.permutations.map(permutation => ColumnType.fold(permutation)).toSet
    folded.size shouldBe 1
    folded.head shouldBe StringType
  }

  test("fold of a homogeneous set keeps the type") {
    ColumnType.fold(Seq(LongType, LongType, LongType)) shouldBe LongType
    ColumnType.fold(Nil) shouldBe NullType
  }

  test("escalation folds file schemas in any order") {
    val current = TableSchema.empty
    val files   = Seq(
      (1L, 100L, ObservedSchema.fromRow(row("a" -> JSONLong(1), "b" -> JSONString("x")))),
      (2L, 200L, ObservedSchema.fromRow(row("b" -> JSONString("y"), "c" -> JSONDouble(2.5)))),
      (3L, 300L, ObservedSchema.fromRow(row("c" -> JSONString("z"))))
    )
    val results =
      files
        .permutations
        .map(permutation =>
          SchemaEscalation.escalate(current, nextVersion = 1L, files = permutation)
        )
        .toSeq
    results.toSet.size shouldBe 1
    val result = results.head
    result.quarantinedFiles shouldBe empty
    val schema = result.escalatedSchema.get
    schema.column("a").get.columnType shouldBe LongType
    schema.column("b").get.columnType shouldBe StringType
    schema.column("c").get.columnType shouldBe StringType
  }

  test("the outlier guardrail quarantines a widening forced by a tiny minority") {
    // One small file forces long -> string; it must be quarantined instead of escalating
    val current  = TableSchema(1, Seq(ColumnDesc("user_id", LongType)))
    val bigFiles = (1L to 5L).map { id =>
      (id, 10_000L, ObservedSchema.fromRow(row("user_id" -> JSONLong(id))))
    }
    val oddFile = (99L, 5L, ObservedSchema.fromRow(row("user_id" -> JSONString("garbage"))))

    val result = SchemaEscalation.escalate(current, nextVersion = 2L, files = bigFiles :+ oddFile)
    result.quarantinedFiles shouldBe Seq(99L)
    // The published type stays long: no irreversible widening happened, and since nothing widened
    // there may be no new schema version at all
    val effectiveType =
      result.escalatedSchema match
        case Some(schema) =>
          schema.column("user_id").get.columnType
        case None =>
          LongType
    effectiveType shouldBe LongType
  }

  test("a widening backed by enough rows escalates normally") {
    val current = TableSchema(1, Seq(ColumnDesc("user_id", LongType)))
    val files   = (1L to 4L).map { id =>
      (id, 10_000L, ObservedSchema.fromRow(row("user_id" -> JSONString(s"user-${id}"))))
    }
    val result = SchemaEscalation.escalate(current, nextVersion = 2L, files = files)
    result.quarantinedFiles shouldBe empty
    result.escalatedSchema.get.column("user_id").get.columnType shouldBe StringType
  }

  test("escalation never narrows the published type") {
    val current       = TableSchema(2, Seq(ColumnDesc("v", StringType)))
    val files         = Seq((1L, 100L, ObservedSchema.fromRow(row("v" -> JSONLong(42)))))
    val result        = SchemaEscalation.escalate(current, nextVersion = 3L, files = files)
    val effectiveType =
      result.escalatedSchema match
        case Some(schema) =>
          schema.column("v").get.columnType
        case None =>
          StringType
    effectiveType shouldBe StringType
    result.quarantinedFiles shouldBe empty
  }

  private def row(pairs: (String, wvlet.uni.json.JSON.JSONValue)*): wvlet.uni.json.JSON.JSONObject =
    wvlet.uni.json.JSON.JSONObject(pairs)

end PromotionLatticeTest
