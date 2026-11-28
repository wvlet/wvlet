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

import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

import java.nio.charset.StandardCharsets

/** LIKE over the UTF-8 bytes of bytes-backed string vectors (`plain_string_bytes`) */
class LikeBytesTest extends UniTest:
  private val strings = Seq(
    "",
    "a",
    "abc",
    "special requests",
    "a special deposit, then requests",
    "requests special",
    "日本語のテキスト",
    "naïve café",
    "x%y",
    "specialrequests",
    "spécial requests"
  )

  /** A bytes-backed vector of `values` (null for NULL) */
  private def bytesVec(values: Seq[String]): StrVec =
    val encoded = values.map(v => Option(v).map(_.getBytes(StandardCharsets.UTF_8)))
    val offsets = encoded.scanLeft(0)((o, b) => o + b.map(_.length).getOrElse(0)).toArray
    val bytes   = encoded.flatMap(_.getOrElse(Array.emptyByteArray)).toArray
    val nulls   = values.map(_ == null).toArray
    StrVec.ofBytes(
      bytes,
      offsets,
      if nulls.exists(identity) then
        nulls
      else
        null
      ,
      values.size
    )

  test("match the same rows as the String matcher") {
    val patterns = Seq(
      "abc",
      "",
      "%",
      "%%",
      "a%",
      "%c",
      "%special%",
      "%special%requests%",
      "a%requests",
      "日本%",
      "%café",
      "%ï%",
      "x%y",
      "%requests%special%",
      "ab%bc"
    )
    val v = bytesVec(strings)
    patterns.foreach { p =>
      val m = LikeBytes.compile(p, caseInsensitive = false, escape = None)
      val s = LikeMatcher.compile(p, caseInsensitive = false, escape = None)
      val o = v.bytesView.offsets
      strings
        .indices
        .foreach { i =>
          (p, strings(i), m.matches(v.bytesView.data, o(i), o(i + 1))) shouldBe
            (p, strings(i), s(strings(i)))
        }
    }
  }

  test("leave patterns with _, an escape, or case folding to the String matcher") {
    LikeBytes.compile("a_c", false, None) shouldBe null
    LikeBytes.compile("a%", true, None) shouldBe null
    LikeBytes.compile("a!%", false, Some('!')) shouldBe null
  }

  test("evaluate LIKE over a bytes-backed vector with NULLs") {
    val values = Seq("special requests", null, "plain", "日本語", null, "a special b requests")
    val v      = bytesVec(values)
    val batch  = Batch(Array[Vec](v), values.size)
    val e      = LikeExpr(ColRef(0, PType.Str, "c"), "%special%requests%", negate = true)
    val out    = e.eval(batch)
    (0 until values.size).map(i => out.get(i)) shouldBe Seq(false, null, true, true, null, false)
  }

  test("read and gather rows of a bytes-backed vector") {
    val values = Seq("abc", null, "", "日本語", "naïve")
    val v      = bytesVec(values)
    v.holdsBytes shouldBe true
    // A gather keeps the source's bytes and indexes them; a gather of a view composes the index
    val g  = v.gather(Array(3, -1, 0, 1, 4), 5).asInstanceOf[StrVec]
    val gg = g.gather(Array(4, 2, 0, 1), 4).asInstanceOf[StrVec]
    g.holdsBytes shouldBe true
    (g.bytesView.data eq v.bytesView.data) shouldBe true
    // LIKE reads the bytes through the index
    val like = LikeExpr(ColRef(0, PType.Str, "c"), "%ï%", negate = false).eval(
      Batch(Array[Vec](gg), 4)
    )
    (0 until 4).map(like.get) shouldBe Seq(true, false, false, null)
    (0 until 4).map(gg.get) shouldBe Seq("naïve", "abc", "日本語", null)
    (0 until 5).map(g.get) shouldBe Seq("日本語", null, "abc", null, "naïve")
    (0 until values.size).map(v.str) shouldBe values
    v.values.toSeq shouldBe values
    // Once their Strings are built, the vectors drop the bytes (and the views their source)
    Seq(v, g, gg).map(_.holdsBytes) shouldBe Seq(false, false, false)
    // Views share the source's Strings, so equal rows are one instance
    (g.values(2) eq v.values(0)) shouldBe true
    (gg.values(1) eq v.values(0)) shouldBe true
    // Once the Strings are built, a gather reuses them
    val h = v.gather(Array(4, 0), 2).asInstanceOf[StrVec]
    h.holdsBytes shouldBe false
    h.values.toSeq shouldBe Seq("naïve", "abc")
  }

end LikeBytesTest
