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

import wvlet.lang.api.WvletLangException
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/** Math functions on constants; results against DuckDB are checked in EngineStdlibMathTest */
class MathFunctionsTest extends UniTest:

  private def call(name: String, args: Any*): Any =
    val bound = args.map {
      case l: Long =>
        Const(l, PType.Int64)
      case i: Int =>
        Const(i, PType.Int32)
      case d: Double =>
        Const(d, PType.Float64)
      case null =>
        Const(null, PType.Int64)
      case other =>
        fail(s"Unexpected argument: ${other}")
    }
    ScalarFunctions.lookup(name, bound).get.eval(Batch.ofRows(1)).get(0)

  test("should compute gcd and lcm as non-negative values") {
    call("gcd", 12L, 18L) shouldBe 6L
    call("gcd", -12L, 18L) shouldBe 6L
    call("gcd", 0L, 0L) shouldBe 0L
    call("lcm", -4L, 6L) shouldBe 12L
    call("lcm", 0L, 5L) shouldBe 0L
  }

  test("should fail on gcd and lcm results out of the long range") {
    intercept[WvletLangException] {
      call("lcm", Long.MaxValue, 2L)
    }.getMessage shouldContain "out of range"
    intercept[WvletLangException] {
      call("lcm", Long.MinValue, 1L)
    }.getMessage shouldContain "out of range"
    intercept[WvletLangException] {
      call("gcd", Long.MinValue, 0L)
    }.getMessage shouldContain "out of range"
  }

  test("should compute factorials up to 20 and reject negative or larger inputs") {
    call("factorial", 0L) shouldBe 1L
    call("factorial", 20L) shouldBe 2432902008176640000L
    intercept[WvletLangException] {
      call("factorial", -1L)
    }.getMessage shouldContain "negative"
    intercept[WvletLangException] {
      call("factorial", 21L)
    }.getMessage shouldContain "out of range"
  }

  test("should round ties to the even neighbor") {
    call("round_even", 2.5, 0) shouldBe 2.0
    call("round_even", 3.5, 0) shouldBe 4.0
    call("round_even", -2.5, 0) shouldBe -2.0
    // Doubles round their exact binary value: 2.675 is 2.67499..., not a tie (as DuckDB 2.0.1)
    call("round_even", 2.675, 2) shouldBe 2.67
    call("round", 2.675, 2) shouldBe 2.67
    call("round_even", 0.125, 2) shouldBe 0.12
    call("round", 0.125, 2) shouldBe 0.13
  }

  test("should round integers to tens with negative digits") {
    call("round", 25L, -1) shouldBe 30L
    call("round_even", 25L, -1) shouldBe 20L
    call("round_even", 35, -1) shouldBe 40
    intercept[WvletLangException] {
      call("round", Int.MaxValue, -1)
    }.getMessage shouldContain "out of range"
  }

  test("should keep inverse hyperbolic functions precise for tiny and huge inputs") {
    call("asinh", 1e-20) shouldBe 1e-20
    call("asinh", -1e-20) shouldBe -1e-20
    call("atanh", 1e-20) shouldBe 1e-20
    val big = call("asinh", 1e200).asInstanceOf[Double]
    math.abs(big - 461.2101657793691) < 1e-9 shouldBe true
    call("asinh", -1e8).asInstanceOf[Double] < -19.0 shouldBe true
    math.abs(call("acosh", 1e200).asInstanceOf[Double] - 461.2101657793691) < 1e-9 shouldBe true
  }

  test("should return NaN outside the domain of inverse hyperbolic functions") {
    call("acosh", 0.5).asInstanceOf[Double].isNaN shouldBe true
    call("atanh", 2.0).asInstanceOf[Double].isNaN shouldBe true
    call("atanh", 1.0) shouldBe Double.PositiveInfinity
    call("cot", 0.0) shouldBe Double.PositiveInfinity
  }

end MathFunctionsTest
